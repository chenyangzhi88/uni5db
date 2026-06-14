use super::*;

impl GatewayServer {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self::with_mode(store, GatewayMode::Postgres)
    }

    pub fn with_mode(store: Arc<dyn KvStore>, mode: GatewayMode) -> Self {
        Self {
            catalog: CatalogStore::new(store.clone()),
            mode,
            store,
            datafusion_user_catalogs: tokio::sync::RwLock::new(HashMap::new()),
            active_transactions: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_snapshots: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_isolations: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_read_only: tokio::sync::Mutex::new(HashSet::new()),
            active_mysql_current_reads: tokio::sync::Mutex::new(HashSet::new()),
            active_mysql_locks: tokio::sync::Mutex::new(Vec::new()),
            active_mysql_lock_queue: tokio::sync::Mutex::new(VecDeque::new()),
            active_mysql_lock_waits: tokio::sync::Mutex::new(HashMap::new()),
            mysql_lock_notify: tokio::sync::Notify::new(),
            active_sql_prepared: tokio::sync::Mutex::new(HashMap::new()),
            active_copy_in: tokio::sync::Mutex::new(HashMap::new()),
            cancelled_sessions: tokio::sync::Mutex::new(HashSet::new()),
            mvcc_clock: AtomicU64::new(1),
        }
    }

    pub(super) fn default_transaction_isolation(&self) -> TransactionIsolation {
        TransactionIsolation::from_level(dialect::profile(self.mode).transaction.default_isolation)
    }

    pub(super) fn default_schema_name(&self) -> &'static str {
        dialect::profile(self.mode).default_schema
    }

    pub(super) fn new_datafusion_context(database_name: &str, schema_name: &str) -> SessionContext {
        let config =
            SessionConfig::new().with_default_catalog_and_schema(database_name, schema_name);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_optimizer_rule(Arc::new(KvAggregateOptimizerRule))
            .with_optimizer_rule(Arc::new(KvTopNOptimizerRule))
            .with_query_planner(Arc::new(KvQueryPlanner))
            .build();
        SessionContext::new_with_state(state)
    }

    pub(super) async fn invalidate_datafusion_catalog(&self, database_name: &str) {
        self.datafusion_user_catalogs
            .write()
            .await
            .remove(database_name);
    }

    pub(super) async fn cached_user_catalog_provider(
        &self,
        database_name: &str,
    ) -> PgWireResult<Arc<dyn CatalogProvider>> {
        if let Some(provider) = self
            .datafusion_user_catalogs
            .read()
            .await
            .get(database_name)
            .cloned()
        {
            return Ok(provider);
        }

        let provider =
            build_user_catalog_provider(self.store.clone(), &self.catalog, database_name).await?;
        self.datafusion_user_catalogs
            .write()
            .await
            .insert(database_name.to_string(), provider.clone());
        Ok(provider)
    }

    pub(super) async fn register_datafusion_catalogs(
        &self,
        ctx: &SessionContext,
        database_name: &str,
        include_system_catalogs: bool,
    ) -> PgWireResult<()> {
        if include_system_catalogs {
            register_catalog_tables_with_options_for_mode(
                ctx,
                self.store.clone(),
                &self.catalog,
                database_name,
                true,
                self.mode,
            )
            .await
        } else {
            register_pg_catalog_functions(ctx);
            ctx.register_catalog(
                database_name,
                self.cached_user_catalog_provider(database_name).await?,
            );
            Ok(())
        }
    }

    pub(crate) async fn mysql_describe_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<Vec<MySqlColumnMetadata>> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut rows = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let key = if column.primary_key || column.name == schema.primary_key {
                "PRI"
            } else if schema.unique_constraints.iter().any(|constraint| {
                !constraint.primary_key
                    && constraint.columns.len() == 1
                    && constraint.columns[0].eq_ignore_ascii_case(&column.name)
            }) || indexes.iter().any(|index| {
                index.unique
                    && index.column_names.len() == 1
                    && index.column_names[0].eq_ignore_ascii_case(&column.name)
            }) {
                "UNI"
            } else if indexes.iter().any(|index| {
                !index.unique
                    && index
                        .column_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&column.name))
            }) {
                "MUL"
            } else {
                ""
            };
            let mut extras = Vec::new();
            if column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
            {
                extras.push("auto_increment".to_string());
            }
            if let Some(on_update) = &column.on_update {
                extras.push(format!("on update {on_update}"));
            }
            rows.push(MySqlColumnMetadata {
                field: column.name.clone(),
                column_type: mysql_column_type_name(&column.data_type),
                nullable: if column.nullable { "YES" } else { "NO" }.to_string(),
                key: key.to_string(),
                default_value: column.default.clone(),
                extra: extras.join(" "),
            });
        }
        Ok(rows)
    }

    pub(crate) async fn mysql_describe_table_fast(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<Vec<MySqlColumnMetadata>> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let mut rows = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let key = if column.primary_key || column.name == schema.primary_key {
                "PRI"
            } else if schema.unique_constraints.iter().any(|constraint| {
                !constraint.primary_key
                    && constraint.columns.len() == 1
                    && constraint.columns[0].eq_ignore_ascii_case(&column.name)
            }) {
                "UNI"
            } else {
                ""
            };
            let mut extras = Vec::new();
            if column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
            {
                extras.push("auto_increment".to_string());
            }
            if let Some(on_update) = &column.on_update {
                extras.push(format!("on update {on_update}"));
            }
            rows.push(MySqlColumnMetadata {
                field: column.name.clone(),
                column_type: mysql_column_type_name(&column.data_type),
                nullable: if column.nullable { "YES" } else { "NO" }.to_string(),
                key: key.to_string(),
                default_value: column.default.clone(),
                extra: extras.join(" "),
            });
        }
        Ok(rows)
    }

    pub(crate) async fn mysql_show_tables(&self, database_name: &str) -> PgWireResult<Vec<String>> {
        let mut tables = self
            .catalog
            .list_tables(database_name)
            .await?
            .into_iter()
            .map(|table| table.table_name)
            .collect::<Vec<_>>();
        tables.sort();
        Ok(tables)
    }

    pub(crate) async fn mysql_show_databases(&self) -> PgWireResult<Vec<String>> {
        let mut databases = self
            .catalog
            .list_databases()
            .await?
            .into_iter()
            .map(|database| database.database_name)
            .collect::<Vec<_>>();
        databases.sort();
        Ok(databases)
    }

    pub(crate) async fn mysql_show_create_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> PgWireResult<String> {
        let search_path = vec![
            self.default_schema_name().to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ];
        let (_schema_name, schema) = self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?;
        let Some(schema) = schema else {
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut lines = Vec::new();
        for column in &schema.columns {
            let mut line = format!(
                "  `{}` {}",
                mysql_escape_identifier(&column.name),
                mysql_column_type_name(&column.data_type)
            );
            if !column.nullable || column.primary_key || column.name == schema.primary_key {
                line.push_str(" NOT NULL");
            }
            if column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
            {
                line.push_str(" AUTO_INCREMENT");
            } else if let Some(default) = &column.default {
                line.push_str(" DEFAULT ");
                line.push_str(default);
            }
            if let Some(character_set) = &column.character_set {
                line.push_str(" CHARACTER SET ");
                line.push_str(character_set);
            }
            if let Some(collation) = &column.collation {
                line.push_str(" COLLATE ");
                line.push_str(collation);
            }
            if let Some(on_update) = &column.on_update {
                line.push_str(" ON UPDATE ");
                line.push_str(on_update);
            }
            lines.push(line);
        }
        if schema.has_user_primary_key() {
            lines.push(format!(
                "  PRIMARY KEY (`{}`)",
                mysql_escape_identifier(&schema.primary_key)
            ));
        }
        for constraint in &schema.unique_constraints {
            if constraint.primary_key {
                continue;
            }
            let columns = constraint
                .columns
                .iter()
                .map(|column| format!("`{}`", mysql_escape_identifier(column)))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!(
                "  UNIQUE KEY `{}` ({columns})",
                mysql_escape_identifier(&constraint.name)
            ));
        }
        for index in indexes {
            let columns = index
                .column_names
                .iter()
                .map(|column| format!("`{}`", mysql_escape_identifier(column)))
                .collect::<Vec<_>>()
                .join(", ");
            let prefix = if index.unique { "UNIQUE KEY" } else { "KEY" };
            lines.push(format!(
                "  {prefix} `{}` ({columns})",
                mysql_escape_identifier(&index.index_name)
            ));
        }
        Ok(format!(
            "CREATE TABLE `{}` (\n{}\n) ENGINE=UniDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
            mysql_escape_identifier(&schema.table_name),
            lines.join(",\n")
        ))
    }

    // ── spoofed / introspection ───────────────────────────────────────

    pub(super) fn normalize_sql_whitespace(sql: &str) -> String {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase()
    }

    pub(super) async fn catalog_query_response<C>(
        &self,
        client: &C,
        sql: &str,
    ) -> Option<PgWireResult<Vec<Response>>>
    where
        C: ClientInfo,
    {
        let normalized = Self::normalize_sql_whitespace(sql);
        if normalized.contains("from pg_catalog.pg_database")
            && normalized.contains("pg_get_userbyid")
            && normalized.contains("pg_encoding_to_char")
        {
            let owner = client
                .metadata()
                .get(METADATA_USER)
                .cloned()
                .unwrap_or_else(|| "postgres".to_string());
            let rows = match self.catalog.list_databases().await {
                Ok(databases) => databases
                    .into_iter()
                    .map(|database| {
                        vec![
                            Some(database.database_name),
                            Some(owner.clone()),
                            Some("UTF8".to_string()),
                            Some("C.UTF-8".to_string()),
                            Some("C.UTF-8".to_string()),
                            None,
                            Some("libc".to_string()),
                            None,
                        ]
                    })
                    .collect::<Vec<_>>(),
                Err(error) => return Some(Err(error)),
            };
            return Some(multi_text_row_response(
                &[
                    "Name",
                    "Owner",
                    "Encoding",
                    "Collate",
                    "Ctype",
                    "ICU Locale",
                    "Locale Provider",
                    "Access privileges",
                ],
                &rows,
            ));
        }
        if normalized.contains("from pg_catalog.pg_class c")
            && normalized.contains("pg_catalog.pg_namespace n")
            && normalized.contains("pg_catalog.pg_get_userbyid(c.relowner)")
            && normalized.contains("case c.relkind")
        {
            let rows = match self
                .catalog
                .list_tables(&self.session_catalog(client).database_name)
                .await
            {
                Ok(tables) => tables
                    .into_iter()
                    .map(|table| {
                        vec![
                            Some(table.schema_name),
                            Some(table.table_name),
                            Some("table".to_string()),
                            Some(
                                client
                                    .metadata()
                                    .get(METADATA_USER)
                                    .cloned()
                                    .unwrap_or_else(|| "postgres".to_string()),
                            ),
                        ]
                    })
                    .collect::<Vec<_>>(),
                Err(error) => return Some(Err(error)),
            };
            return Some(multi_text_row_response(
                &["Schema", "Name", "Type", "Owner"],
                &rows,
            ));
        }
        None
    }

    pub(super) fn spoofed_response<C>(
        &self,
        client: &C,
        sql: &str,
    ) -> Option<PgWireResult<Vec<Response>>>
    where
        C: ClientInfo,
    {
        let normalized_sql = strip_pg_catalog_function_qualifiers(sql);
        let normalized = normalized_sql.trim().to_ascii_lowercase();
        let session = self.session_catalog(client);

        match normalized.as_str() {
            "select 1" | "select 1;" => {
                return Some(single_int4_row_response("?column?", 1));
            }
            "select version()" | "select version();" => {
                return Some(single_text_row_response(
                    "version",
                    "PostgreSQL 14.0 (pg_gateway)",
                ));
            }
            "select current_schema()" | "select current_schema();" => {
                return Some(single_text_row_response(
                    "current_schema",
                    &session.schema_name,
                ));
            }
            "select current_database()" | "select current_database();" => {
                return Some(single_text_row_response(
                    "current_database",
                    &session.database_name,
                ));
            }
            "select current_user"
            | "select current_user();"
            | "select session_user"
            | "select session_user;" => {
                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .unwrap_or_else(|| "postgres".to_string());
                let column = if normalized.contains("session_user") {
                    "session_user"
                } else {
                    "current_user"
                };
                return Some(single_text_row_response(column, &user));
            }
            "show search_path" | "show search_path;" => {
                let search_path = client
                    .metadata()
                    .get(METADATA_SEARCH_PATH)
                    .cloned()
                    .unwrap_or_else(|| session.schema_name.clone());
                return Some(single_text_row_response("search_path", &search_path));
            }
            "show transaction isolation level"
            | "show transaction isolation level;"
            | "show transaction_isolation"
            | "show transaction_isolation;" => {
                let isolation = client
                    .metadata()
                    .get(METADATA_TRANSACTION_ISOLATION)
                    .map(String::as_str)
                    .unwrap_or(self.default_transaction_isolation().as_pg_str());
                return Some(single_text_row_response("transaction_isolation", isolation));
            }
            "show standard_conforming_strings" | "show standard_conforming_strings;" => {
                return Some(single_text_row_response(
                    "standard_conforming_strings",
                    "on",
                ));
            }
            "show server_version" | "show server_version;" => {
                return Some(single_text_row_response("server_version", "14.0"));
            }
            "show server_version_num" | "show server_version_num;" => {
                return Some(single_text_row_response("server_version_num", "140000"));
            }
            "show client_encoding" | "show client_encoding;" => {
                return Some(single_text_row_response("client_encoding", "UTF8"));
            }
            "show timezone" | "show timezone;" | "show time zone" | "show time zone;" => {
                return Some(single_text_row_response("TimeZone", "UTC"));
            }
            _ => {}
        }

        if normalized.starts_with("set ") {
            if Self::is_supported_noop_set(&normalized) {
                return Some(Ok(vec![empty_query_response()]));
            }
            return Some(Err(unsupported(format!(
                "session parameter is not supported yet: {sql}"
            ))));
        }

        let implemented_catalog_relation = normalized.contains("pg_catalog.pg_class")
            || normalized.contains("pg_catalog.pg_attribute")
            || normalized.contains("pg_catalog.pg_index")
            || normalized.contains("pg_catalog.pg_constraint")
            || normalized.contains("pg_catalog.pg_proc")
            || normalized.contains("pg_catalog.pg_cast")
            || normalized.contains("pg_catalog.pg_settings")
            || normalized.contains("pg_catalog.pg_tables")
            || normalized.contains("pg_catalog.pg_type")
            || normalized.contains("pg_catalog.pg_authid")
            || normalized.contains("pg_catalog.pg_auth_members")
            || normalized.contains("pg_catalog.pg_roles")
            || normalized.contains("pg_catalog.pg_user")
            || normalized.contains("pg_catalog.pg_group")
            || normalized.contains("pg_catalog.pg_attrdef")
            || normalized.contains("pg_catalog.pg_depend")
            || normalized.contains("pg_catalog.pg_description")
            || normalized.contains("pg_catalog.pg_shdepend")
            || normalized.contains("pg_catalog.pg_shdescription")
            || normalized.contains("pg_catalog.pg_am")
            || normalized.contains("pg_catalog.pg_opclass")
            || normalized.contains("pg_catalog.pg_opfamily")
            || normalized.contains("pg_catalog.pg_operator")
            || normalized.contains("pg_catalog.pg_collation")
            || normalized.contains("pg_catalog.pg_sequence")
            || normalized.contains("pg_catalog.pg_sequences")
            || normalized.contains("pg_catalog.pg_rewrite")
            || normalized.contains("pg_catalog.pg_views")
            || normalized.contains("pg_catalog.pg_trigger")
            || normalized.contains("pg_catalog.pg_policy")
            || normalized.contains("pg_catalog.pg_statistic")
            || normalized.contains("pg_catalog.pg_stats")
            || normalized.contains("pg_catalog.pg_statistic_ext")
            || normalized.contains("pg_catalog.pg_statistic_ext_data")
            || normalized.contains("pg_catalog.pg_stats_ext")
            || normalized.contains("pg_catalog.pg_stats_ext_exprs")
            || normalized.contains("pg_catalog.pg_indexes")
            || normalized.contains("information_schema.tables")
            || normalized.contains("information_schema.columns")
            || normalized.contains("information_schema.schemata")
            || normalized.contains("information_schema.table_constraints")
            || normalized.contains("information_schema.key_column_usage")
            || normalized.contains("information_schema.statistics")
            || normalized.contains("information_schema.referential_constraints")
            || normalized.contains("information_schema.constraint_column_usage")
            || normalized.contains("information_schema.constraint_table_usage")
            || normalized.contains("information_schema.column_privileges")
            || normalized.contains("information_schema.table_privileges")
            || normalized.contains("information_schema.views")
            || normalized.contains("information_schema.engines")
            || normalized.contains("information_schema.character_sets")
            || normalized.contains("information_schema.collations")
            || normalized.contains("information_schema.processlist")
            || normalized.contains("information_schema.global_variables")
            || normalized.contains("information_schema.session_variables")
            || normalized.contains("information_schema.sequences")
            || normalized.contains("information_schema.routines")
            || normalized.contains("information_schema.parameters");
        if (normalized.contains("pg_class") && !implemented_catalog_relation)
            || (normalized.contains("information_schema") && !implemented_catalog_relation)
        {
            return Some(Ok(vec![empty_query_response()]));
        }

        None
    }

    pub(super) fn is_supported_noop_set(normalized: &str) -> bool {
        const SUPPORTED_PREFIXES: &[&str] = &[
            "set application_name",
            "set client_encoding",
            "set standard_conforming_strings",
            "set extra_float_digits",
            "set datestyle",
            "set timezone",
            "set time zone",
            "set statement_timeout",
            "set lock_timeout",
            "set idle_in_transaction_session_timeout",
        ];
        SUPPORTED_PREFIXES
            .iter()
            .any(|prefix| normalized.starts_with(prefix))
    }

    pub(super) fn session_catalog<C>(&self, client: &C) -> SessionCatalog
    where
        C: ClientInfo,
    {
        let database_name = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| DEFAULT_DATABASE_NAME.to_string());
        let schema_name = client
            .metadata()
            .get(METADATA_CURRENT_SCHEMA)
            .cloned()
            .or_else(|| {
                client
                    .metadata()
                    .get(METADATA_SEARCH_PATH)
                    .and_then(|value| parse_search_path(value).into_iter().next())
            })
            .unwrap_or_else(|| self.default_schema_name().to_string());
        let search_path = client
            .metadata()
            .get(METADATA_SEARCH_PATH)
            .map(|value| parse_search_path(value))
            .filter(|schemas| !schemas.is_empty())
            .unwrap_or_else(|| vec![schema_name.clone()]);
        SessionCatalog {
            database_name,
            schema_name,
            search_path,
        }
    }

    pub(super) fn set_session_schema<C>(&self, client: &mut C, schema_name: &str)
    where
        C: ClientInfo,
    {
        client
            .metadata_mut()
            .insert(METADATA_CURRENT_SCHEMA.to_string(), schema_name.to_string());
        client
            .metadata_mut()
            .insert(METADATA_SEARCH_PATH.to_string(), schema_name.to_string());
    }

    pub(super) async fn resolve_startup_database(
        &self,
        requested: Option<String>,
        user: Option<String>,
    ) -> PgWireResult<String> {
        let requested = requested.filter(|value| !value.trim().is_empty());

        let Some(requested) = requested else {
            return Ok(DEFAULT_DATABASE_NAME.to_string());
        };

        if self.catalog.get_database(&requested).await?.is_some() {
            return Ok(requested);
        }

        if requested == DEFAULT_DATABASE_NAME || user.as_deref() == Some(requested.as_str()) {
            return Ok(DEFAULT_DATABASE_NAME.to_string());
        }

        Err(user_error(
            "3D000",
            format!("database '{requested}' does not exist"),
        ))
    }

    pub(super) fn session_id<C>(&self, client: &C) -> i32
    where
        C: ClientInfo,
    {
        client.pid_and_secret_key().0
    }

    pub(super) async fn mark_cancelled(&self, session_id: i32) {
        self.cancelled_sessions.lock().await.insert(session_id);
    }

    pub(super) async fn check_cancelled(&self, session_id: i32) -> PgWireResult<()> {
        if self.cancelled_sessions.lock().await.remove(&session_id) {
            return Err(user_error(
                "57014",
                "canceling statement due to user request",
            ));
        }
        Ok(())
    }

    pub(super) async fn begin_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        self.begin_session_transaction_with_options(client, None, false)
            .await
    }

    pub(super) fn mysql_autocommit_disabled<C>(&self, client: &C) -> bool
    where
        C: ClientInfo,
    {
        self.mode == GatewayMode::MySql
            && client
                .metadata()
                .get(MYSQL_AUTOCOMMIT_METADATA)
                .is_some_and(|value| value == "0" || value.eq_ignore_ascii_case("off"))
    }

    pub(super) async fn restart_mysql_autocommit_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        if self.mysql_autocommit_disabled(client) {
            self.begin_session_transaction(client).await?;
        }
        Ok(())
    }

    pub(super) async fn begin_session_transaction_with_options<C>(
        &self,
        client: &mut C,
        isolation: Option<TransactionIsolation>,
        read_only: bool,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let mut txns = self.active_transactions.lock().await;
        if txns.contains_key(&session_id) {
            if self.mode == GatewayMode::MySql {
                let txn = txns.remove(&session_id);
                drop(txns);
                self.release_session_table_locks(session_id).await;
                self.active_transaction_snapshots
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_transaction_isolations
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_transaction_read_only
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_mysql_current_reads
                    .lock()
                    .await
                    .remove(&session_id);
                if let Some(txn) = txn {
                    txn.commit().await.map_err(|e| user_error("40001", e))?;
                }
                client.set_transaction_status(TransactionStatus::Idle);
                txns = self.active_transactions.lock().await;
            } else {
                if read_only {
                    self.active_transaction_read_only
                        .lock()
                        .await
                        .insert(session_id);
                }
                client.set_transaction_status(TransactionStatus::Transaction);
                return Ok(vec![command_complete("BEGIN")]);
            }
        }
        if txns.contains_key(&session_id) {
            if read_only {
                self.active_transaction_read_only
                    .lock()
                    .await
                    .insert(session_id);
            }
            client.set_transaction_status(TransactionStatus::Transaction);
            return Ok(vec![command_complete("BEGIN")]);
        }
        let txn = self
            .store
            .begin_transaction()
            .await
            .map_err(|e| user_error("XX000", e))?;
        let isolation = isolation
            .or_else(|| {
                client
                    .metadata()
                    .get(METADATA_TRANSACTION_ISOLATION)
                    .and_then(|value| parse_transaction_isolation(value))
            })
            .unwrap_or_else(|| self.default_transaction_isolation());
        txns.insert(session_id, txn);
        self.active_transaction_snapshots
            .lock()
            .await
            .insert(session_id, self.mvcc_clock.load(Ordering::SeqCst));
        self.active_transaction_isolations
            .lock()
            .await
            .insert(session_id, isolation);
        let mut read_only_txns = self.active_transaction_read_only.lock().await;
        if read_only {
            read_only_txns.insert(session_id);
        } else {
            read_only_txns.remove(&session_id);
        }
        client.metadata_mut().insert(
            METADATA_TRANSACTION_ISOLATION.to_string(),
            isolation.as_pg_str().to_string(),
        );
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(vec![command_complete("BEGIN")])
    }

    pub(super) async fn commit_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            let started_at = Instant::now();
            log_copy_profile(format!("session_commit start session={session_id}"));
            match txn.commit().await {
                Ok(()) => {
                    log_copy_profile(format!(
                        "session_commit done session={} elapsed_ms={} success=true",
                        session_id,
                        started_at.elapsed().as_millis()
                    ));
                    client.set_transaction_status(TransactionStatus::Idle);
                    self.restart_mysql_autocommit_transaction(client).await?;
                    Ok(vec![command_complete("COMMIT")])
                }
                Err(error) => {
                    log_copy_profile(format!(
                        "session_commit done session={} elapsed_ms={} success=false error={}",
                        session_id,
                        started_at.elapsed().as_millis(),
                        error
                    ));
                    client.set_transaction_status(TransactionStatus::Error);
                    Err(user_error("40001", error))
                }
            }
        } else {
            client.set_transaction_status(TransactionStatus::Idle);
            self.restart_mysql_autocommit_transaction(client).await?;
            Ok(vec![command_complete("COMMIT")])
        }
    }

    pub(super) async fn rollback_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.rollback().await.map_err(|e| user_error("XX000", e))?;
        }
        client.set_transaction_status(TransactionStatus::Idle);
        self.restart_mysql_autocommit_transaction(client).await?;
        Ok(vec![command_complete("ROLLBACK")])
    }

    pub(crate) async fn rollback_session_by_id(&self, session_id: i32) -> PgWireResult<()> {
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.rollback().await.map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn implicit_commit_active_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.commit().await.map_err(|e| user_error("40001", e))?;
        }
        client.set_transaction_status(TransactionStatus::Idle);
        Ok(())
    }

    pub(super) async fn is_active_transaction_read_only<C>(&self, client: &C) -> bool
    where
        C: ClientInfo,
    {
        self.active_transaction_read_only
            .lock()
            .await
            .contains(&self.session_id(client))
    }

    pub(super) async fn release_session_table_locks(&self, session_id: i32) {
        self.active_mysql_locks
            .lock()
            .await
            .retain(|lock| lock.owner != session_id);
        self.active_mysql_lock_queue
            .lock()
            .await
            .retain(|waiter| waiter.session_id != session_id);
        self.active_mysql_lock_waits
            .lock()
            .await
            .remove(&session_id);
        self.mysql_lock_notify.notify_waiters();
    }

    pub(super) async fn release_session_statement_locks(&self, session_id: i32) {
        self.active_mysql_locks.lock().await.retain(|lock| {
            lock.owner != session_id || lock.duration != MySqlLockDuration::Statement
        });
        self.mysql_lock_notify.notify_waiters();
    }
}

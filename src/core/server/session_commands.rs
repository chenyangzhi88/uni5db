use super::*;

impl GatewayServer {
    pub(super) async fn handle_session_command<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Option<Vec<Response>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let normalized = query.trim().trim_end_matches(';').trim();
        let lower = normalized.to_ascii_lowercase();
        match lower.as_str() {
            "begin" | "start transaction" => {
                return Ok(Some(self.begin_session_transaction(client).await?));
            }
            "commit" | "end" => {
                return Ok(Some(self.commit_session_transaction(client).await?));
            }
            "rollback" => {
                return Ok(Some(self.rollback_session_transaction(client).await?));
            }
            _ => {}
        }
        if let Some(value) = parse_mysql_autocommit_assignment(normalized) {
            let enabled = !(value == "0" || value.eq_ignore_ascii_case("off"));
            client.metadata_mut().insert(
                MYSQL_AUTOCOMMIT_METADATA.to_string(),
                if enabled { "1" } else { "0" }.to_string(),
            );
            if enabled {
                if self.has_active_transaction(client).await {
                    self.commit_session_transaction(client).await?;
                }
            } else if !self.has_active_transaction(client).await {
                self.begin_session_transaction(client).await?;
            }
            return Ok(Some(vec![command_complete("SET")]));
        }
        if lower.starts_with("set innodb_lock_wait_timeout")
            || lower.starts_with("set session innodb_lock_wait_timeout")
            || lower.starts_with("set @@innodb_lock_wait_timeout")
        {
            let value = normalized
                .split_once('=')
                .map(|(_, value)| value.trim().trim_matches('\'').trim_matches('"'))
                .unwrap_or("50");
            let timeout = value
                .parse::<u64>()
                .map_err(|_| user_error("42000", "innodb_lock_wait_timeout must be numeric"))?;
            client.metadata_mut().insert(
                MYSQL_LOCK_WAIT_TIMEOUT_METADATA.to_string(),
                timeout.to_string(),
            );
            return Ok(Some(vec![command_complete("SET")]));
        }
        if (lower.starts_with("begin ") || lower.starts_with("start transaction "))
            && (lower.contains("isolation level")
                || lower.contains("read only")
                || lower.contains("read write")
                || lower.contains("consistent snapshot"))
        {
            let isolation = if lower.contains("isolation level") {
                Some(parse_transaction_isolation(&lower).ok_or_else(|| {
                    user_error("42601", "unsupported transaction isolation level")
                })?)
            } else {
                None
            };
            let read_only = lower.contains("read only");
            return Ok(Some(
                self.begin_session_transaction_with_options(client, isolation, read_only)
                    .await?,
            ));
        }
        if lower.starts_with("set transaction isolation level ")
            || lower.starts_with("set session characteristics as transaction isolation level ")
            || lower.starts_with("set default transaction isolation level ")
        {
            let isolation = parse_transaction_isolation(&lower)
                .ok_or_else(|| user_error("42601", "unsupported transaction isolation level"))?;
            return Ok(Some(
                self.set_session_transaction_isolation(client, isolation)
                    .await?,
            ));
        }
        if let Some(rest) = lower.strip_prefix("savepoint ") {
            let name = parse_savepoint_identifier(&normalized[normalized.len() - rest.len()..])?;
            return Ok(Some(
                self.savepoint_session_transaction(client, &name).await?,
            ));
        }
        if let Some(rest) = lower
            .strip_prefix("rollback to savepoint ")
            .or_else(|| lower.strip_prefix("rollback to "))
        {
            let name = parse_savepoint_identifier(&normalized[normalized.len() - rest.len()..])?;
            return Ok(Some(
                self.rollback_to_session_savepoint(client, &name).await?,
            ));
        }
        if let Some(rest) = lower
            .strip_prefix("release savepoint ")
            .or_else(|| lower.strip_prefix("release "))
        {
            let name = parse_savepoint_identifier(&normalized[normalized.len() - rest.len()..])?;
            return Ok(Some(self.release_session_savepoint(client, &name).await?));
        }
        if let Some(target) = Self::parse_mysql_kill_target(normalized) {
            self.rollback_session_by_id(target).await?;
            return Ok(Some(vec![command_complete("KILL")]));
        }
        if let Some(sequence) = parse_nextval_query(normalized) {
            let session = self.session_catalog(client);
            let value = self
                .allocate_sequence_value(
                    &session.database_name,
                    &session.schema_name,
                    &sequence,
                    &DataType::Int64,
                )
                .await?;
            if let ColumnValue::Int64(value) = value {
                return Ok(Some(single_int8_row_response("nextval", value)?));
            }
        }
        if let Some(rest) = lower
            .strip_prefix("set search_path to ")
            .or_else(|| lower.strip_prefix("set search_path = "))
        {
            let search_path = normalized[normalized.len() - rest.len()..].trim();
            let schemas = parse_search_path(search_path);
            let schema_name = schemas.first().cloned().ok_or_else(|| {
                user_error("42601", "SET search_path requires at least one schema")
            })?;
            let session = self.session_catalog(client);
            let database = self
                .catalog
                .get_database(&session.database_name)
                .await?
                .ok_or_else(|| {
                    user_error(
                        "3D000",
                        format!("database '{}' does not exist", session.database_name),
                    )
                })?;
            for schema in &schemas {
                if self
                    .catalog
                    .get_schema(database.database_id, schema)
                    .await?
                    .is_none()
                {
                    return Err(user_error(
                        "3F000",
                        format!("schema '{schema}' does not exist"),
                    ));
                }
            }
            client
                .metadata_mut()
                .insert(METADATA_CURRENT_SCHEMA.to_string(), schema_name);
            client
                .metadata_mut()
                .insert(METADATA_SEARCH_PATH.to_string(), search_path.to_string());
            return Ok(Some(vec![empty_query_response()]));
        }
        Ok(None)
    }

    pub(super) fn parse_mysql_kill_target(query: &str) -> Option<i32> {
        let mut parts = query.trim().trim_end_matches(';').split_whitespace();
        if !parts.next()?.eq_ignore_ascii_case("kill") {
            return None;
        }
        let next = parts.next()?;
        let id = if next.eq_ignore_ascii_case("connection") || next.eq_ignore_ascii_case("query") {
            parts.next()?
        } else {
            next
        };
        id.parse::<i32>().ok()
    }

    pub(super) fn is_transaction_control_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
        )
    }

    pub(super) fn is_mysql_ddl_implicit_commit_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateDatabase { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateTable(_)
                | Statement::CreateSequence { .. }
                | Statement::CreateView(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::Drop { .. }
                | Statement::Truncate(_)
                | Statement::Analyze(_)
                | Statement::OptimizeTable { .. }
        )
    }

    pub(super) fn is_mysql_locking_read_statement(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Query(query) if !query.locks.is_empty() || query.for_clause.is_some()
        )
    }

    pub(super) fn is_write_plan(plan: &QueryPlan) -> bool {
        !matches!(
            plan,
            QueryPlan::Noop { .. }
                | QueryPlan::SelectRows { .. }
                | QueryPlan::ExplainRows { .. }
                | QueryPlan::PostgresExplainRows { .. }
                | QueryPlan::TableMaintenanceRows { .. }
        )
    }

    pub(super) async fn secondary_index_locks_at(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        index_name: &str,
        index_value: &ColumnValue,
    ) -> PgWireResult<Vec<MySqlLock>> {
        let value = Self::normalize_mysql_index_lock_value(index_name, index_value);
        let entries = self
            .scan_index_row_entries_at(
                Some(session_id),
                database_name,
                schema_name,
                schema,
                index_name,
                index_value,
                None,
            )
            .await?;
        let lock_kind = if entries.is_empty() {
            MySqlLockKind::IndexGap {
                index_name: index_name.to_ascii_lowercase(),
                lower: Some(value.clone()),
                upper: next_key_prefix(value.as_bytes())
                    .map(|value| String::from_utf8_lossy(&value).into_owned()),
            }
        } else {
            MySqlLockKind::IndexNextKey {
                index_name: index_name.to_ascii_lowercase(),
                lower: Some(value.clone()),
                upper: next_key_prefix(value.as_bytes())
                    .map(|value| String::from_utf8_lossy(&value).into_owned()),
            }
        };
        let mut locks = vec![Self::mysql_lock(
            session_id,
            database_name,
            schema_name,
            &schema.table_name,
            lock_kind,
        )];
        locks.extend(entries.into_iter().map(|(pk, _)| {
            Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::Record(Self::normalize_mysql_lock_value(&pk)),
            )
        }));
        Ok(locks)
    }

    pub(super) async fn primary_key_record_or_gap_locks_at(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        key: &ColumnValue,
    ) -> PgWireResult<Vec<MySqlLock>> {
        let value = Self::normalize_mysql_lock_value(key);
        let kind = if self
            .read_visible_row_at(None, database_name, schema_name, schema, key)
            .await?
            .is_some()
        {
            MySqlLockKind::Record(value)
        } else {
            MySqlLockKind::Gap {
                lower: Some(value.clone()),
                upper: next_key_prefix(value.as_bytes())
                    .map(|value| String::from_utf8_lossy(&value).into_owned()),
            }
        };
        Ok(vec![Self::mysql_lock(
            session_id,
            database_name,
            schema_name,
            &schema.table_name,
            kind,
        )])
    }

    pub(super) async fn secondary_index_range_locks_at(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        index_name: &str,
        lower: Option<&(ColumnValue, bool)>,
        upper: Option<&(ColumnValue, bool)>,
    ) -> PgWireResult<Vec<MySqlLock>> {
        let mut locks = vec![Self::mysql_lock(
            session_id,
            database_name,
            schema_name,
            &schema.table_name,
            MySqlLockKind::IndexNextKey {
                index_name: index_name.to_ascii_lowercase(),
                lower: lower
                    .map(|(value, _)| Self::normalize_mysql_index_lock_value(index_name, value)),
                upper: upper
                    .map(|(value, _)| Self::normalize_mysql_index_lock_value(index_name, value)),
            },
        )];
        let entries = self
            .scan_index_row_entries_by_range_at(
                Some(session_id),
                database_name,
                schema_name,
                schema,
                index_name,
                lower.map(|(value, inclusive)| (value, *inclusive)),
                upper.map(|(value, inclusive)| (value, *inclusive)),
                None,
            )
            .await?;
        locks.extend(entries.into_iter().map(|(pk, _)| {
            Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::Record(Self::normalize_mysql_lock_value(&pk)),
            )
        }));
        Ok(locks)
    }

    pub(super) async fn insert_row_locks_at(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        rows: &[RowMap],
    ) -> PgWireResult<Vec<MySqlLock>> {
        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut locks = Vec::new();
        for row in rows {
            if let Some(key) = row.get(&schema.primary_key) {
                locks.push(Self::mysql_lock(
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                    MySqlLockKind::InsertIntention(Self::normalize_mysql_lock_value(key)),
                ));
                locks.push(Self::mysql_lock(
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                    MySqlLockKind::Record(Self::normalize_mysql_lock_value(key)),
                ));
            }
            for index in &indexes {
                let index_value = Self::indexed_value(row, index);
                let value = Self::normalize_mysql_index_lock_value(&index.index_name, &index_value);
                locks.push(Self::mysql_lock(
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                    MySqlLockKind::IndexInsertIntention {
                        index_name: index.index_name.to_ascii_lowercase(),
                        value: value.clone(),
                    },
                ));
                locks.push(Self::mysql_lock(
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                    MySqlLockKind::IndexRecord {
                        index_name: index.index_name.to_ascii_lowercase(),
                        value,
                    },
                ));
            }
        }
        Ok(locks)
    }

    pub(super) async fn read_access_locks(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        access: &ReadAccess,
    ) -> PgWireResult<Vec<MySqlLock>> {
        match access {
            ReadAccess::PointLookup { key } => {
                self.primary_key_record_or_gap_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    key,
                )
                .await
            }
            ReadAccess::PrimaryKeyInLookup { keys } => {
                let mut locks = Vec::new();
                for key in keys {
                    locks.extend(
                        self.primary_key_record_or_gap_locks_at(
                            session_id,
                            database_name,
                            schema_name,
                            schema,
                            key,
                        )
                        .await?,
                    );
                }
                Ok(locks)
            }
            ReadAccess::PrimaryKeyRangeScan { lower, upper, .. } => Ok(vec![Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::NextKey {
                    lower: lower
                        .as_ref()
                        .map(|(value, _)| Self::normalize_mysql_lock_value(value)),
                    upper: upper
                        .as_ref()
                        .map(|(value, _)| Self::normalize_mysql_lock_value(value)),
                },
            )]),
            ReadAccess::SecondaryIndexLookup {
                index_name, key, ..
            } => {
                self.secondary_index_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    index_name,
                    key,
                )
                .await
            }
            ReadAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                ..
            } => {
                self.secondary_index_range_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    index_name,
                    lower.as_ref(),
                    upper.as_ref(),
                )
                .await
            }
            ReadAccess::PrefixScan { .. } => Ok(vec![Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::Table,
            )]),
        }
    }

    pub(super) async fn write_access_locks(
        &self,
        session_id: i32,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        access: &WriteAccess,
    ) -> PgWireResult<Vec<MySqlLock>> {
        match access {
            WriteAccess::PointLookup { key } => {
                self.primary_key_record_or_gap_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    key,
                )
                .await
            }
            WriteAccess::PrimaryKeyRangeScan { lower, upper, .. } => Ok(vec![Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::NextKey {
                    lower: lower
                        .as_ref()
                        .map(|(value, _)| Self::normalize_mysql_lock_value(value)),
                    upper: upper
                        .as_ref()
                        .map(|(value, _)| Self::normalize_mysql_lock_value(value)),
                },
            )]),
            WriteAccess::SecondaryIndexLookup {
                index_name, key, ..
            } => {
                self.secondary_index_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    index_name,
                    key,
                )
                .await
            }
            WriteAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                ..
            } => {
                self.secondary_index_range_locks_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    index_name,
                    lower.as_ref(),
                    upper.as_ref(),
                )
                .await
            }
            WriteAccess::PrefixScan { .. } => Ok(vec![Self::mysql_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::Table,
            )]),
        }
    }

    pub(super) async fn plan_lock_targets(
        &self,
        session_id: i32,
        plan: &QueryPlan,
        locking_read: bool,
    ) -> PgWireResult<Vec<MySqlLock>> {
        Ok(match plan {
            QueryPlan::CreateDatabase { database_name, .. } => vec![Self::mysql_database_lock(
                session_id,
                database_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateSchema {
                database_name,
                schema_name,
                ..
            } => vec![Self::mysql_schema_lock(
                session_id,
                database_name,
                schema_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateSequence {
                database_name,
                schema_name,
                sequence_name,
                ..
            } => vec![Self::mysql_sequence_lock(
                session_id,
                database_name,
                schema_name,
                sequence_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateView {
                database_name,
                schema_name,
                view_name,
                ..
            } => vec![Self::mysql_view_lock(
                session_id,
                database_name,
                schema_name,
                view_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateTable {
                database_name,
                schema_name,
                schema,
                ..
            } => vec![Self::mysql_statement_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateTableAs {
                database_name,
                schema_name,
                schema,
                ..
            } => vec![Self::mysql_statement_lock(
                session_id,
                database_name,
                schema_name,
                &schema.table_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::InsertRows {
                database_name,
                schema_name,
                schema,
                rows,
                ..
            } => {
                let mut locks = self
                    .insert_row_locks_at(session_id, database_name, schema_name, schema, rows)
                    .await?;
                Self::add_metadata_read_lock(
                    &mut locks,
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                );
                return Ok(locks);
            }
            QueryPlan::SelectRows {
                database_name,
                schema_name,
                schema,
                access,
                ..
            } if locking_read => {
                let mut locks = self
                    .read_access_locks(session_id, database_name, schema_name, schema, access)
                    .await?;
                Self::add_metadata_read_lock(
                    &mut locks,
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                );
                return Ok(locks);
            }
            QueryPlan::UpdateRows {
                database_name,
                schema_name,
                schema,
                access,
                ..
            } => {
                let mut locks = self
                    .write_access_locks(session_id, database_name, schema_name, schema, access)
                    .await?;
                Self::add_metadata_read_lock(
                    &mut locks,
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                );
                return Ok(locks);
            }
            QueryPlan::DeleteRows {
                database_name,
                schema_name,
                schema,
                access,
                ..
            } => {
                let mut locks = self
                    .write_access_locks(session_id, database_name, schema_name, schema, access)
                    .await?;
                Self::add_metadata_read_lock(
                    &mut locks,
                    session_id,
                    database_name,
                    schema_name,
                    &schema.table_name,
                );
                return Ok(locks);
            }
            QueryPlan::AlterTableAddPrimaryKey {
                database_name,
                schema_name,
                table_name,
                ..
            }
            | QueryPlan::AlterTable {
                database_name,
                schema_name,
                table_name,
                ..
            } => vec![Self::mysql_statement_lock(
                session_id,
                database_name,
                schema_name,
                table_name,
                MySqlLockKind::MetadataWrite,
            )],
            QueryPlan::CreateIndex {
                database_name,
                schema_name,
                table_name,
                index_name,
                ..
            } => vec![
                Self::mysql_statement_lock(
                    session_id,
                    database_name,
                    schema_name,
                    table_name,
                    MySqlLockKind::MetadataWrite,
                ),
                Self::mysql_index_lock(
                    session_id,
                    database_name,
                    schema_name,
                    index_name,
                    MySqlLockKind::MetadataWrite,
                ),
            ],
            QueryPlan::DropTables {
                database_name,
                tables,
                ..
            } => tables
                .iter()
                .map(|(schema_name, table_name)| {
                    Self::mysql_statement_lock(
                        session_id,
                        database_name,
                        schema_name,
                        table_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            QueryPlan::DropIndexes {
                database_name,
                indexes,
                ..
            } => {
                let mut locks = Vec::new();
                let database = self.catalog.get_database(database_name).await?;
                for (schema_name, index_name) in indexes {
                    locks.push(Self::mysql_index_lock(
                        session_id,
                        database_name,
                        schema_name,
                        index_name,
                        MySqlLockKind::MetadataWrite,
                    ));
                    let Some(database) = &database else {
                        continue;
                    };
                    let Some(schema_meta) = self
                        .catalog
                        .get_schema(database.database_id, schema_name)
                        .await?
                    else {
                        continue;
                    };
                    if let Some(index) = self
                        .catalog
                        .get_index(database.database_id, schema_meta.schema_id, index_name)
                        .await?
                    {
                        locks.push(Self::mysql_statement_lock(
                            session_id,
                            database_name,
                            schema_name,
                            &index.table_name,
                            MySqlLockKind::MetadataWrite,
                        ));
                    }
                }
                locks
            }
            QueryPlan::DropSequences {
                database_name,
                sequences,
                ..
            } => sequences
                .iter()
                .map(|(schema_name, sequence_name)| {
                    Self::mysql_sequence_lock(
                        session_id,
                        database_name,
                        schema_name,
                        sequence_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            QueryPlan::DropViews {
                database_name,
                views,
                ..
            } => views
                .iter()
                .map(|(schema_name, view_name)| {
                    Self::mysql_view_lock(
                        session_id,
                        database_name,
                        schema_name,
                        view_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            QueryPlan::DropSchemas {
                database_name,
                schemas,
                ..
            } => schemas
                .iter()
                .map(|schema_name| {
                    Self::mysql_schema_lock(
                        session_id,
                        database_name,
                        schema_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            QueryPlan::DropDatabases { databases, .. } => databases
                .iter()
                .map(|database_name| {
                    Self::mysql_database_lock(
                        session_id,
                        database_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            QueryPlan::TruncateTables {
                database_name,
                tables,
            } => tables
                .iter()
                .map(|(schema_name, schema)| {
                    Self::mysql_statement_lock(
                        session_id,
                        database_name,
                        schema_name,
                        &schema.table_name,
                        MySqlLockKind::MetadataWrite,
                    )
                })
                .collect(),
            _ => Vec::new(),
        })
    }

    pub(super) async fn acquire_plan_table_locks<C>(
        &self,
        client: &C,
        plan: &QueryPlan,
        locking_read: bool,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        self.acquire_mysql_locks(
            client,
            self.plan_lock_targets(session_id, plan, locking_read)
                .await?,
        )
        .await
    }

    pub(super) async fn handle_transaction_statement<C>(
        &self,
        client: &mut C,
        stmt: &Statement,
    ) -> PgWireResult<Option<Response>>
    where
        C: ClientInfo,
    {
        match stmt {
            Statement::StartTransaction { .. } => Ok(Some(
                self.begin_session_transaction(client)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("BEGIN")),
            )),
            Statement::Commit { .. } => Ok(Some(
                self.commit_session_transaction(client)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("COMMIT")),
            )),
            Statement::Rollback {
                savepoint: Some(name),
                ..
            } => Ok(Some(
                self.rollback_to_session_savepoint(client, &name.value)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("ROLLBACK")),
            )),
            Statement::Rollback { .. } => Ok(Some(
                self.rollback_session_transaction(client)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("ROLLBACK")),
            )),
            Statement::Savepoint { name } => Ok(Some(
                self.savepoint_session_transaction(client, &name.value)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("SAVEPOINT")),
            )),
            Statement::ReleaseSavepoint { name } => Ok(Some(
                self.release_session_savepoint(client, &name.value)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| command_complete("RELEASE")),
            )),
            _ => Ok(None),
        }
    }
}

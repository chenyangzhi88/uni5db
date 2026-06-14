use super::*;

impl GatewayServer {
    pub(super) async fn qualify_sql_for_search_path(
        &self,
        sql: &str,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<String> {
        let mut statements = self.parse_sql(sql)?;
        for statement in &mut statements {
            self.qualify_statement_for_search_path(statement, database_name, search_path)
                .await?;
        }
        Ok(statements
            .into_iter()
            .map(|statement| statement.to_string())
            .collect::<Vec<_>>()
            .join("; "))
    }

    pub(super) async fn qualify_statement_for_search_path(
        &self,
        statement: &mut Statement,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        match statement {
            Statement::Query(query) => {
                Box::pin(self.qualify_query_for_search_path(query, database_name, search_path))
                    .await?
            }
            Statement::Insert(insert) => {
                if let TableObject::TableName(table_name) = &mut insert.table {
                    self.qualify_object_name_for_search_path(
                        table_name,
                        database_name,
                        search_path,
                    )
                    .await?;
                }
                if let Some(source) = &mut insert.source {
                    Box::pin(self.qualify_query_for_search_path(
                        source,
                        database_name,
                        search_path,
                    ))
                    .await?;
                }
            }
            Statement::Update(update) => {
                Box::pin(self.qualify_table_with_joins_for_search_path(
                    &mut update.table,
                    database_name,
                    search_path,
                ))
                .await?
            }
            Statement::Delete(delete) => {
                let from = match &mut delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(from)
                    | sqlparser::ast::FromTable::WithoutKeyword(from) => from,
                };
                for table in from {
                    Box::pin(self.qualify_table_with_joins_for_search_path(
                        table,
                        database_name,
                        search_path,
                    ))
                    .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) async fn qualify_query_for_search_path(
        &self,
        query: &mut Query,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        query.locks.clear();
        query.for_clause = None;
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                Box::pin(self.qualify_query_for_search_path(
                    &mut cte.query,
                    database_name,
                    search_path,
                ))
                .await?;
            }
        }
        Box::pin(self.qualify_set_expr_for_search_path(&mut query.body, database_name, search_path))
            .await
    }

    pub(super) async fn qualify_set_expr_for_search_path(
        &self,
        set_expr: &mut SetExpr,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        match set_expr {
            SetExpr::Select(select) => {
                for table in &mut select.from {
                    Box::pin(self.qualify_table_with_joins_for_search_path(
                        table,
                        database_name,
                        search_path,
                    ))
                    .await?;
                }
            }
            SetExpr::Query(query) => {
                Box::pin(self.qualify_query_for_search_path(query, database_name, search_path))
                    .await?;
            }
            SetExpr::SetOperation { left, right, .. } => {
                Box::pin(self.qualify_set_expr_for_search_path(left, database_name, search_path))
                    .await?;
                Box::pin(self.qualify_set_expr_for_search_path(right, database_name, search_path))
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) async fn qualify_table_with_joins_for_search_path(
        &self,
        table: &mut TableWithJoins,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        Box::pin(self.qualify_table_factor_for_search_path(
            &mut table.relation,
            database_name,
            search_path,
        ))
        .await?;
        for join in &mut table.joins {
            Box::pin(self.qualify_table_factor_for_search_path(
                &mut join.relation,
                database_name,
                search_path,
            ))
            .await?;
        }
        Ok(())
    }

    pub(super) async fn qualify_table_factor_for_search_path(
        &self,
        relation: &mut sqlparser::ast::TableFactor,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                self.qualify_object_name_for_search_path(name, database_name, search_path)
                    .await?;
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                Box::pin(self.qualify_query_for_search_path(subquery, database_name, search_path))
                    .await?;
            }
            sqlparser::ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                Box::pin(self.qualify_table_with_joins_for_search_path(
                    table_with_joins,
                    database_name,
                    search_path,
                ))
                .await?;
            }
            sqlparser::ast::TableFactor::Pivot { table, .. }
            | sqlparser::ast::TableFactor::Unpivot { table, .. } => {
                Box::pin(self.qualify_table_factor_for_search_path(
                    table,
                    database_name,
                    search_path,
                ))
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) async fn qualify_object_name_for_search_path(
        &self,
        name: &mut ObjectName,
        database_name: &str,
        search_path: &[String],
    ) -> PgWireResult<()> {
        let name_text = object_name_to_string(name)?;
        if is_pg_catalog_relation_name(&name_text) {
            let parsed = self.parse_sql(&format!("SELECT * FROM pg_catalog.{name_text}"))?;
            if let Some(Statement::Query(query)) = parsed.into_iter().next()
                && let SetExpr::Select(select) = *query.body
                && let Some(table) = select.from.first()
                && let sqlparser::ast::TableFactor::Table { name: resolved, .. } = &table.relation
            {
                *name = resolved.clone();
            }
            return Ok(());
        }
        if name_text.contains('.') {
            return Ok(());
        }

        let Some(qualified_name) = self
            .resolve_table_name_for_search_path(database_name, search_path, &name_text)
            .await?
        else {
            return Ok(());
        };
        let parsed = self.parse_sql(&format!("SELECT * FROM {qualified_name}"))?;
        if let Some(Statement::Query(query)) = parsed.into_iter().next()
            && let SetExpr::Select(select) = *query.body
            && let Some(table) = select.from.first()
            && let sqlparser::ast::TableFactor::Table { name: resolved, .. } = &table.relation
        {
            *name = resolved.clone();
        }
        Ok(())
    }

    // ── schema inference for schema-less INSERT ──────────────────────

    pub(super) async fn resolve_insert_schema(
        &self,
        database_name: &str,
        default_schema_name: &str,
        table_name: &str,
        existing: Option<TableSchema>,
        columns: &[Ident],
        rows: &[Vec<Expr>],
    ) -> PgWireResult<TableSchema> {
        if let Some(schema) = existing {
            return Ok(schema);
        }

        let declared_columns = if !columns.is_empty() {
            columns.iter().map(|c| c.value.clone()).collect::<Vec<_>>()
        } else {
            let width = rows.first().map(Vec::len).unwrap_or_default();
            (0..width).map(|i| format!("col{}", i + 1)).collect()
        };

        let primary_key = declared_columns
            .iter()
            .find(|c| c.as_str() == "id")
            .cloned()
            .ok_or_else(|| {
                unsupported("INSERT into a schema-less table requires an 'id' column")
            })?;

        let (schema_name, table_name) = if table_name.contains('.') {
            resolve_table_reference(table_name)?
        } else {
            (default_schema_name.to_string(), table_name.to_string())
        };
        let database = self
            .catalog
            .get_database(database_name)
            .await?
            .ok_or_else(|| {
                user_error(
                    "3D000",
                    format!("database '{database_name}' does not exist"),
                )
            })?;
        if self
            .catalog
            .get_schema(database.database_id, &schema_name)
            .await?
            .is_none()
        {
            return Err(user_error(
                "3F000",
                format!("schema '{schema_name}' does not exist"),
            ));
        }
        let table_id = self.catalog.allocate_table_id().await?;

        let mut schema = TableSchema {
            table_name,
            table_id,
            schema_version: 1,
            table_epoch: 1,
            primary_key: primary_key.clone(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            columns: declared_columns
                .into_iter()
                .enumerate()
                .map(|(idx, name)| ColumnSchema {
                    column_id: idx as u32 + 1,
                    primary_key: name == primary_key,
                    nullable: name != primary_key,
                    data_type: DataType::Text,
                    name,
                    default: None,
                    on_update: None,
                    character_set: None,
                    collation: None,
                })
                .collect(),
        };
        schema.normalize_descriptor();
        Ok(schema)
    }

    pub(crate) async fn run_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.run_query_with_format(client, query, FieldFormat::Text)
            .await
    }

    pub(super) async fn handle_postgres_vacuum_command<C>(
        &self,
        client: &C,
        query: &str,
    ) -> PgWireResult<Option<Vec<Response>>>
    where
        C: ClientInfo,
    {
        if self.mode == GatewayMode::MySql {
            return Ok(None);
        }
        let normalized = Self::normalize_sql_whitespace(query)
            .trim_end_matches(';')
            .trim()
            .to_string();
        if !normalized.starts_with("vacuum") {
            return Ok(None);
        }
        if !Self::vacuum_requests_analyze(&normalized) {
            return Ok(Some(vec![command_complete("VACUUM")]));
        }

        let session = self.session_catalog(client);
        let names = self.vacuum_analyze_table_names(query)?;
        let plan = self
            .plan_postgres_analyze(&session.database_name, &session.search_path, names)
            .await?;
        let response = self
            .execute_plan(plan, Some(self.session_id(client)), FieldFormat::Text)
            .await?;
        Ok(Some(vec![response]))
    }

    pub(super) fn vacuum_requests_analyze(normalized_vacuum_sql: &str) -> bool {
        normalized_vacuum_sql
            .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
            .any(|token| token.eq_ignore_ascii_case("analyze"))
    }

    pub(super) fn vacuum_analyze_table_names(
        &self,
        query: &str,
    ) -> PgWireResult<Option<Vec<ObjectName>>> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let Some(first_token) = trimmed.split_whitespace().next() else {
            return Ok(None);
        };
        if !first_token.eq_ignore_ascii_case("vacuum") {
            return Ok(None);
        }
        let mut rest = trimmed[first_token.len()..].trim_start();
        if rest.starts_with('(') {
            if let Some(end) = rest.find(')') {
                rest = rest[end + 1..].trim_start();
            }
        } else if rest
            .get(..7)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("analyze"))
        {
            rest = rest[7..].trim_start();
        }

        while let Some((keyword, remaining)) = Self::take_vacuum_option_keyword(rest) {
            let _ = keyword;
            rest = remaining.trim_start();
        }

        if rest.is_empty() {
            return Ok(None);
        }
        let table_name = rest
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_matches(|ch| ch == ',' || ch == ';')
            .trim();
        if table_name.is_empty() {
            return Ok(None);
        }
        let statements = self.parse_sql(&format!("ANALYZE {table_name}"))?;
        let Some(Statement::Analyze(analyze)) = statements.into_iter().next() else {
            return Err(user_error("42601", "invalid VACUUM ANALYZE target"));
        };
        Ok(analyze.table_name.map(|name| vec![name]))
    }

    pub(super) fn take_vacuum_option_keyword(rest: &str) -> Option<(&str, &str)> {
        for keyword in ["analyze", "verbose", "freeze", "full"] {
            if rest
                .get(..keyword.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
                && rest[keyword.len()..]
                    .chars()
                    .next()
                    .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
            {
                return Some((&rest[..keyword.len()], &rest[keyword.len()..]));
            }
        }
        None
    }

    pub(super) async fn run_query_with_format<C>(
        &self,
        client: &mut C,
        query: &str,
        result_format: FieldFormat,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let current_session_id = self.session_id(client);
        self.check_cancelled(current_session_id).await?;
        if let Some(response) = self.handle_session_command(client, query).await? {
            return Ok(response);
        }
        if let Some(response) = self.catalog_query_response(client, query).await {
            return response;
        }
        if let Some(response) = self.handle_postgres_vacuum_command(client, query).await? {
            return Ok(response);
        }
        if let Some(response) = self.spoofed_response(client, query) {
            return response;
        }

        let mut statements = self.parse_sql(query)?;
        let session = self.session_catalog(client);
        let in_transaction = self.has_active_transaction(client).await;
        if statements.len() == 1 {
            match statements[0].clone() {
                Statement::Copy {
                    source:
                        CopySource::Table {
                            table_name,
                            columns,
                        },
                    to: false,
                    target: CopyTarget::Stdin,
                    options,
                    legacy_options,
                    values,
                } => {
                    if !values.is_empty() {
                        return Err(unsupported("COPY FROM STDIN VALUES is not supported yet"));
                    }
                    let copy_options = Self::parse_copy_in_options(&options, &legacy_options)?;
                    return self
                        .begin_copy_from_stdin(
                            client,
                            &session,
                            table_name,
                            columns,
                            copy_options,
                            in_transaction,
                        )
                        .await;
                }
                Statement::Prepare {
                    name, statement, ..
                } => {
                    return self
                        .prepare_sql_statement(self.session_id(client), &name.value, &statement)
                        .await;
                }
                Statement::Deallocate { name, .. } => {
                    return self
                        .deallocate_sql_statement(self.session_id(client), &name.value)
                        .await;
                }
                Statement::Execute {
                    name, parameters, ..
                } => {
                    let name = name
                        .as_ref()
                        .map(|name| name.to_string())
                        .unwrap_or_default();
                    let execution = self
                        .prepare_sql_execution(current_session_id, &name, &parameters)
                        .await?;
                    statements = match execution {
                        PreparedSqlExecution::Statement(statement) => vec![statement],
                        PreparedSqlExecution::Sql(sql) => self.parse_sql(&sql)?,
                    };
                }
                _ => {}
            }
        }
        let implicit_transaction = statements.len() > 1
            && !in_transaction
            && !statements
                .iter()
                .any(Self::is_transaction_control_statement);
        if implicit_transaction {
            self.begin_session_transaction(client).await?;
        }

        let session_id = Some(self.session_id(client));
        let mut responses = Vec::with_capacity(statements.len().max(1));
        for stmt in statements {
            self.check_cancelled(current_session_id).await?;
            let is_query_statement = matches!(stmt, Statement::Query(_));
            let ddl_implicit_commit = self.mode == GatewayMode::MySql
                && Self::is_mysql_ddl_implicit_commit_statement(&stmt);
            let locking_read =
                self.mode == GatewayMode::MySql && Self::is_mysql_locking_read_statement(&stmt);
            if ddl_implicit_commit && self.has_active_transaction(client).await {
                self.implicit_commit_active_transaction(client).await?;
            }
            if let Some(response) = self.handle_transaction_statement(client, &stmt).await? {
                responses.push(response);
                continue;
            }
            match self.plan_statement(&session, stmt).await {
                Ok(plan) => {
                    if self.is_active_transaction_read_only(client).await
                        && Self::is_write_plan(&plan)
                    {
                        client.set_transaction_status(TransactionStatus::Error);
                        return Err(user_error(
                            "25006",
                            "cannot execute write statement in a read-only transaction",
                        ));
                    }
                    let mysql_locking_plan = self.mode == GatewayMode::MySql
                        && (locking_read || Self::is_write_plan(&plan));
                    let hold_mysql_locks = self.has_active_transaction(client).await;
                    if mysql_locking_plan {
                        self.acquire_plan_table_locks(client, &plan, locking_read)
                            .await?;
                        self.enter_mysql_current_read(self.session_id(client)).await;
                    }
                    let execute_result = self.execute_plan(plan, session_id, result_format).await;
                    if mysql_locking_plan {
                        let session_id = self.session_id(client);
                        self.exit_mysql_current_read(session_id).await;
                        if hold_mysql_locks {
                            self.release_session_statement_locks(session_id).await;
                        } else {
                            self.release_session_table_locks(session_id).await;
                        }
                    }
                    responses.push(execute_result?);
                    if ddl_implicit_commit {
                        self.restart_mysql_autocommit_transaction(client).await?;
                    }
                }
                Err(e) if is_unsupported_error(&e) => {
                    if implicit_transaction {
                        client.set_transaction_status(TransactionStatus::Error);
                        let _ = self.rollback_session_transaction(client).await;
                        return Err(user_error(
                            "0A000",
                            "slow-path statements are not supported inside pg_gateway implicit multi-statement transactions yet",
                        ));
                    }
                    if self.has_active_transaction(client).await && !is_query_statement {
                        client.set_transaction_status(TransactionStatus::Error);
                        return Err(user_error(
                            "0A000",
                            "slow-path statements are not supported inside pg_gateway transactions yet",
                        ));
                    }
                    return self.execute_via_datafusion(query, &session).await;
                }
                Err(e) => {
                    if self.has_active_transaction(client).await || implicit_transaction {
                        client.set_transaction_status(TransactionStatus::Error);
                    }
                    if implicit_transaction {
                        let _ = self.rollback_session_transaction(client).await;
                    }
                    return Err(e);
                }
            }
        }
        if implicit_transaction {
            if let Err(e) = self.commit_session_transaction(client).await {
                let _ = self.rollback_session_transaction(client).await;
                return Err(e);
            }
        }
        if responses.is_empty() {
            responses.push(empty_query_response());
        }
        Ok(responses)
    }
}

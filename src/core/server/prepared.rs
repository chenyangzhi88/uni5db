use super::*;

impl GatewayServer {
    pub(super) fn parse_sql(&self, sql: &str) -> PgWireResult<Vec<Statement>> {
        parser::parse_sql(self.mode.sql_dialect(), sql)
    }

    pub(super) fn quote_sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub(super) fn dollar_quote_tag_len(sql: &str, start: usize) -> Option<usize> {
        let bytes = sql.as_bytes();
        if bytes.get(start) != Some(&b'$') {
            return None;
        }
        let mut end = start + 1;
        while let Some(byte) = bytes.get(end) {
            match byte {
                b'$' => return Some(end - start + 1),
                b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' => end += 1,
                _ => return None,
            }
        }
        None
    }

    pub(super) fn replace_pg_parameter_placeholders(sql: &str, parameters: &[String]) -> String {
        let bytes = sql.as_bytes();
        let mut out = String::with_capacity(sql.len());
        let mut idx = 0;
        while idx < bytes.len() {
            match bytes[idx] {
                b'\'' => {
                    let start = idx;
                    idx += 1;
                    while idx < bytes.len() {
                        if bytes[idx] == b'\'' {
                            idx += 1;
                            if bytes.get(idx) == Some(&b'\'') {
                                idx += 1;
                                continue;
                            }
                            break;
                        }
                        idx += 1;
                    }
                    out.push_str(&sql[start..idx]);
                }
                b'"' => {
                    let start = idx;
                    idx += 1;
                    while idx < bytes.len() {
                        if bytes[idx] == b'"' {
                            idx += 1;
                            if bytes.get(idx) == Some(&b'"') {
                                idx += 1;
                                continue;
                            }
                            break;
                        }
                        idx += 1;
                    }
                    out.push_str(&sql[start..idx]);
                }
                b'-' if bytes.get(idx + 1) == Some(&b'-') => {
                    let start = idx;
                    idx += 2;
                    while idx < bytes.len() && bytes[idx] != b'\n' {
                        idx += 1;
                    }
                    out.push_str(&sql[start..idx]);
                }
                b'/' if bytes.get(idx + 1) == Some(&b'*') => {
                    let start = idx;
                    idx += 2;
                    while idx + 1 < bytes.len() {
                        if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                            idx += 2;
                            break;
                        }
                        idx += 1;
                    }
                    out.push_str(&sql[start..idx]);
                }
                b'$' if bytes.get(idx + 1).is_some_and(u8::is_ascii_digit) => {
                    let start = idx;
                    idx += 1;
                    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                        idx += 1;
                    }
                    let number = sql[start + 1..idx].parse::<usize>().ok();
                    if let Some(value) = number
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|n| parameters.get(n))
                    {
                        out.push_str(value);
                    } else {
                        out.push_str(&sql[start..idx]);
                    }
                }
                b'$' => {
                    if let Some(tag_len) = Self::dollar_quote_tag_len(sql, idx) {
                        let start = idx;
                        let tag = &sql[idx..idx + tag_len];
                        idx += tag_len;
                        if let Some(end) = sql[idx..].find(tag) {
                            idx += end + tag_len;
                        } else {
                            idx = bytes.len();
                        }
                        out.push_str(&sql[start..idx]);
                    } else {
                        out.push('$');
                        idx += 1;
                    }
                }
                _ => {
                    let ch = sql[idx..].chars().next().unwrap();
                    out.push(ch);
                    idx += ch.len_utf8();
                }
            }
        }
        out
    }

    pub(super) fn type_prefers_unquoted_literal(pg_type: &Type) -> bool {
        matches!(
            *pg_type,
            Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::OID
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::NUMERIC
                | Type::BOOL
        )
    }

    pub(super) fn render_text_parameter(raw: &[u8], pg_type: &Type) -> PgWireResult<String> {
        let value = std::str::from_utf8(raw)
            .map_err(|e| user_error("22021", format!("parameter is not valid UTF-8: {e}")))?;
        if Self::type_prefers_unquoted_literal(pg_type) {
            Ok(value.to_string())
        } else {
            Ok(Self::quote_sql_literal(value))
        }
    }

    pub(super) fn render_binary_parameter(
        portal: &Portal<String>,
        idx: usize,
        pg_type: &Type,
    ) -> PgWireResult<String> {
        match *pg_type {
            Type::INT2 => portal
                .parameter::<i16>(idx, pg_type)
                .map(|value| value.map_or_else(|| "NULL".to_string(), |v| v.to_string())),
            Type::INT4 => portal
                .parameter::<i32>(idx, pg_type)
                .map(|value| value.map_or_else(|| "NULL".to_string(), |v| v.to_string())),
            Type::INT8 => portal
                .parameter::<i64>(idx, pg_type)
                .map(|value| value.map_or_else(|| "NULL".to_string(), |v| v.to_string())),
            Type::FLOAT4 => portal
                .parameter::<f32>(idx, pg_type)
                .map(|value| value.map_or_else(|| "NULL".to_string(), |v| v.to_string())),
            Type::FLOAT8 => portal
                .parameter::<f64>(idx, pg_type)
                .map(|value| value.map_or_else(|| "NULL".to_string(), |v| v.to_string())),
            Type::BOOL => portal.parameter::<bool>(idx, pg_type).map(|value| {
                value.map_or_else(
                    || "NULL".to_string(),
                    |v| if v { "true" } else { "false" }.to_string(),
                )
            }),
            _ => portal.parameter::<String>(idx, pg_type).map(|value| {
                value.map_or_else(|| "NULL".to_string(), |v| Self::quote_sql_literal(&v))
            }),
        }
    }

    pub(super) fn render_portal_parameter(
        portal: &Portal<String>,
        idx: usize,
    ) -> PgWireResult<String> {
        let Some(parameter) = portal.parameters.get(idx) else {
            return Err(user_error(
                "08P01",
                format!("missing bind parameter ${}", idx + 1),
            ));
        };
        let Some(raw) = parameter else {
            return Ok("NULL".to_string());
        };
        let pg_type = portal
            .statement
            .parameter_types
            .get(idx)
            .and_then(|t| t.clone())
            .unwrap_or(Type::UNKNOWN);
        if portal.parameter_format.is_binary(idx) {
            Self::render_binary_parameter(portal, idx, &pg_type)
        } else {
            Self::render_text_parameter(raw, &pg_type)
        }
    }

    pub(super) fn bind_portal_sql(portal: &Portal<String>) -> PgWireResult<String> {
        if portal.parameters.is_empty() {
            return Ok(portal.statement.statement.clone());
        }
        let parameters = (0..portal.parameters.len())
            .map(|idx| Self::render_portal_parameter(portal, idx))
            .collect::<PgWireResult<Vec<_>>>()?;
        Ok(Self::replace_pg_parameter_placeholders(
            &portal.statement.statement,
            &parameters,
        ))
    }

    pub(super) fn portal_result_format(portal: &Portal<String>) -> FieldFormat {
        match &portal.result_column_format {
            Format::UnifiedText => FieldFormat::Text,
            Format::UnifiedBinary => FieldFormat::Binary,
            Format::Individual(formats) => formats
                .first()
                .copied()
                .map(FieldFormat::from)
                .unwrap_or(FieldFormat::Text),
        }
    }

    pub(super) fn render_execute_parameter(expr: &Expr) -> PgWireResult<String> {
        match expr {
            Expr::Value(value) => match &value.value {
                sqlparser::ast::Value::SingleQuotedString(value)
                | sqlparser::ast::Value::DoubleQuotedString(value) => {
                    Ok(Self::quote_sql_literal(value))
                }
                sqlparser::ast::Value::Null => Ok("NULL".to_string()),
                _ => Ok(expr.to_string()),
            },
            Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("null") => {
                Ok("NULL".to_string())
            }
            _ => Ok(expr.to_string()),
        }
    }

    pub(super) async fn prepare_sql_statement(
        &self,
        session_id: i32,
        name: &str,
        statement: &Statement,
    ) -> PgWireResult<Vec<Response>> {
        self.active_sql_prepared
            .lock()
            .await
            .entry(session_id)
            .or_default()
            .insert(
                name.to_ascii_lowercase(),
                PreparedSqlStatement {
                    sql: statement.to_string(),
                    statement: statement.clone(),
                },
            );
        Ok(vec![command_complete("PREPARE")])
    }

    pub(super) async fn deallocate_sql_statement(
        &self,
        session_id: i32,
        name: &str,
    ) -> PgWireResult<Vec<Response>> {
        if name.eq_ignore_ascii_case("all") {
            self.active_sql_prepared.lock().await.remove(&session_id);
        } else if let Some(statements) = self.active_sql_prepared.lock().await.get_mut(&session_id)
        {
            statements.remove(&name.to_ascii_lowercase());
        }
        Ok(vec![command_complete("DEALLOCATE")])
    }

    pub(super) async fn prepare_sql_execution(
        &self,
        session_id: i32,
        name: &str,
        parameters: &[Expr],
    ) -> PgWireResult<PreparedSqlExecution> {
        let prepared = self
            .active_sql_prepared
            .lock()
            .await
            .get(&session_id)
            .and_then(|statements| statements.get(&name.to_ascii_lowercase()).cloned())
            .ok_or_else(|| {
                user_error(
                    "26000",
                    format!("prepared statement '{name}' does not exist"),
                )
            })?;
        if parameters.is_empty() {
            return Ok(PreparedSqlExecution::Statement(prepared.statement));
        }
        let rendered = parameters
            .iter()
            .map(Self::render_execute_parameter)
            .collect::<PgWireResult<Vec<_>>>()?;
        Ok(PreparedSqlExecution::Sql(
            Self::replace_pg_parameter_placeholders(&prepared.sql, &rendered),
        ))
    }

    pub(super) fn placeholder_defaults(types: &[Option<Type>]) -> Vec<String> {
        types
            .iter()
            .map(|pg_type| match pg_type.as_ref() {
                Some(pg_type) if Self::type_prefers_unquoted_literal(pg_type) => {
                    if *pg_type == Type::BOOL {
                        "false".to_string()
                    } else {
                        "0".to_string()
                    }
                }
                _ => "NULL".to_string(),
            })
            .collect()
    }

    pub(super) fn describe_fields_from_responses(responses: &[Response]) -> Vec<FieldInfo> {
        responses
            .iter()
            .find_map(|response| match response {
                Response::Query(query) => Some(query.row_schema().as_ref().clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    pub(super) fn describe_fields_from_plan(
        plan: &QueryPlan,
        format: FieldFormat,
    ) -> Vec<FieldInfo> {
        match plan {
            QueryPlan::SelectRows {
                schema, projection, ..
            } => field_infos_for_projection(schema, projection, format),
            QueryPlan::InsertRows {
                schema,
                returning: Some(projection),
                ..
            }
            | QueryPlan::UpdateRows {
                schema,
                returning: Some(projection),
                ..
            }
            | QueryPlan::DeleteRows {
                schema,
                returning: Some(projection),
                ..
            } => field_infos_for_returning_projection(schema, projection, format),
            QueryPlan::ExplainRows { .. } => [
                "id",
                "select_type",
                "table",
                "partitions",
                "type",
                "possible_keys",
                "key",
                "key_len",
                "ref",
                "rows",
                "filtered",
                "Extra",
            ]
            .into_iter()
            .map(|name| FieldInfo::new(name.into(), None, None, Type::TEXT, format))
            .collect(),
            QueryPlan::PostgresExplainRows { .. } => {
                vec![FieldInfo::new(
                    "QUERY PLAN".into(),
                    None,
                    None,
                    Type::TEXT,
                    format,
                )]
            }
            QueryPlan::TableMaintenanceRows { .. } => ["Table", "Op", "Msg_type", "Msg_text"]
                .into_iter()
                .map(|name| FieldInfo::new(name.into(), None, None, Type::TEXT, format))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub(super) async fn describe_sql_fields<C>(
        &self,
        client: &C,
        sql: &str,
        format: FieldFormat,
    ) -> PgWireResult<Vec<FieldInfo>>
    where
        C: ClientInfo,
    {
        if let Some(response) = self.catalog_query_response(client, sql).await {
            return response.map(|responses| Self::describe_fields_from_responses(&responses));
        }
        if let Some(response) = self.spoofed_response(client, sql) {
            return response.map(|responses| Self::describe_fields_from_responses(&responses));
        }

        let statements = self.parse_sql(sql)?;
        if statements.len() != 1 {
            return Ok(Vec::new());
        }
        let statement = statements.into_iter().next().unwrap();
        let is_query_statement = matches!(statement, Statement::Query(_));
        let session = self.session_catalog(client);
        match self.plan_statement(&session, statement).await {
            Ok(plan) => Ok(Self::describe_fields_from_plan(&plan, format)),
            Err(e) if is_unsupported_error(&e) && is_query_statement => self
                .execute_via_datafusion(sql, &session)
                .await
                .map(|responses| Self::describe_fields_from_responses(&responses)),
            Err(e) if is_unsupported_error(&e) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    // ── planning ──────────────────────────────────────────────────────

    pub(super) async fn plan_statement(
        &self,
        session: &SessionCatalog,
        stmt: Statement,
    ) -> PgWireResult<QueryPlan> {
        self.catalog.ensure_bootstrap().await?;
        match stmt {
            Statement::CreateView(create_view) => {
                if create_view.materialized {
                    return Err(unsupported("materialized views are not supported yet"));
                }
                self.plan_create_view(
                    &session.database_name,
                    &session.schema_name,
                    create_view.name,
                    *create_view.query,
                    create_view.or_replace,
                    create_view.if_not_exists,
                )
                .await
            }
            Statement::CreateRole { .. } => Err(unsupported("CREATE ROLE is not supported yet")),
            Statement::Grant { .. } => Err(unsupported("GRANT is not supported yet")),
            Statement::Revoke { .. } => Err(unsupported("REVOKE is not supported yet")),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => self.plan_create_database(db_name, if_not_exists).await,
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => {
                self.plan_create_schema(&session.database_name, schema_name, if_not_exists)
                    .await
            }
            Statement::CreateTable(create_table) => {
                let auto_increment_start =
                    mysql_table_option_auto_increment(&create_table.table_options.to_string());
                if let Some(query) = create_table.query {
                    return self
                        .plan_create_table_as(
                            &session.database_name,
                            &session.schema_name,
                            &session.search_path,
                            create_table.name,
                            create_table.if_not_exists,
                            *query,
                        )
                        .await;
                }
                if let Some(like) = create_table.like {
                    return self
                        .plan_create_table_like(
                            &session.database_name,
                            &session.schema_name,
                            &session.search_path,
                            create_table.name,
                            create_table.if_not_exists,
                            like,
                            auto_increment_start,
                        )
                        .await;
                }
                self.plan_create_table(
                    &session.database_name,
                    &session.schema_name,
                    &session.search_path,
                    create_table.name,
                    create_table.columns,
                    create_table.constraints,
                    create_table.if_not_exists,
                    auto_increment_start,
                )
                .await
            }
            Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                sequence_options,
                ..
            } => {
                if temporary {
                    return Err(unsupported("temporary sequences are not supported yet"));
                }
                self.plan_create_sequence(
                    &session.database_name,
                    &session.schema_name,
                    name,
                    if_not_exists,
                    sequence_options,
                )
                .await
            }
            Statement::AlterTable(alter_table) => {
                self.plan_alter_table(
                    &session.database_name,
                    &session.search_path,
                    alter_table.name,
                    alter_table.operations,
                )
                .await
            }
            Statement::CreateIndex(create_index) => {
                self.plan_create_index(
                    &session.database_name,
                    &session.search_path,
                    create_index.name,
                    create_index.table_name,
                    create_index.using,
                    create_index.columns,
                    create_index.unique,
                    create_index.concurrently,
                    create_index.if_not_exists,
                    create_index.include,
                    create_index.predicate,
                )
                .await
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => match object_type {
                ObjectType::Table => {
                    self.plan_drop_table(
                        &session.database_name,
                        &session.search_path,
                        names,
                        if_exists,
                    )
                    .await
                }
                ObjectType::Index => {
                    self.plan_drop_index(
                        &session.database_name,
                        &session.search_path,
                        names,
                        if_exists,
                    )
                    .await
                }
                ObjectType::Sequence => {
                    self.plan_drop_sequence(
                        &session.database_name,
                        &session.schema_name,
                        names,
                        if_exists,
                    )
                    .await
                }
                ObjectType::View => {
                    self.plan_drop_view(
                        &session.database_name,
                        &session.schema_name,
                        names,
                        if_exists,
                    )
                    .await
                }
                ObjectType::Schema => {
                    self.plan_drop_schema(&session.database_name, names, if_exists)
                        .await
                }
                ObjectType::Role => Ok(QueryPlan::DropDatabases {
                    databases: Vec::new(),
                    if_exists: true,
                }),
                _ => Err(unsupported("DROP object type is not supported yet")),
            },
            Statement::Truncate(truncate) => {
                self.plan_truncate_table(
                    &session.database_name,
                    &session.search_path,
                    truncate
                        .table_names
                        .into_iter()
                        .map(|target| target.name)
                        .collect(),
                )
                .await
            }
            Statement::Analyze(analyze) => {
                if self.mode == GatewayMode::MySql {
                    let Some(table_name) = analyze.table_name else {
                        return Err(unsupported("bare ANALYZE is not supported yet"));
                    };
                    self.plan_mysql_table_maintenance(
                        &session.database_name,
                        &session.search_path,
                        "analyze",
                        vec![table_name],
                    )
                    .await
                } else {
                    self.plan_postgres_analyze(
                        &session.database_name,
                        &session.search_path,
                        analyze.table_name.map(|name| vec![name]),
                    )
                    .await
                }
            }
            Statement::Vacuum(_) => {
                if self.mode == GatewayMode::MySql {
                    return Err(unsupported("VACUUM is not supported in MySQL mode"));
                }
                Ok(QueryPlan::Noop {
                    tag: "VACUUM".to_string(),
                })
            }
            Statement::OptimizeTable { name, .. } => {
                self.plan_mysql_table_maintenance(
                    &session.database_name,
                    &session.search_path,
                    "optimize",
                    vec![name],
                )
                .await
            }
            Statement::Explain { statement, .. } => {
                let plan = Box::pin(self.plan_statement(&session, *statement)).await?;
                if self.mode == GatewayMode::MySql {
                    let rows = self.mysql_explain_rows(&plan).await?;
                    Ok(QueryPlan::ExplainRows { rows })
                } else {
                    let rows = self.postgres_explain_rows(&plan).await?;
                    Ok(QueryPlan::PostgresExplainRows { rows })
                }
            }
            Statement::Insert(insert) => {
                let table_name = match insert.table {
                    TableObject::TableName(name) => name.to_string(),
                    _ => return Err(unsupported("INSERT fast-path requires a table name")),
                };
                let columns = Self::insert_columns_to_idents(insert.columns)?;
                self.plan_insert(
                    &session.database_name,
                    &session.search_path,
                    table_name,
                    columns,
                    insert.source.map(|source| *source),
                    insert.assignments,
                    insert.on,
                    insert.returning,
                    insert.ignore,
                    insert.replace_into,
                )
                .await
            }
            Statement::Query(query) => {
                self.plan_select(&session.database_name, &session.search_path, *query)
                    .await
            }
            Statement::Update(update) => {
                self.plan_update(
                    &session.database_name,
                    &session.search_path,
                    update.table,
                    update.assignments,
                    update.selection,
                    update.returning,
                    update.order_by,
                    update.limit,
                )
                .await
            }
            Statement::Delete(delete) => {
                let from = match delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(from)
                    | sqlparser::ast::FromTable::WithoutKeyword(from) => from,
                };
                self.plan_delete(
                    &session.database_name,
                    &session.search_path,
                    from,
                    delete.selection,
                    delete.returning,
                    delete.order_by,
                    delete.limit,
                )
                .await
            }
            other => Err(unsupported(format!(
                "statement not supported in fast path: {other}"
            ))),
        }
    }
}

use super::*;

impl GatewayServer {
    pub(super) fn insert_columns_to_idents(columns: Vec<ObjectName>) -> PgWireResult<Vec<Ident>> {
        columns
            .into_iter()
            .map(|name| {
                if name.0.len() == 1 {
                    name.0[0].as_ident().cloned().ok_or_else(|| {
                        unsupported("INSERT fast-path only supports identifier column names")
                    })
                } else {
                    Err(unsupported(
                        "INSERT fast-path only supports unqualified column names",
                    ))
                }
            })
            .collect()
    }

    pub(super) async fn plan_insert(
        &self,
        database_name: &str,
        search_path: &[String],
        table_name: String,
        columns: Vec<Ident>,
        source: Option<Query>,
        assignments: Vec<Assignment>,
        on: Option<OnInsert>,
        returning: Option<Vec<sqlparser::ast::SelectItem>>,
        ignore: bool,
        replace_into: bool,
    ) -> PgWireResult<QueryPlan> {
        if ignore && replace_into {
            return Err(unsupported("INSERT IGNORE cannot be combined with REPLACE"));
        }
        let (schema_name, existing_schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let values_rows = source
            .as_ref()
            .and_then(|source| extract_insert_values(source).ok());
        let schema = self
            .resolve_insert_schema(
                database_name,
                &schema_name,
                &table_name,
                existing_schema,
                &columns,
                values_rows.as_deref().unwrap_or(&[]),
            )
            .await?;

        let row_maps = if !assignments.is_empty() {
            if source.is_some() || !columns.is_empty() {
                return Err(unsupported(
                    "INSERT ... SET cannot be combined with column list or VALUES",
                ));
            }
            vec![
                self.build_insert_row_from_assignments_at(
                    database_name,
                    &schema_name,
                    &schema,
                    assignments,
                )
                .await?,
            ]
        } else if let Some(rows) = values_rows {
            let mut row_maps = Vec::with_capacity(rows.len());
            for row in rows {
                row_maps.push(
                    self.build_insert_row_at(database_name, &schema_name, &schema, &columns, row)
                        .await?,
                );
            }
            row_maps
        } else {
            let source = source
                .ok_or_else(|| unsupported("INSERT fast-path requires VALUES, SELECT, or SET"))?;
            let session = SessionCatalog {
                database_name: database_name.to_string(),
                schema_name: schema_name.clone(),
                search_path: search_path.to_vec(),
            };
            let batches = self
                .collect_datafusion_batches(&source.to_string(), &session)
                .await?;
            let target_columns = if columns.is_empty() {
                schema.column_names()
            } else {
                columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
            };
            self.datafusion_rows_for_schema(&batches, &schema, &target_columns)?
        };

        let on_conflict = self
            .resolve_insert_on_conflict(&schema, on, ignore, replace_into)
            .await?;
        let returning = returning
            .as_ref()
            .map(|items| resolve_returning_projection(items, &schema))
            .transpose()?;

        Ok(QueryPlan::InsertRows {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            rows: row_maps,
            on_conflict,
            returning,
        })
    }

    pub(super) async fn plan_truncate_table(
        &self,
        database_name: &str,
        search_path: &[String],
        names: Vec<ObjectName>,
    ) -> PgWireResult<QueryPlan> {
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let qualified_name = object_name_to_string(&name)?;
            let (schema_name, table_schema) = if qualified_name.contains('.') {
                let (schema_name, table_name) = resolve_table_reference(&qualified_name)?;
                let exists = self
                    .catalog
                    .load_table(database_name, &schema_name, &table_name)
                    .await?;
                let Some(existing) = exists else {
                    return Err(user_error(
                        "42P01",
                        format!("table '{qualified_name}' does not exist"),
                    ));
                };
                (schema_name, existing.schema)
            } else {
                let (resolved_schema, existing) = self
                    .resolve_table_schema(database_name, search_path, &qualified_name)
                    .await?;
                let Some(existing) = existing else {
                    return Err(user_error(
                        "42P01",
                        format!("table '{qualified_name}' does not exist"),
                    ));
                };
                (resolved_schema, existing)
            };
            tables.push((schema_name, table_schema));
        }

        Ok(QueryPlan::TruncateTables {
            database_name: database_name.to_string(),
            tables,
        })
    }

    pub(super) async fn plan_mysql_table_maintenance(
        &self,
        database_name: &str,
        search_path: &[String],
        op: &str,
        names: Vec<ObjectName>,
    ) -> PgWireResult<QueryPlan> {
        if self.mode != GatewayMode::MySql {
            return Err(unsupported(format!(
                "{} TABLE is not supported yet",
                op.to_ascii_uppercase()
            )));
        }
        let mut rows = Vec::with_capacity(names.len());
        for name in names {
            let (_schema_name, table_schema) = self
                .resolve_existing_table_schema(database_name, search_path, &name)
                .await?;
            rows.push(vec![
                Some(format!("{database_name}.{}", table_schema.table_name)),
                Some(op.to_string()),
                Some("status".to_string()),
                Some("OK".to_string()),
            ]);
        }
        Ok(QueryPlan::TableMaintenanceRows { rows })
    }

    pub(super) async fn plan_postgres_analyze(
        &self,
        database_name: &str,
        search_path: &[String],
        names: Option<Vec<ObjectName>>,
    ) -> PgWireResult<QueryPlan> {
        let tables = if let Some(names) = names {
            let mut tables = Vec::with_capacity(names.len());
            for name in names {
                let (schema_name, table_schema) = self
                    .resolve_existing_table_schema(database_name, search_path, &name)
                    .await?;
                tables.push((schema_name, table_schema));
            }
            tables
        } else {
            self.catalog
                .list_tables(database_name)
                .await?
                .into_iter()
                .map(|table| (table.schema_name, table.schema))
                .collect()
        };
        Ok(QueryPlan::AnalyzeTables {
            database_name: database_name.to_string(),
            tables,
        })
    }

    pub(super) async fn resolve_existing_table_schema(
        &self,
        database_name: &str,
        search_path: &[String],
        name: &ObjectName,
    ) -> PgWireResult<(String, TableSchema)> {
        let qualified_name = object_name_to_string(name)?;
        if qualified_name.contains('.') {
            let (schema_name, table_name) = resolve_table_reference(&qualified_name)?;
            let Some(existing) = self
                .catalog
                .load_table(database_name, &schema_name, &table_name)
                .await?
            else {
                return Err(user_error(
                    "42P01",
                    format!("table '{qualified_name}' does not exist"),
                ));
            };
            Ok((schema_name, existing.schema))
        } else {
            let (resolved_schema, existing) = self
                .resolve_table_schema(database_name, search_path, &qualified_name)
                .await?;
            let Some(existing) = existing else {
                return Err(user_error(
                    "42P01",
                    format!("table '{qualified_name}' does not exist"),
                ));
            };
            Ok((resolved_schema, existing))
        }
    }

    pub(super) async fn mysql_explain_rows(
        &self,
        plan: &QueryPlan,
    ) -> PgWireResult<Vec<Vec<Option<String>>>> {
        match plan {
            QueryPlan::SelectRows {
                schema,
                access,
                limit,
                ..
            } => {
                let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
                let (access_type, key, possible_keys, rows, extra) =
                    self.mysql_explain_access(schema, access, &indexes, *limit);
                Ok(vec![vec![
                    Some("1".to_string()),
                    Some("SIMPLE".to_string()),
                    Some(schema.table_name.clone()),
                    None,
                    Some(access_type),
                    possible_keys,
                    key,
                    None,
                    None,
                    Some(rows.to_string()),
                    Some("100.00".to_string()),
                    extra,
                ]])
            }
            _ => Err(unsupported(
                "EXPLAIN currently supports SELECT statements in MySQL mode",
            )),
        }
    }

    pub(super) async fn postgres_explain_rows(
        &self,
        plan: &QueryPlan,
    ) -> PgWireResult<Vec<Vec<Option<String>>>> {
        let QueryPlan::SelectRows {
            schema,
            access,
            limit,
            offset,
            ..
        } = plan
        else {
            return Err(unsupported(
                "EXPLAIN currently supports SELECT statements in PostgreSQL mode",
            ));
        };

        let indexes = self.catalog.list_indexes_for_table(schema.table_id).await?;
        let mut lines = match access {
            ReadAccess::PointLookup { key } => vec![
                format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                ),
                format!(
                    "  Index Cond: ({} = {})",
                    schema.primary_key,
                    Self::postgres_explain_value(key)
                ),
            ],
            ReadAccess::PrimaryKeyInLookup { keys } => vec![
                format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                ),
                format!(
                    "  Index Cond: ({} = ANY ({{{}}}))",
                    schema.primary_key,
                    keys.iter()
                        .map(Self::postgres_explain_value)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ],
            ReadAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {}_pkey on {}",
                    schema.table_name, schema.table_name
                )];
                if let Some(cond) =
                    Self::postgres_explain_range_condition(&schema.primary_key, lower, upper)
                {
                    lines.push(format!("  Index Cond: {cond}"));
                }
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::SecondaryIndexLookup {
                index_name,
                column_name,
                key,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {index_name} on {}",
                    schema.table_name
                )];
                lines.push(format!(
                    "  Index Cond: ({column_name} = {})",
                    Self::postgres_explain_value(key)
                ));
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::SecondaryIndexRangeScan {
                index_name,
                column_name,
                lower,
                upper,
                filter,
            } => {
                let mut lines = vec![format!(
                    "Index Scan using {index_name} on {}",
                    schema.table_name
                )];
                if let Some(cond) =
                    Self::postgres_explain_range_condition(column_name, lower, upper)
                {
                    lines.push(format!("  Index Cond: {cond}"));
                }
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
            ReadAccess::PrefixScan { filter } => {
                let possible_index = indexes.iter().find(|index| {
                    index.unique && index.column_names == vec![schema.primary_key.clone()]
                });
                let mut lines = if possible_index.is_some() && schema.has_user_primary_key() {
                    vec![format!("Seq Scan on {}", schema.table_name)]
                } else {
                    vec![format!("Seq Scan on {}", schema.table_name)]
                };
                if let Some(filter) = filter {
                    lines.push(format!("  Filter: ({filter})"));
                }
                lines
            }
        };

        if let Some(limit) = limit {
            lines.insert(0, format!("Limit: {limit}"));
        }
        if *offset > 0 {
            lines.insert(0, format!("Offset: {offset}"));
        }

        Ok(lines.into_iter().map(|line| vec![Some(line)]).collect())
    }

    pub(super) fn postgres_explain_value(value: &ColumnValue) -> String {
        match value {
            ColumnValue::Null => "NULL".to_string(),
            ColumnValue::Text(value)
            | ColumnValue::Date(value)
            | ColumnValue::Timestamp(value)
            | ColumnValue::TimestampTz(value)
            | ColumnValue::Uuid(value)
            | ColumnValue::Json(value)
            | ColumnValue::Jsonb(value)
            | ColumnValue::Numeric(value) => format!("'{}'", value.replace('\'', "''")),
            ColumnValue::Boolean(value) => value.to_string(),
            ColumnValue::Bytea(bytes) => format!("'\\x{}'", Self::postgres_explain_hex(bytes)),
            ColumnValue::Array(values) => format!(
                "{{{}}}",
                values
                    .iter()
                    .map(Self::postgres_explain_value)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => value.to_text().unwrap_or_else(|| format!("{value:?}")),
        }
    }

    pub(super) fn postgres_explain_hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().saturating_mul(2));
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "{byte:02x}");
        }
        out
    }

    pub(super) fn postgres_explain_range_condition(
        column_name: &str,
        lower: &Option<(ColumnValue, bool)>,
        upper: &Option<(ColumnValue, bool)>,
    ) -> Option<String> {
        let mut parts = Vec::new();
        if let Some((value, inclusive)) = lower {
            parts.push(format!(
                "({column_name} {} {})",
                if *inclusive { ">=" } else { ">" },
                Self::postgres_explain_value(value)
            ));
        }
        if let Some((value, inclusive)) = upper {
            parts.push(format!(
                "({column_name} {} {})",
                if *inclusive { "<=" } else { "<" },
                Self::postgres_explain_value(value)
            ));
        }
        (!parts.is_empty()).then(|| parts.join(" AND "))
    }

    pub(super) fn mysql_explain_access(
        &self,
        schema: &TableSchema,
        access: &ReadAccess,
        indexes: &[IndexCatalog],
        limit: Option<usize>,
    ) -> (
        String,
        Option<String>,
        Option<String>,
        usize,
        Option<String>,
    ) {
        let possible_keys = self.mysql_possible_keys(schema, indexes);
        match access {
            ReadAccess::PointLookup { .. } | ReadAccess::PrimaryKeyInLookup { keys: _ } => (
                "const".to_string(),
                Some("PRIMARY".to_string()),
                possible_keys,
                1,
                None,
            ),
            ReadAccess::PrimaryKeyRangeScan { filter, .. } => (
                "range".to_string(),
                Some("PRIMARY".to_string()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::SecondaryIndexLookup {
                index_name, filter, ..
            } => (
                "ref".to_string(),
                Some(index_name.clone()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::SecondaryIndexRangeScan {
                index_name, filter, ..
            } => (
                "range".to_string(),
                Some(index_name.clone()),
                possible_keys,
                limit.unwrap_or(1).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
            ReadAccess::PrefixScan { filter } => (
                "ALL".to_string(),
                None,
                possible_keys,
                limit.unwrap_or(1000).max(1),
                filter.as_ref().map(|_| "Using where".to_string()),
            ),
        }
    }

    pub(super) fn mysql_possible_keys(
        &self,
        schema: &TableSchema,
        indexes: &[IndexCatalog],
    ) -> Option<String> {
        let mut keys = Vec::new();
        if schema.has_user_primary_key() {
            keys.push("PRIMARY".to_string());
        }
        keys.extend(indexes.iter().map(|index| index.index_name.clone()));
        (!keys.is_empty()).then(|| keys.join(","))
    }

    pub(super) async fn resolve_insert_on_conflict(
        &self,
        schema: &TableSchema,
        on: Option<OnInsert>,
        ignore: bool,
        replace_into: bool,
    ) -> PgWireResult<Option<InsertConflictAction>> {
        if replace_into {
            let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
            if target_column_sets.is_empty() {
                return Ok(None);
            }
            return Ok(Some(InsertConflictAction::ReplaceAnyUnique {
                target_column_sets,
            }));
        }
        if ignore {
            let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
            if target_column_sets.is_empty() {
                return Ok(None);
            }
            return Ok(Some(InsertConflictAction::DoNothingAnyUnique {
                target_column_sets,
            }));
        }
        let Some(on) = on else {
            return Ok(None);
        };

        let (target_columns, action) = match on {
            OnInsert::OnConflict(on_conflict) => {
                let target_columns = match on_conflict.conflict_target {
                    None => {
                        if !schema.has_user_primary_key() {
                            return Err(unsupported(
                                "ON CONFLICT without target requires a user-defined primary key in fast path",
                            ));
                        }
                        vec![schema.primary_key.clone()]
                    }
                    Some(ConflictTarget::Columns(columns)) => {
                        if columns.is_empty() {
                            return Err(unsupported("ON CONFLICT target cannot be empty"));
                        }
                        columns
                            .into_iter()
                            .map(|identifier| identifier.value)
                            .collect::<Vec<_>>()
                    }
                    Some(ConflictTarget::OnConstraint(constraint_name)) => {
                        let constraint_name = object_name_to_string(&constraint_name)?;
                        let constraint = schema
                            .unique_constraints
                            .iter()
                            .find(|constraint| {
                                constraint.name.eq_ignore_ascii_case(&constraint_name)
                            })
                            .ok_or_else(|| {
                                user_error(
                                    "42704",
                                    format!("constraint '{constraint_name}' does not exist"),
                                )
                            })?;
                        if constraint.columns.is_empty() {
                            return Err(unsupported(
                                "ON CONFLICT ON CONSTRAINT requires constraint with at least one column",
                            ));
                        }
                        constraint.columns.clone()
                    }
                };
                let action = match on_conflict.action {
                    AstOnConflictAction::DoNothing => InsertConflictAction::DoNothing,
                    AstOnConflictAction::DoUpdate(do_update) => {
                        let assignments = do_update
                            .assignments
                            .into_iter()
                            .map(|assignment| {
                                self.resolve_insert_conflict_assignment(
                                    schema,
                                    assignment,
                                    &target_columns,
                                )
                            })
                            .collect::<PgWireResult<Vec<_>>>()?;
                        InsertConflictAction::DoUpdate {
                            target_columns: target_columns.clone(),
                            assignments,
                            selection: do_update.selection,
                        }
                    }
                };
                (target_columns, action)
            }
            OnInsert::DuplicateKeyUpdate(assignments) => {
                let assignments = assignments
                    .into_iter()
                    .map(|assignment| {
                        self.resolve_insert_conflict_assignment(schema, assignment, &[])
                    })
                    .collect::<PgWireResult<Vec<_>>>()?;
                let target_column_sets = self.mysql_duplicate_key_targets(schema).await?;
                if target_column_sets.is_empty() {
                    return Err(unsupported(
                        "ON DUPLICATE KEY UPDATE requires a primary key, unique constraint, or unique index",
                    ));
                }
                (
                    target_column_sets[0].clone(),
                    InsertConflictAction::DoUpdateAnyUnique {
                        target_column_sets,
                        assignments,
                    },
                )
            }
            _ => {
                return Err(unsupported(
                    "only ON CONFLICT and ON DUPLICATE KEY UPDATE are supported in fast path",
                ));
            }
        };

        if !target_columns
            .iter()
            .all(|column| schema.find_column(column).is_some())
        {
            return Err(unsupported("upsert target column not found"));
        }

        let target_is_keyed = if matches!(action, InsertConflictAction::DoUpdateAnyUnique { .. }) {
            true
        } else if matches!(
            action,
            InsertConflictAction::DoNothingAnyUnique { .. }
                | InsertConflictAction::ReplaceAnyUnique { .. }
        ) {
            true
        } else if target_columns == [schema.primary_key.clone()] {
            true
        } else {
            schema
                .unique_constraints
                .iter()
                .any(|constraint| constraint.primary_key || constraint.columns == target_columns)
        };

        if !target_is_keyed {
            return Err(unsupported(
                "upsert target must match the primary key or a unique constraint",
            ));
        }

        Ok(Some(action))
    }

    pub(super) async fn mysql_duplicate_key_targets(
        &self,
        schema: &TableSchema,
    ) -> PgWireResult<Vec<Vec<String>>> {
        let mut targets = Vec::new();
        if schema.has_user_primary_key() {
            targets.push(vec![schema.primary_key.clone()]);
        }
        for constraint in &schema.unique_constraints {
            if constraint.columns.is_empty() {
                continue;
            }
            if !targets.iter().any(|target| target == &constraint.columns) {
                targets.push(constraint.columns.clone());
            }
        }
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            if !index.unique || index.column_names.is_empty() {
                continue;
            }
            if !targets.iter().any(|target| target == &index.column_names) {
                targets.push(index.column_names);
            }
        }
        Ok(targets)
    }

    pub(super) fn resolve_insert_conflict_assignment(
        &self,
        schema: &TableSchema,
        assignment: Assignment,
        target_columns: &[String],
    ) -> PgWireResult<InsertConflictAssignment> {
        let column = extract_assignment_column(&assignment)?;
        let _target_column = schema
            .find_column(&column)
            .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))?;
        if target_columns.iter().any(|name| name == &column) {
            return Err(unsupported(
                "ON CONFLICT DO UPDATE cannot update the conflict target columns in fast path",
            ));
        }

        Ok(InsertConflictAssignment {
            column,
            value: assignment.value,
        })
    }

    pub(super) fn resolve_update_assignment(
        &self,
        schema: &TableSchema,
        assignment: Assignment,
    ) -> PgWireResult<UpdateAssignment> {
        let column = extract_assignment_column(&assignment)?;
        schema
            .find_column(&column)
            .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))?;

        Ok(UpdateAssignment::Expr {
            column,
            expr: assignment.value,
        })
    }

    pub(super) fn extract_indexable_eq_filter(
        &self,
        selection: Option<&Expr>,
        schema: &TableSchema,
    ) -> PgWireResult<Option<(String, ColumnValue)>> {
        let Some(expr) = selection else {
            return Ok(None);
        };
        match expr {
            Expr::BinaryOp { left, op, right } if *op == sqlparser::ast::BinaryOperator::Eq => {
                let column_name = expr_identifier_name(left)?;
                let Some(column) = schema.find_column(&column_name) else {
                    return Ok(None);
                };
                let value = sql_expr_to_column_value(right, &column.data_type)?;
                Ok(Some((column_name, value)))
            }
            Expr::Nested(expr) => self.extract_indexable_eq_filter(Some(expr), schema),
            _ => Ok(None),
        }
    }

    pub(super) async fn extract_indexable_range_filter(
        &self,
        selection: Option<&Expr>,
        schema: &TableSchema,
    ) -> PgWireResult<
        Option<(
            IndexCatalog,
            String,
            Option<(ColumnValue, bool)>,
            Option<(ColumnValue, bool)>,
        )>,
    > {
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            if index.column_names.len() != 1 || index.column_names[0] == schema.primary_key {
                continue;
            }
            let column_name = index.column_names[0].clone();
            let Some(column) = schema.find_column(&column_name) else {
                continue;
            };
            if let Some((lower, upper)) =
                extract_column_range_filter(selection, &column_name, &column.data_type)?
            {
                return Ok(Some((index, column_name, lower, upper)));
            }
        }
        Ok(None)
    }

    pub(super) fn extract_primary_key_in_filter(
        &self,
        selection: Option<&Expr>,
        schema: &TableSchema,
    ) -> PgWireResult<Option<Vec<ColumnValue>>> {
        let Some(expr) = selection else {
            return Ok(None);
        };
        match expr {
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let column_name = expr_identifier_name(expr)?;
                if column_name != schema.primary_key {
                    return Ok(None);
                }
                let Some(column) = schema.find_column(&column_name) else {
                    return Ok(None);
                };
                let mut seen = HashSet::new();
                let mut keys = Vec::new();
                for value_expr in list {
                    let value = sql_expr_to_column_value(value_expr, &column.data_type)?;
                    if value.is_null() {
                        continue;
                    }
                    let encoded = storage_layout::encode_key_value(&value);
                    if seen.insert(encoded) {
                        keys.push(value);
                    }
                }
                Ok(Some(keys))
            }
            Expr::Nested(expr) => self.extract_primary_key_in_filter(Some(expr), schema),
            _ => Ok(None),
        }
    }

    pub(super) fn fast_path_limit_offset(
        limit_clause: Option<&LimitClause>,
    ) -> PgWireResult<(Option<usize>, usize)> {
        let Some(limit_clause) = limit_clause else {
            return Ok((None, 0));
        };
        match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return Err(unsupported("LIMIT BY is not supported in fast path"));
                }
                let limit = limit
                    .as_ref()
                    .map(|expr| Self::fast_path_usize_expr(expr, "LIMIT"))
                    .transpose()?;
                let offset = offset
                    .as_ref()
                    .map(|offset| Self::fast_path_usize_expr(&offset.value, "OFFSET"))
                    .transpose()?
                    .unwrap_or(0);
                Ok((limit, offset))
            }
            LimitClause::OffsetCommaLimit { offset, limit } => Ok((
                Some(Self::fast_path_usize_expr(limit, "LIMIT")?),
                Self::fast_path_usize_expr(offset, "OFFSET")?,
            )),
        }
    }

    pub(super) fn fast_path_usize_expr(expr: &Expr, clause: &str) -> PgWireResult<usize> {
        let value = sql_expr_to_column_value(expr, &DataType::Int64)?;
        let value = match value {
            ColumnValue::Int16(value) => value as i64,
            ColumnValue::Int32(value) => value as i64,
            ColumnValue::Int64(value) => value,
            _ => {
                return Err(unsupported(format!(
                    "{clause} expression is not supported in fast path"
                )));
            }
        };
        if value < 0 {
            return Err(user_error(
                "2201X",
                format!("{clause} must not be negative"),
            ));
        }
        Ok(value as usize)
    }

    pub(super) fn fast_path_order_by(
        order_by: Option<&OrderBy>,
        schema: &TableSchema,
    ) -> PgWireResult<()> {
        let Some(order_by) = order_by else {
            return Ok(());
        };
        if order_by.interpolate.is_some() {
            return Err(unsupported(
                "SELECT ORDER BY is not supported in fast path and should use DataFusion",
            ));
        }
        let OrderByKind::Expressions(exprs) = &order_by.kind else {
            return Err(unsupported(
                "SELECT ORDER BY is not supported in fast path and should use DataFusion",
            ));
        };
        if exprs.len() != 1 {
            return Err(unsupported(
                "SELECT ORDER BY is not supported in fast path and should use DataFusion",
            ));
        }
        let expr = &exprs[0];
        if expr.with_fill.is_some() {
            return Err(unsupported(
                "SELECT ORDER BY is not supported in fast path and should use DataFusion",
            ));
        }
        let column_name = expr_identifier_name(&expr.expr)?;
        let Some(_) = schema.find_column(&column_name) else {
            return Err(user_error(
                "42703",
                format!("column '{column_name}' does not exist"),
            ));
        };
        let descending = expr.options.asc == Some(false);
        if column_name == schema.primary_key && !descending && expr.options.nulls_first.is_none() {
            return Ok(());
        }
        Err(unsupported(
            "SELECT ORDER BY is not supported in fast path and should use DataFusion",
        ))
    }

    pub(super) fn fast_path_write_order_by(
        order_by: &[sqlparser::ast::OrderByExpr],
        schema: &TableSchema,
    ) -> PgWireResult<bool> {
        if order_by.is_empty() {
            return Ok(false);
        }
        if order_by.len() != 1 {
            return Err(unsupported(
                "UPDATE/DELETE ORDER BY fast path supports only the primary key",
            ));
        }
        let expr = &order_by[0];
        let column_name = expr_identifier_name(&expr.expr)?;
        if column_name == schema.primary_key
            && expr.options.asc != Some(false)
            && expr.options.nulls_first.is_none()
        {
            return Ok(true);
        }
        Err(unsupported(
            "UPDATE/DELETE ORDER BY fast path supports only primary key ascending",
        ))
    }

    pub(super) async fn plan_select(
        &self,
        database_name: &str,
        search_path: &[String],
        query: Query,
    ) -> PgWireResult<QueryPlan> {
        if query.with.is_some() || query.fetch.is_some() {
            return Err(unsupported(
                "SELECT shape is not supported in fast path and should use DataFusion",
            ));
        }
        let order_by = query.order_by.clone();
        let limit_clause = query.limit_clause.clone();
        let (limit, offset) = Self::fast_path_limit_offset(limit_clause.as_ref())?;

        let select = match *query.body {
            SetExpr::Select(select) => select,
            _ => return Err(unsupported("only SELECT queries are supported")),
        };
        if select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.having.is_some()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || !supports_fast_path_projection(&select)
        {
            return Err(unsupported(
                "SELECT shape is not supported in fast path and should use DataFusion",
            ));
        }

        let table_name = extract_single_table_name(&select)?;
        if table_name.starts_with("pg_catalog.") || table_name.starts_with("information_schema.") {
            return Err(unsupported(
                "catalog SELECT should use the DataFusion slow path",
            ));
        }
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let Some(schema) = schema else {
            let default_schema = DEFAULT_SCHEMA_NAME.to_string();
            for candidate_schema in search_path
                .iter()
                .chain(std::iter::once(&schema_name))
                .chain(std::iter::once(&default_schema))
            {
                if self
                    .catalog
                    .load_view(database_name, candidate_schema, &table_name)
                    .await?
                    .is_some()
                {
                    return Err(unsupported(
                        "SELECT from a view should use the DataFusion slow path",
                    ));
                }
            }
            return Err(user_error(
                "42P01",
                format!("table '{table_name}' does not exist"),
            ));
        };
        Self::fast_path_order_by(order_by.as_ref(), &schema)?;
        if select
            .selection
            .as_ref()
            .is_some_and(|expr| !supports_fast_path_filter(expr))
        {
            return Err(unsupported(
                "SELECT filter is not supported in fast path and should use DataFusion",
            ));
        }
        let projection = resolve_projection(&select, &schema)?;
        let access = match extract_primary_key_filter(select.selection.as_ref(), &schema)? {
            Some(key) => ReadAccess::PointLookup { key },
            None => {
                if let Some(keys) =
                    self.extract_primary_key_in_filter(select.selection.as_ref(), &schema)?
                {
                    ReadAccess::PrimaryKeyInLookup { keys }
                } else if let Some((lower, upper)) =
                    extract_primary_key_range_filter(select.selection.as_ref(), &schema)?
                {
                    ReadAccess::PrimaryKeyRangeScan {
                        lower,
                        upper,
                        filter: None,
                    }
                } else if let Some((column_name, key)) =
                    self.extract_indexable_eq_filter(select.selection.as_ref(), &schema)?
                    && let Some(index) = self
                        .catalog
                        .list_indexes_for_table(schema.table_id)
                        .await?
                        .into_iter()
                        .find(|index| index.column_names == vec![column_name.clone()])
                {
                    ReadAccess::SecondaryIndexLookup {
                        index_name: index.index_name,
                        column_name,
                        key,
                        filter: select.selection.clone(),
                    }
                } else if let Some((index, column_name, lower, upper)) = self
                    .extract_indexable_range_filter(select.selection.as_ref(), &schema)
                    .await?
                {
                    ReadAccess::SecondaryIndexRangeScan {
                        index_name: index.index_name,
                        column_name,
                        lower,
                        upper,
                        filter: select.selection.clone(),
                    }
                } else {
                    ReadAccess::PrefixScan {
                        filter: select.selection.clone(),
                    }
                }
            }
        };

        Ok(QueryPlan::SelectRows {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            projection,
            access,
            limit,
            offset,
        })
    }

    pub(super) async fn plan_update(
        &self,
        database_name: &str,
        search_path: &[String],
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        returning: Option<Vec<sqlparser::ast::SelectItem>>,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        limit: Option<Expr>,
    ) -> PgWireResult<QueryPlan> {
        let table_name = extract_table_name_from_table_with_joins(&table)?;
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        if selection
            .as_ref()
            .is_some_and(|expr| !supports_fast_path_filter(expr))
        {
            return Err(unsupported("UPDATE filter is not supported in fast path"));
        }
        let order_by_primary_key = Self::fast_path_write_order_by(&order_by, &schema)?;
        let limit = limit
            .as_ref()
            .map(|expr| Self::fast_path_usize_expr(expr, "LIMIT"))
            .transpose()?;

        let typed_assignments = assignments
            .into_iter()
            .map(|a| self.resolve_update_assignment(&schema, a))
            .collect::<PgWireResult<Vec<_>>>()?;

        let access = match extract_primary_key_filter(selection.as_ref(), &schema)? {
            Some(key) => WriteAccess::PointLookup { key },
            None => {
                if let Some((lower, upper)) =
                    extract_primary_key_range_filter(selection.as_ref(), &schema)?
                {
                    WriteAccess::PrimaryKeyRangeScan {
                        lower,
                        upper,
                        filter: selection,
                    }
                } else if let Some((column_name, key)) =
                    self.extract_indexable_eq_filter(selection.as_ref(), &schema)?
                    && let Some(index) = self
                        .catalog
                        .list_indexes_for_table(schema.table_id)
                        .await?
                        .into_iter()
                        .find(|index| index.column_names == vec![column_name.clone()])
                {
                    WriteAccess::SecondaryIndexLookup {
                        index_name: index.index_name,
                        key,
                        filter: selection.clone(),
                    }
                } else if let Some((index, _column_name, lower, upper)) = self
                    .extract_indexable_range_filter(selection.as_ref(), &schema)
                    .await?
                {
                    WriteAccess::SecondaryIndexRangeScan {
                        index_name: index.index_name,
                        lower,
                        upper,
                        filter: selection.clone(),
                    }
                } else {
                    WriteAccess::PrefixScan { filter: selection }
                }
            }
        };

        let returning = returning
            .as_ref()
            .map(|items| resolve_returning_projection(items, &schema))
            .transpose()?;

        Ok(QueryPlan::UpdateRows {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            assignments: typed_assignments,
            access,
            limit,
            order_by_primary_key,
            returning,
        })
    }

    pub(super) async fn plan_delete(
        &self,
        database_name: &str,
        search_path: &[String],
        from: Vec<TableWithJoins>,
        selection: Option<Expr>,
        returning: Option<Vec<sqlparser::ast::SelectItem>>,
        order_by: Vec<sqlparser::ast::OrderByExpr>,
        limit: Option<Expr>,
    ) -> PgWireResult<QueryPlan> {
        if from.len() != 1 {
            return Err(unsupported("DELETE fast-path only supports a single table"));
        }

        let table_name = extract_table_name_from_table_with_joins(&from[0])?;
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, &table_name)
            .await?;
        let schema = schema
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        if selection
            .as_ref()
            .is_some_and(|expr| !supports_fast_path_filter(expr))
        {
            return Err(unsupported("DELETE filter is not supported in fast path"));
        }
        let order_by_primary_key = Self::fast_path_write_order_by(&order_by, &schema)?;
        let limit = limit
            .as_ref()
            .map(|expr| Self::fast_path_usize_expr(expr, "LIMIT"))
            .transpose()?;

        let access = match extract_primary_key_filter(selection.as_ref(), &schema)? {
            Some(key) => WriteAccess::PointLookup { key },
            None => {
                if let Some((lower, upper)) =
                    extract_primary_key_range_filter(selection.as_ref(), &schema)?
                {
                    WriteAccess::PrimaryKeyRangeScan {
                        lower,
                        upper,
                        filter: selection,
                    }
                } else if let Some((column_name, key)) =
                    self.extract_indexable_eq_filter(selection.as_ref(), &schema)?
                    && let Some(index) = self
                        .catalog
                        .list_indexes_for_table(schema.table_id)
                        .await?
                        .into_iter()
                        .find(|index| index.column_names == vec![column_name.clone()])
                {
                    WriteAccess::SecondaryIndexLookup {
                        index_name: index.index_name,
                        key,
                        filter: selection.clone(),
                    }
                } else if let Some((index, _column_name, lower, upper)) = self
                    .extract_indexable_range_filter(selection.as_ref(), &schema)
                    .await?
                {
                    WriteAccess::SecondaryIndexRangeScan {
                        index_name: index.index_name,
                        lower,
                        upper,
                        filter: selection.clone(),
                    }
                } else {
                    WriteAccess::PrefixScan { filter: selection }
                }
            }
        };

        let returning = returning
            .as_ref()
            .map(|items| resolve_returning_projection(items, &schema))
            .transpose()?;

        Ok(QueryPlan::DeleteRows {
            database_name: database_name.to_string(),
            schema_name,
            schema,
            access,
            limit,
            order_by_primary_key,
            returning,
        })
    }

    // ── execution ─────────────────────────────────────────────────────
}

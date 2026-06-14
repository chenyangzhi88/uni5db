use super::*;

impl GatewayServer {
    pub(super) async fn read_visible_row(
        &self,
        session_id: Option<i32>,
        schema: &TableSchema,
        pk_value: &ColumnValue,
    ) -> PgWireResult<Option<RowMap>> {
        self.read_visible_row_at(
            session_id,
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            schema,
            pk_value,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn scan_visible_rows(
        &self,
        session_id: Option<i32>,
        schema: &TableSchema,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<RowMap>> {
        self.scan_visible_rows_at(
            session_id,
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            schema,
            filter,
        )
        .await
    }

    // ── slow-path (DataFusion) ────────────────────────────────────────

    pub(super) async fn execute_via_datafusion(
        &self,
        sql: &str,
        session: &SessionCatalog,
    ) -> PgWireResult<Vec<Response>> {
        let ctx = Self::new_datafusion_context(&session.database_name, &session.schema_name);
        let sql = self.expand_views_in_sql(sql, session).await?;
        let sql = self
            .qualify_sql_for_search_path(&sql, &session.database_name, &session.search_path)
            .await?;
        let include_system_catalogs = sql_needs_system_catalogs(&sql);
        let sql = strip_pg_catalog_function_qualifiers(&sql);
        let datafusion_started_at = std::time::Instant::now();
        self.register_datafusion_catalogs(&ctx, &session.database_name, include_system_catalogs)
            .await?;
        if matches!(
            std::env::var("PG_GATEWAY_PROFILE_COPY").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        ) {
            log::info!(
                "[pg_gateway copy-profile] datafusion.register_catalog_tables elapsed_ms={}",
                datafusion_started_at.elapsed().as_millis()
            );
        }
        let plan_started_at = std::time::Instant::now();
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| user_error("XX000", format!("DataFusion plan error: {e}")))?;
        let output_schema = Arc::new(df.schema().as_arrow().clone());
        if matches!(
            std::env::var("PG_GATEWAY_PROFILE_COPY").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        ) {
            log::info!(
                "[pg_gateway copy-profile] datafusion.ctx_sql elapsed_ms={}",
                plan_started_at.elapsed().as_millis()
            );
        }
        let collect_started_at = std::time::Instant::now();
        let mut batches = df
            .collect()
            .await
            .map_err(|e| user_error("XX000", format!("DataFusion execution error: {e}")))?;
        if batches.is_empty() {
            batches.push(arrow::record_batch::RecordBatch::new_empty(output_schema));
        }
        if matches!(
            std::env::var("PG_GATEWAY_PROFILE_COPY").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        ) {
            log::info!(
                "[pg_gateway copy-profile] datafusion.collect elapsed_ms={}",
                collect_started_at.elapsed().as_millis()
            );
        }
        let response = arrow_to_pgwire_response(batches)?;
        Ok(vec![response])
    }

    pub(super) async fn collect_datafusion_batches(
        &self,
        sql: &str,
        session: &SessionCatalog,
    ) -> PgWireResult<Vec<arrow::record_batch::RecordBatch>> {
        let ctx = Self::new_datafusion_context(&session.database_name, &session.schema_name);
        let sql = self.expand_views_in_sql(sql, session).await?;
        let sql = self
            .qualify_sql_for_search_path(&sql, &session.database_name, &session.search_path)
            .await?;
        let include_system_catalogs = sql_needs_system_catalogs(&sql);
        let sql = strip_pg_catalog_function_qualifiers(&sql);
        self.register_datafusion_catalogs(&ctx, &session.database_name, include_system_catalogs)
            .await?;
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| user_error("XX000", format!("DataFusion plan error: {e}")))?;
        df.collect()
            .await
            .map_err(|e| user_error("XX000", format!("DataFusion execution error: {e}")))
    }

    pub(super) async fn expand_views_in_sql(
        &self,
        sql: &str,
        session: &SessionCatalog,
    ) -> PgWireResult<String> {
        let mut expanded = sql.to_string();
        for view in self.catalog.list_views(&session.database_name).await? {
            let replacement = format!("({}) AS {}", view.definition, view.view_name);
            for relation in [
                view.view_name.clone(),
                format!("{}.{}", view.schema_name, view.view_name),
            ] {
                expanded =
                    replace_relation_after_keyword(&expanded, "from", &relation, &replacement);
                expanded =
                    replace_relation_after_keyword(&expanded, "join", &relation, &replacement);
            }
        }
        Ok(expanded)
    }

    pub(super) fn arrow_type_to_gateway_type(data_type: &arrow::datatypes::DataType) -> DataType {
        match data_type {
            arrow::datatypes::DataType::Int16 => DataType::Int16,
            arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::UInt8
            | arrow::datatypes::DataType::UInt16 => DataType::Int32,
            arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt64 => DataType::Int64,
            arrow::datatypes::DataType::Float32 => DataType::Float32,
            arrow::datatypes::DataType::Float64 => DataType::Float64,
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64 => {
                DataType::Date
            }
            arrow::datatypes::DataType::Timestamp(_, _) => DataType::Timestamp,
            _ => DataType::Text,
        }
    }

    pub(super) fn datafusion_rows_for_schema(
        &self,
        batches: &[arrow::record_batch::RecordBatch],
        schema: &TableSchema,
        target_columns: &[String],
    ) -> PgWireResult<Vec<RowMap>> {
        use arrow::array::Array;

        let mut rows = Vec::new();
        for batch in batches {
            if batch.num_columns() != target_columns.len() {
                return Err(user_error(
                    "42601",
                    format!(
                        "INSERT has {} target columns but query returns {} columns",
                        target_columns.len(),
                        batch.num_columns()
                    ),
                ));
            }
            for row_idx in 0..batch.num_rows() {
                let mut row = RowMap::new();
                for (col_idx, column_name) in target_columns.iter().enumerate() {
                    let column = schema.find_column(column_name).ok_or_else(|| {
                        user_error("42703", format!("column '{column_name}' not found"))
                    })?;
                    let array = batch.column(col_idx);
                    let value = if array.is_null(row_idx) {
                        ColumnValue::Null
                    } else {
                        let text = arrow_array_value_to_string(array, row_idx);
                        crate::sql::parse_text_for_type(&text, &column.data_type)?
                    };
                    row.insert(column_name.clone(), value);
                }
                for column in &schema.columns {
                    if !row.contains_key(&column.name) {
                        let value = column
                            .default
                            .as_deref()
                            .map(|default_sql| column_default_value(default_sql, &column.data_type))
                            .transpose()?
                            .unwrap_or(ColumnValue::Null);
                        row.insert(column.name.clone(), value);
                    }
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    // ── schema persistence (still JSON for metadata) ─────────────────

    #[cfg(test)]
    pub(super) async fn load_schema(
        &self,
        database_name: &str,
        default_schema_name: &str,
        table_name: &str,
    ) -> PgWireResult<Option<TableSchema>> {
        let search_path = vec![default_schema_name.to_string()];
        Ok(self
            .resolve_table_schema(database_name, &search_path, table_name)
            .await?
            .1)
    }

    pub(super) async fn resolve_table_schema(
        &self,
        database_name: &str,
        search_path: &[String],
        table_name: &str,
    ) -> PgWireResult<(String, Option<TableSchema>)> {
        if table_name.contains('.') {
            let (schema_name, table_name) = resolve_table_reference(table_name)?;
            let schema = self
                .catalog
                .load_table(database_name, &schema_name, &table_name)
                .await?
                .map(|table| table.schema);
            return Ok((schema_name, schema));
        }

        for schema_name in search_path {
            if let Some(table) = self
                .catalog
                .load_table(database_name, schema_name, table_name)
                .await?
            {
                return Ok((schema_name.clone(), Some(table.schema)));
            }
        }

        let schema_name = search_path
            .first()
            .cloned()
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());
        Ok((schema_name, None))
    }

    pub(super) async fn resolve_table_name_for_search_path(
        &self,
        database_name: &str,
        search_path: &[String],
        table_name: &str,
    ) -> PgWireResult<Option<String>> {
        let (schema_name, schema) = self
            .resolve_table_schema(database_name, search_path, table_name)
            .await?;
        Ok(schema.map(|table| format!("{schema_name}.{}", table.table_name)))
    }
}

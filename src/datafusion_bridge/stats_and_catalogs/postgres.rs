use super::*;

pub(in crate::datafusion_bridge) fn avg_width_for_stats(
    column_type: &DataType,
    zone: &storage_layout::ColumnZoneMap,
) -> i32 {
    match column_type {
        DataType::Boolean => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
            unsigned: false,
        } => 2,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
            ..
        } => 8,
        DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => 4,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::MySqlFloat { .. } => 4,
        DataType::MySqlDouble { .. } => 8,
        DataType::Text
        | DataType::MySqlText { .. }
        | DataType::VarChar(_)
        | DataType::Char(_)
        | DataType::Numeric { .. }
        | DataType::Date
        | DataType::Time
        | DataType::MySqlTime { .. }
        | DataType::TimeTz
        | DataType::Interval
        | DataType::Timestamp
        | DataType::MySqlDateTime { .. }
        | DataType::MySqlTimestamp { .. }
        | DataType::TimestampTz
        | DataType::Uuid
        | DataType::Json
        | DataType::Jsonb
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob { .. }
        | DataType::Array(_)
        | DataType::Domain(_)
        | DataType::Enum(_)
        | DataType::Set(_)
        | DataType::Geometry(_) => {
            let min_len = zone
                .min
                .as_ref()
                .and_then(ColumnValue::to_text)
                .map(|v| v.len());
            let max_len = zone
                .max
                .as_ref()
                .and_then(ColumnValue::to_text)
                .map(|v| v.len());
            match (min_len, max_len) {
                (Some(min), Some(max)) => ((min + max) / 2) as i32,
                (Some(len), None) | (None, Some(len)) => len as i32,
                (None, None) => 0,
            }
        }
    }
}

pub(in crate::datafusion_bridge) async fn register_statistic_catalog_tables(
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut statistic_rows = Vec::new();
    let mut stats_rows = Vec::new();
    for table in &tables {
        let Some(stats) = load_table_stats(store.as_ref(), &table.schema).await? else {
            continue;
        };
        for (attnum, column) in table.schema.columns.iter().enumerate() {
            let Some(zone) = stats
                .zones
                .iter()
                .find(|zone| zone.column_id == column.column_id)
            else {
                continue;
            };
            let null_frac = if stats.row_count == 0 {
                0.0
            } else {
                zone.null_count as f32 / stats.row_count as f32
            };
            let avg_width = avg_width_for_stats(&column.data_type, zone);
            statistic_rows.push((
                table.schema.table_id as i32,
                attnum as i32 + 1,
                false,
                null_frac,
                avg_width,
                -1.0_f32,
            ));
            stats_rows.push((
                table.schema_name.clone(),
                table.table_name.clone(),
                column.name.clone(),
                false,
                null_frac,
                avg_width,
                -1.0_f32,
            ));
        }
    }
    let statistic_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("starelid", ArrowDataType::Int32, false),
        Field::new("staattnum", ArrowDataType::Int32, false),
        Field::new("stainherit", ArrowDataType::Boolean, false),
        Field::new("stanullfrac", ArrowDataType::Float32, false),
        Field::new("stawidth", ArrowDataType::Int32, false),
        Field::new("stadistinct", ArrowDataType::Float32, false),
    ]));
    let statistic_batch = RecordBatch::try_new(
        statistic_schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                statistic_rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                statistic_rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                statistic_rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                statistic_rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                statistic_rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                statistic_rows.iter().map(|row| row.5).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_statistic batch: {e}")))?;
    let statistic_table = MemTable::try_new(statistic_schema, vec![vec![statistic_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_statistic table: {e}")))?;
    schema_provider
        .register_table("pg_statistic".to_string(), Arc::new(statistic_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_statistic: {e}")))?;

    let stats_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("tablename", ArrowDataType::Utf8, false),
        Field::new("attname", ArrowDataType::Utf8, false),
        Field::new("inherited", ArrowDataType::Boolean, false),
        Field::new("null_frac", ArrowDataType::Float32, false),
        Field::new("avg_width", ArrowDataType::Int32, false),
        Field::new("n_distinct", ArrowDataType::Float32, false),
    ]));
    let stats_batch = RecordBatch::try_new(
        stats_schema.clone(),
        vec![
            Arc::new(StringArray::from(
                stats_rows
                    .iter()
                    .map(|row| row.0.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                stats_rows
                    .iter()
                    .map(|row| row.1.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                stats_rows
                    .iter()
                    .map(|row| row.2.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                stats_rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                stats_rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                stats_rows.iter().map(|row| row.5).collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(
                stats_rows.iter().map(|row| row.6).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_stats batch: {e}")))?;
    let stats_table = MemTable::try_new(stats_schema, vec![vec![stats_batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_stats table: {e}")))?;
    schema_provider
        .register_table("pg_stats".to_string(), Arc::new(stats_table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_stats: {e}")))?;
    register_empty_table(
        schema_provider,
        "pg_statistic_ext",
        vec![
            Field::new("oid", ArrowDataType::Int32, false),
            Field::new("stxrelid", ArrowDataType::Int32, false),
            Field::new("stxname", ArrowDataType::Utf8, false),
            Field::new("stxnamespace", ArrowDataType::Int32, false),
            Field::new("stxowner", ArrowDataType::Int32, false),
            Field::new("stxkeys", ArrowDataType::Utf8, false),
            Field::new("stxkind", ArrowDataType::Utf8, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "pg_statistic_ext_data",
        vec![
            Field::new("stxoid", ArrowDataType::Int32, false),
            Field::new("stxdinherit", ArrowDataType::Boolean, false),
            Field::new("stxdndistinct", ArrowDataType::Utf8, true),
            Field::new("stxddependencies", ArrowDataType::Utf8, true),
            Field::new("stxdmcv", ArrowDataType::Utf8, true),
            Field::new("stxdexpr", ArrowDataType::Utf8, true),
        ],
    )?;
    for table_name in ["pg_stats_ext", "pg_stats_ext_exprs"] {
        register_empty_table(
            schema_provider,
            table_name,
            vec![
                Field::new("schemaname", ArrowDataType::Utf8, false),
                Field::new("tablename", ArrowDataType::Utf8, false),
                Field::new("statistics_name", ArrowDataType::Utf8, false),
            ],
        )?;
    }
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_pg_tables_view(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("tablename", ArrowDataType::Utf8, false),
        Field::new("tableowner", ArrowDataType::Utf8, false),
        Field::new("tablespace", ArrowDataType::Utf8, true),
        Field::new("hasindexes", ArrowDataType::Boolean, false),
        Field::new("hasrules", ArrowDataType::Boolean, false),
        Field::new("hastriggers", ArrowDataType::Boolean, false),
        Field::new("rowsecurity", ArrowDataType::Boolean, false),
    ]));
    let schemanames = StringArray::from(
        tables
            .iter()
            .map(|table| table.schema_name.clone())
            .collect::<Vec<_>>(),
    );
    let tablenames = StringArray::from(
        tables
            .iter()
            .map(|table| table.table_name.clone())
            .collect::<Vec<_>>(),
    );
    let owners = StringArray::from(vec!["postgres"; tables.len()]);
    let tablespace = StringArray::from(vec![Option::<String>::None; tables.len()]);
    let mut hasindexes_values = Vec::with_capacity(tables.len());
    for table in &tables {
        hasindexes_values.push(
            !catalog
                .list_indexes_for_table(table.schema.table_id)
                .await?
                .is_empty(),
        );
    }
    let hasindexes = BooleanArray::from(hasindexes_values);
    let hasrules = BooleanArray::from(vec![false; tables.len()]);
    let hastriggers = BooleanArray::from(vec![false; tables.len()]);
    let rowsecurity = BooleanArray::from(vec![false; tables.len()]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(schemanames),
            Arc::new(tablenames),
            Arc::new(owners),
            Arc::new(tablespace),
            Arc::new(hasindexes),
            Arc::new(hasrules),
            Arc::new(hastriggers),
            Arc::new(rowsecurity),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_tables batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_tables view: {e}")))?;
    schema_provider
        .register_table("pg_tables".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_tables: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_pg_indexes_view(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut schemanames = Vec::new();
    let mut tablenames = Vec::new();
    let mut indexnames = Vec::new();
    let mut tablespaces = Vec::new();
    let mut indexdefs = Vec::new();
    for table in &tables {
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            schemanames.push(table.schema_name.clone());
            tablenames.push(table.table_name.clone());
            indexnames.push(index.index_name.clone());
            tablespaces.push(Option::<String>::None);
            indexdefs.push(format!(
                "CREATE {}INDEX {} ON {}.{} USING btree ({})",
                if index.unique { "UNIQUE " } else { "" },
                index.index_name,
                table.schema_name,
                table.table_name,
                index.column_names.join(", ")
            ));
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("tablename", ArrowDataType::Utf8, false),
        Field::new("indexname", ArrowDataType::Utf8, false),
        Field::new("tablespace", ArrowDataType::Utf8, true),
        Field::new("indexdef", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(schemanames)),
            Arc::new(StringArray::from(tablenames)),
            Arc::new(StringArray::from(indexnames)),
            Arc::new(StringArray::from(tablespaces)),
            Arc::new(StringArray::from(indexdefs)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_indexes batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_indexes table: {e}")))?;
    schema_provider
        .register_table("pg_indexes".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_indexes: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_pg_sequences_view(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    register_empty_table(
        schema_provider,
        "pg_sequences",
        vec![
            Field::new("schemaname", ArrowDataType::Utf8, false),
            Field::new("sequencename", ArrowDataType::Utf8, false),
            Field::new("sequenceowner", ArrowDataType::Utf8, false),
            Field::new("data_type", ArrowDataType::Utf8, false),
            Field::new("start_value", ArrowDataType::Int64, false),
            Field::new("min_value", ArrowDataType::Int64, false),
            Field::new("max_value", ArrowDataType::Int64, false),
            Field::new("increment_by", ArrowDataType::Int64, false),
            Field::new("cycle", ArrowDataType::Boolean, false),
            Field::new("cache_size", ArrowDataType::Int64, false),
            Field::new("last_value", ArrowDataType::Int64, true),
        ],
    )
}

pub(in crate::datafusion_bridge) async fn register_pg_views_view(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let views = catalog.list_views(database_name).await?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("schemaname", ArrowDataType::Utf8, false),
        Field::new("viewname", ArrowDataType::Utf8, false),
        Field::new("viewowner", ArrowDataType::Utf8, false),
        Field::new("definition", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| view.schema_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| view.view_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["postgres"; views.len()])),
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| Some(view.definition.clone()))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_views batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_views: {e}")))?;
    schema_provider
        .register_table("pg_views".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_views: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_tables(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let views = catalog.list_views(database_name).await?;
    let row_count = tables.len() + views.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("table_type", ArrowDataType::Utf8, false),
        Field::new("self_referencing_column_name", ArrowDataType::Utf8, true),
        Field::new("reference_generation", ArrowDataType::Utf8, true),
        Field::new("user_defined_type_catalog", ArrowDataType::Utf8, true),
        Field::new("user_defined_type_schema", ArrowDataType::Utf8, true),
        Field::new("user_defined_type_name", ArrowDataType::Utf8, true),
        Field::new("is_insertable_into", ArrowDataType::Utf8, false),
        Field::new("is_typed", ArrowDataType::Utf8, false),
        Field::new("commit_action", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![database_name; row_count])),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.schema_name.clone())
                    .chain(views.iter().map(|view| view.schema_name.clone()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.table_name.clone())
                    .chain(views.iter().map(|view| view.view_name.clone()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                std::iter::repeat("BASE TABLE")
                    .take(tables.len())
                    .chain(std::iter::repeat("VIEW").take(views.len()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec!["YES"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build information_schema.tables batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build information_schema.tables: {e}"),
        )
    })?;
    schema_provider
        .register_table("tables".to_string(), Arc::new(table))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register information_schema.tables: {e}"),
            )
        })?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_columns(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut table_catalogs = Vec::new();
    let mut table_schemas = Vec::new();
    let mut table_names = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinal_positions = Vec::new();
    let mut is_nullables = Vec::new();
    let mut data_types = Vec::new();
    let mut udt_names = Vec::new();
    let mut column_defaults = Vec::new();
    let mut char_max_lengths = Vec::new();
    let mut char_octet_lengths = Vec::new();
    let mut numeric_precisions = Vec::new();
    let mut numeric_precision_radixes = Vec::new();
    let mut numeric_scales = Vec::new();
    let mut datetime_precisions = Vec::new();
    let mut udt_catalogs = Vec::new();
    let mut udt_schemas = Vec::new();
    let mut is_identitys = Vec::new();
    let mut identity_generations = Vec::new();
    let mut is_generateds = Vec::new();
    let mut generation_expressions = Vec::new();
    let mut is_updatables = Vec::new();

    for table in tables {
        for (idx, column) in table.schema.columns.iter().enumerate() {
            table_catalogs.push(database_name.to_string());
            table_schemas.push(table.schema_name.clone());
            table_names.push(table.table_name.clone());
            column_names.push(column.name.clone());
            ordinal_positions.push(idx as i32 + 1);
            is_nullables.push(if column.nullable && !column.primary_key {
                "YES".to_string()
            } else {
                "NO".to_string()
            });
            data_types.push(information_schema_data_type(&column.data_type).to_string());
            udt_names.push(pg_type_name(&column.data_type).to_string());
            column_defaults.push(Option::<String>::None);
            char_max_lengths.push(Option::<i32>::None);
            char_octet_lengths.push(Option::<i32>::None);
            let (precision, radix, scale) = match column.data_type {
                DataType::Int32 => (Some(32), Some(2), Some(0)),
                DataType::Int64 => (Some(64), Some(2), Some(0)),
                _ => (None, None, None),
            };
            numeric_precisions.push(precision);
            numeric_precision_radixes.push(radix);
            numeric_scales.push(scale);
            datetime_precisions.push(Option::<i32>::None);
            udt_catalogs.push(database_name.to_string());
            udt_schemas.push(PG_CATALOG_SCHEMA_NAME.to_string());
            is_identitys.push("NO".to_string());
            identity_generations.push(Option::<String>::None);
            is_generateds.push("NEVER".to_string());
            generation_expressions.push(Option::<String>::None);
            is_updatables.push("YES".to_string());
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("ordinal_position", ArrowDataType::Int32, false),
        Field::new("column_default", ArrowDataType::Utf8, true),
        Field::new("is_nullable", ArrowDataType::Utf8, false),
        Field::new("data_type", ArrowDataType::Utf8, false),
        Field::new("character_maximum_length", ArrowDataType::Int32, true),
        Field::new("character_octet_length", ArrowDataType::Int32, true),
        Field::new("numeric_precision", ArrowDataType::Int32, true),
        Field::new("numeric_precision_radix", ArrowDataType::Int32, true),
        Field::new("numeric_scale", ArrowDataType::Int32, true),
        Field::new("datetime_precision", ArrowDataType::Int32, true),
        Field::new("interval_type", ArrowDataType::Utf8, true),
        Field::new("interval_precision", ArrowDataType::Int32, true),
        Field::new("character_set_catalog", ArrowDataType::Utf8, true),
        Field::new("character_set_schema", ArrowDataType::Utf8, true),
        Field::new("character_set_name", ArrowDataType::Utf8, true),
        Field::new("collation_catalog", ArrowDataType::Utf8, true),
        Field::new("collation_schema", ArrowDataType::Utf8, true),
        Field::new("collation_name", ArrowDataType::Utf8, true),
        Field::new("domain_catalog", ArrowDataType::Utf8, true),
        Field::new("domain_schema", ArrowDataType::Utf8, true),
        Field::new("domain_name", ArrowDataType::Utf8, true),
        Field::new("udt_catalog", ArrowDataType::Utf8, false),
        Field::new("udt_schema", ArrowDataType::Utf8, false),
        Field::new("udt_name", ArrowDataType::Utf8, false),
        Field::new("scope_catalog", ArrowDataType::Utf8, true),
        Field::new("scope_schema", ArrowDataType::Utf8, true),
        Field::new("scope_name", ArrowDataType::Utf8, true),
        Field::new("maximum_cardinality", ArrowDataType::Int32, true),
        Field::new("dtd_identifier", ArrowDataType::Utf8, false),
        Field::new("is_self_referencing", ArrowDataType::Utf8, true),
        Field::new("is_identity", ArrowDataType::Utf8, false),
        Field::new("identity_generation", ArrowDataType::Utf8, true),
        Field::new("identity_start", ArrowDataType::Utf8, true),
        Field::new("identity_increment", ArrowDataType::Utf8, true),
        Field::new("identity_maximum", ArrowDataType::Utf8, true),
        Field::new("identity_minimum", ArrowDataType::Utf8, true),
        Field::new("identity_cycle", ArrowDataType::Utf8, true),
        Field::new("is_generated", ArrowDataType::Utf8, false),
        Field::new("generation_expression", ArrowDataType::Utf8, true),
        Field::new("is_updatable", ArrowDataType::Utf8, false),
    ]));
    let row_count = table_catalogs.len();
    let null_strings = vec![Option::<String>::None; row_count];
    let null_ints = vec![Option::<i32>::None; row_count];
    let dtd_identifiers = (1..=row_count)
        .map(|idx| idx.to_string())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(StringArray::from(column_defaults)),
            Arc::new(StringArray::from(is_nullables)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(Int32Array::from(char_max_lengths)),
            Arc::new(Int32Array::from(char_octet_lengths)),
            Arc::new(Int32Array::from(numeric_precisions)),
            Arc::new(Int32Array::from(numeric_precision_radixes)),
            Arc::new(Int32Array::from(numeric_scales)),
            Arc::new(Int32Array::from(datetime_precisions)),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(Int32Array::from(null_ints.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(udt_catalogs)),
            Arc::new(StringArray::from(udt_schemas)),
            Arc::new(StringArray::from(udt_names)),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(Int32Array::from(null_ints.clone())),
            Arc::new(StringArray::from(dtd_identifiers)),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(is_identitys)),
            Arc::new(StringArray::from(identity_generations)),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(is_generateds)),
            Arc::new(StringArray::from(generation_expressions)),
            Arc::new(StringArray::from(is_updatables)),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build information_schema.columns batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build information_schema.columns: {e}"),
        )
    })?;
    schema_provider
        .register_table("columns".to_string(), Arc::new(table))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register information_schema.columns: {e}"),
            )
        })?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_schemata(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let mut catalog_names = Vec::new();
    let mut schema_names = Vec::new();
    let mut owners = Vec::new();
    for schema in catalog.list_schemas(database_name).await? {
        catalog_names.push(database_name.to_string());
        schema_names.push(schema.schema_name);
        owners.push("postgres".to_string());
    }
    for schema_name in [PG_CATALOG_SCHEMA_NAME, INFORMATION_SCHEMA_NAME] {
        catalog_names.push(database_name.to_string());
        schema_names.push(schema_name.to_string());
        owners.push("postgres".to_string());
    }
    let row_count = catalog_names.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("catalog_name", ArrowDataType::Utf8, false),
        Field::new("schema_name", ArrowDataType::Utf8, false),
        Field::new("schema_owner", ArrowDataType::Utf8, false),
        Field::new("default_character_set_catalog", ArrowDataType::Utf8, true),
        Field::new("default_character_set_schema", ArrowDataType::Utf8, true),
        Field::new("default_character_set_name", ArrowDataType::Utf8, true),
        Field::new("sql_path", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(catalog_names)),
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(owners)),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build schemata batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build schemata table: {e}")))?;
    schema_provider
        .register_table("schemata".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register schemata: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_constraints(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut constraint_catalogs = Vec::new();
    let mut constraint_schemas = Vec::new();
    let mut constraint_names = Vec::new();
    let mut table_catalogs = Vec::new();
    let mut table_schemas = Vec::new();
    let mut table_names = Vec::new();
    let mut constraint_types = Vec::new();
    let mut column_names = Vec::new();
    let mut ordinal_positions = Vec::new();

    for table in &tables {
        if table.schema.has_user_primary_key() {
            constraint_catalogs.push(database_name.to_string());
            constraint_schemas.push(table.schema_name.clone());
            constraint_names.push(format!("{}_pkey", table.table_name));
            table_catalogs.push(database_name.to_string());
            table_schemas.push(table.schema_name.clone());
            table_names.push(table.table_name.clone());
            constraint_types.push("PRIMARY KEY".to_string());
            column_names.push(table.schema.primary_key.clone());
            ordinal_positions.push(1);
        }
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            if !index.unique {
                continue;
            }
            for (ordinal, column_name) in index.column_names.iter().enumerate() {
                constraint_catalogs.push(database_name.to_string());
                constraint_schemas.push(table.schema_name.clone());
                constraint_names.push(index.index_name.clone());
                table_catalogs.push(database_name.to_string());
                table_schemas.push(table.schema_name.clone());
                table_names.push(table.table_name.clone());
                constraint_types.push("UNIQUE".to_string());
                column_names.push(column_name.clone());
                ordinal_positions.push((ordinal + 1) as i32);
            }
        }
    }
    let row_count = constraint_names.len();

    let table_constraints_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("constraint_catalog", ArrowDataType::Utf8, false),
        Field::new("constraint_schema", ArrowDataType::Utf8, false),
        Field::new("constraint_name", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("constraint_type", ArrowDataType::Utf8, false),
        Field::new("is_deferrable", ArrowDataType::Utf8, false),
        Field::new("initially_deferred", ArrowDataType::Utf8, false),
        Field::new("enforced", ArrowDataType::Utf8, false),
    ]));
    let table_constraints_batch = RecordBatch::try_new(
        table_constraints_schema.clone(),
        vec![
            Arc::new(StringArray::from(constraint_catalogs.clone())),
            Arc::new(StringArray::from(constraint_schemas.clone())),
            Arc::new(StringArray::from(constraint_names.clone())),
            Arc::new(StringArray::from(table_schemas.clone())),
            Arc::new(StringArray::from(table_names.clone())),
            Arc::new(StringArray::from(constraint_types)),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["YES"; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build table_constraints batch: {e}"),
        )
    })?;
    let table_constraints = MemTable::try_new(
        table_constraints_schema,
        vec![vec![table_constraints_batch]],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build table_constraints table: {e}"),
        )
    })?;
    schema_provider
        .register_table("table_constraints".to_string(), Arc::new(table_constraints))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register table_constraints: {e}"),
            )
        })?;

    let key_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("constraint_catalog", ArrowDataType::Utf8, false),
        Field::new("constraint_schema", ArrowDataType::Utf8, false),
        Field::new("constraint_name", ArrowDataType::Utf8, false),
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("ordinal_position", ArrowDataType::Int32, false),
        Field::new("position_in_unique_constraint", ArrowDataType::Int32, true),
    ]));
    let key_batch = RecordBatch::try_new(
        key_schema.clone(),
        vec![
            Arc::new(StringArray::from(constraint_catalogs.clone())),
            Arc::new(StringArray::from(constraint_schemas.clone())),
            Arc::new(StringArray::from(constraint_names.clone())),
            Arc::new(StringArray::from(table_catalogs.clone())),
            Arc::new(StringArray::from(table_schemas.clone())),
            Arc::new(StringArray::from(table_names.clone())),
            Arc::new(StringArray::from(column_names.clone())),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(Int32Array::from(vec![Option::<i32>::None; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build key_column_usage batch: {e}"),
        )
    })?;
    let key_table = MemTable::try_new(key_schema, vec![vec![key_batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build key_column_usage table: {e}"),
        )
    })?;
    schema_provider
        .register_table("key_column_usage".to_string(), Arc::new(key_table))
        .map_err(|e| user_error("XX000", format!("failed to register key_column_usage: {e}")))?;

    let column_usage_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("constraint_catalog", ArrowDataType::Utf8, false),
        Field::new("constraint_schema", ArrowDataType::Utf8, false),
        Field::new("constraint_name", ArrowDataType::Utf8, false),
    ]));
    let column_usage_batch = RecordBatch::try_new(
        column_usage_schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs.clone())),
            Arc::new(StringArray::from(table_schemas.clone())),
            Arc::new(StringArray::from(table_names.clone())),
            Arc::new(StringArray::from(column_names)),
            Arc::new(StringArray::from(constraint_catalogs.clone())),
            Arc::new(StringArray::from(constraint_schemas.clone())),
            Arc::new(StringArray::from(constraint_names.clone())),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build constraint_column_usage batch: {e}"),
        )
    })?;
    let column_usage = MemTable::try_new(column_usage_schema, vec![vec![column_usage_batch]])
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to build constraint_column_usage table: {e}"),
            )
        })?;
    schema_provider
        .register_table(
            "constraint_column_usage".to_string(),
            Arc::new(column_usage),
        )
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register constraint_column_usage: {e}"),
            )
        })?;

    let table_usage_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("constraint_catalog", ArrowDataType::Utf8, false),
        Field::new("constraint_schema", ArrowDataType::Utf8, false),
        Field::new("constraint_name", ArrowDataType::Utf8, false),
    ]));
    let table_usage_batch = RecordBatch::try_new(
        table_usage_schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build constraint_table_usage batch: {e}"),
        )
    })?;
    let table_usage = MemTable::try_new(table_usage_schema, vec![vec![table_usage_batch]])
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to build constraint_table_usage table: {e}"),
            )
        })?;
    schema_provider
        .register_table("constraint_table_usage".to_string(), Arc::new(table_usage))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register constraint_table_usage: {e}"),
            )
        })?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_privileges(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let table_privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"];
    let column_privileges = ["SELECT", "UPDATE"];
    let mut table_catalogs = Vec::new();
    let mut table_schemas = Vec::new();
    let mut table_names = Vec::new();
    let mut table_privilege_types = Vec::new();
    let mut column_catalogs = Vec::new();
    let mut column_schemas = Vec::new();
    let mut column_tables = Vec::new();
    let mut column_names = Vec::new();
    let mut column_privilege_types = Vec::new();
    for table in &tables {
        for privilege in table_privileges {
            table_catalogs.push(database_name.to_string());
            table_schemas.push(table.schema_name.clone());
            table_names.push(table.table_name.clone());
            table_privilege_types.push(privilege.to_string());
        }
        for column in &table.schema.columns {
            for privilege in column_privileges {
                column_catalogs.push(database_name.to_string());
                column_schemas.push(table.schema_name.clone());
                column_tables.push(table.table_name.clone());
                column_names.push(column.name.clone());
                column_privilege_types.push(privilege.to_string());
            }
        }
    }
    let table_row_count = table_catalogs.len();
    let table_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("grantor", ArrowDataType::Utf8, false),
        Field::new("grantee", ArrowDataType::Utf8, false),
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("privilege_type", ArrowDataType::Utf8, false),
        Field::new("is_grantable", ArrowDataType::Utf8, false),
        Field::new("with_hierarchy", ArrowDataType::Utf8, false),
    ]));
    let table_batch = RecordBatch::try_new(
        table_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["postgres"; table_row_count])),
            Arc::new(StringArray::from(vec!["postgres"; table_row_count])),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_privilege_types)),
            Arc::new(StringArray::from(vec!["YES"; table_row_count])),
            Arc::new(StringArray::from(vec!["NO"; table_row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build table_privileges batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(table_schema, vec![vec![table_batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build table_privileges table: {e}"),
        )
    })?;
    schema_provider
        .register_table("table_privileges".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register table_privileges: {e}")))?;

    let column_row_count = column_catalogs.len();
    let column_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("grantor", ArrowDataType::Utf8, false),
        Field::new("grantee", ArrowDataType::Utf8, false),
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("column_name", ArrowDataType::Utf8, false),
        Field::new("privilege_type", ArrowDataType::Utf8, false),
        Field::new("is_grantable", ArrowDataType::Utf8, false),
    ]));
    let column_batch = RecordBatch::try_new(
        column_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["postgres"; column_row_count])),
            Arc::new(StringArray::from(vec!["postgres"; column_row_count])),
            Arc::new(StringArray::from(column_catalogs)),
            Arc::new(StringArray::from(column_schemas)),
            Arc::new(StringArray::from(column_tables)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(StringArray::from(column_privilege_types)),
            Arc::new(StringArray::from(vec!["YES"; column_row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build column_privileges batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(column_schema, vec![vec![column_batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build column_privileges table: {e}"),
        )
    })?;
    schema_provider
        .register_table("column_privileges".to_string(), Arc::new(table))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register column_privileges: {e}"),
            )
        })?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_views(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let views = catalog.list_views(database_name).await?;
    let row_count = views.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("view_definition", ArrowDataType::Utf8, true),
        Field::new("check_option", ArrowDataType::Utf8, false),
        Field::new("is_updatable", ArrowDataType::Utf8, false),
        Field::new("is_insertable_into", ArrowDataType::Utf8, false),
        Field::new("is_trigger_updatable", ArrowDataType::Utf8, false),
        Field::new("is_trigger_deletable", ArrowDataType::Utf8, false),
        Field::new("is_trigger_insertable_into", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![database_name; row_count])),
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| view.schema_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| view.view_name.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                views
                    .iter()
                    .map(|view| Some(view.definition.clone()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["NONE"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build information_schema.views: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build views table: {e}")))?;
    schema_provider
        .register_table("views".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register views: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_information_schema_empty_views(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    register_empty_table(
        schema_provider,
        "sequences",
        vec![
            Field::new("sequence_catalog", ArrowDataType::Utf8, false),
            Field::new("sequence_schema", ArrowDataType::Utf8, false),
            Field::new("sequence_name", ArrowDataType::Utf8, false),
            Field::new("data_type", ArrowDataType::Utf8, false),
            Field::new("numeric_precision", ArrowDataType::Int32, true),
            Field::new("numeric_precision_radix", ArrowDataType::Int32, true),
            Field::new("numeric_scale", ArrowDataType::Int32, true),
            Field::new("start_value", ArrowDataType::Utf8, false),
            Field::new("minimum_value", ArrowDataType::Utf8, false),
            Field::new("maximum_value", ArrowDataType::Utf8, false),
            Field::new("increment", ArrowDataType::Utf8, false),
            Field::new("cycle_option", ArrowDataType::Utf8, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "routines",
        vec![
            Field::new("specific_catalog", ArrowDataType::Utf8, false),
            Field::new("specific_schema", ArrowDataType::Utf8, false),
            Field::new("specific_name", ArrowDataType::Utf8, false),
            Field::new("routine_catalog", ArrowDataType::Utf8, false),
            Field::new("routine_schema", ArrowDataType::Utf8, false),
            Field::new("routine_name", ArrowDataType::Utf8, false),
            Field::new("routine_type", ArrowDataType::Utf8, false),
            Field::new("data_type", ArrowDataType::Utf8, true),
            Field::new("type_udt_name", ArrowDataType::Utf8, true),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "parameters",
        vec![
            Field::new("specific_catalog", ArrowDataType::Utf8, false),
            Field::new("specific_schema", ArrowDataType::Utf8, false),
            Field::new("specific_name", ArrowDataType::Utf8, false),
            Field::new("ordinal_position", ArrowDataType::Int32, false),
            Field::new("parameter_mode", ArrowDataType::Utf8, true),
            Field::new("parameter_name", ArrowDataType::Utf8, true),
            Field::new("data_type", ArrowDataType::Utf8, true),
            Field::new("parameter_default", ArrowDataType::Utf8, true),
        ],
    )?;
    Ok(())
}

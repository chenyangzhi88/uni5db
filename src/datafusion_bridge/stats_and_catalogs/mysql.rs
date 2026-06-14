use super::*;

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    register_mysql_information_schema_tables(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_columns(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_schemata(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_statistics(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_constraints(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_views(catalog, schema_provider, database_name).await?;
    register_mysql_information_schema_static_tables(schema_provider)?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_tables(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let views = catalog.list_views(database_name).await?;
    let row_count = tables.len() + views.len();
    let mut table_catalogs = Vec::with_capacity(row_count);
    let mut table_schemas = Vec::with_capacity(row_count);
    let mut table_names = Vec::with_capacity(row_count);
    let mut table_types = Vec::with_capacity(row_count);
    let mut engines = Vec::with_capacity(row_count);
    let mut table_collations = Vec::with_capacity(row_count);

    for table in &tables {
        table_catalogs.push("def".to_string());
        table_schemas.push(database_name.to_string());
        table_names.push(table.table_name.clone());
        table_types.push("BASE TABLE".to_string());
        engines.push(Some("UniDB".to_string()));
        table_collations.push(Some("utf8mb4_0900_ai_ci".to_string()));
    }
    for view in &views {
        table_catalogs.push("def".to_string());
        table_schemas.push(database_name.to_string());
        table_names.push(view.view_name.clone());
        table_types.push("VIEW".to_string());
        engines.push(Option::<String>::None);
        table_collations.push(Option::<String>::None);
    }

    let null_i64 = vec![Option::<i64>::None; row_count];
    let null_strings = vec![Option::<String>::None; row_count];
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("table_type", ArrowDataType::Utf8, false),
        Field::new("engine", ArrowDataType::Utf8, true),
        Field::new("version", ArrowDataType::Int64, true),
        Field::new("row_format", ArrowDataType::Utf8, true),
        Field::new("table_rows", ArrowDataType::Int64, true),
        Field::new("avg_row_length", ArrowDataType::Int64, true),
        Field::new("data_length", ArrowDataType::Int64, true),
        Field::new("max_data_length", ArrowDataType::Int64, true),
        Field::new("index_length", ArrowDataType::Int64, true),
        Field::new("data_free", ArrowDataType::Int64, true),
        Field::new("auto_increment", ArrowDataType::Int64, true),
        Field::new("create_time", ArrowDataType::Utf8, true),
        Field::new("update_time", ArrowDataType::Utf8, true),
        Field::new("check_time", ArrowDataType::Utf8, true),
        Field::new("table_collation", ArrowDataType::Utf8, true),
        Field::new("checksum", ArrowDataType::Int64, true),
        Field::new("create_options", ArrowDataType::Utf8, true),
        Field::new("table_comment", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_types)),
            Arc::new(StringArray::from(engines)),
            Arc::new(Int64Array::from(vec![Some(10_i64); row_count])),
            Arc::new(StringArray::from(vec![
                Some("Dynamic".to_string());
                row_count
            ])),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(null_strings.clone())),
            Arc::new(StringArray::from(table_collations)),
            Arc::new(Int64Array::from(null_i64.clone())),
            Arc::new(StringArray::from(vec![Some(String::new()); row_count])),
            Arc::new(StringArray::from(vec![String::new(); row_count])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build mysql tables batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql tables: {e}")))?;
    schema_provider
        .register_table("tables".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql tables: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_columns(
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
    let mut column_defaults = Vec::new();
    let mut is_nullables = Vec::new();
    let mut data_types = Vec::new();
    let mut char_lengths = Vec::new();
    let mut char_octet_lengths = Vec::new();
    let mut numeric_precisions = Vec::new();
    let mut numeric_scales = Vec::new();
    let mut datetime_precisions = Vec::new();
    let mut character_sets = Vec::new();
    let mut collations = Vec::new();
    let mut column_types = Vec::new();
    let mut column_keys = Vec::new();
    let mut extras = Vec::new();

    for table in tables {
        let indexes = catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?;
        for (idx, column) in table.schema.columns.iter().enumerate() {
            let column_type = mysql_info_column_type(&column.data_type);
            let char_length = mysql_info_character_length(&column.data_type);
            let (numeric_precision, numeric_scale) =
                mysql_info_numeric_precision_scale(&column.data_type);
            let is_auto_increment = mysql_info_is_auto_increment(column);
            table_catalogs.push("def".to_string());
            table_schemas.push(database_name.to_string());
            table_names.push(table.table_name.clone());
            column_names.push(column.name.clone());
            ordinal_positions.push(idx as i32 + 1);
            column_defaults.push(if is_auto_increment {
                Option::<String>::None
            } else {
                column.default.clone()
            });
            is_nullables.push(if column.nullable && !column.primary_key {
                "YES".to_string()
            } else {
                "NO".to_string()
            });
            data_types.push(mysql_info_data_type(&column.data_type).to_string());
            char_lengths.push(char_length);
            char_octet_lengths.push(char_length.map(|len| len.saturating_mul(4)));
            numeric_precisions.push(numeric_precision);
            numeric_scales.push(numeric_scale);
            datetime_precisions.push(mysql_info_datetime_precision(&column.data_type));
            if mysql_info_is_character_type(&column.data_type) {
                character_sets.push(
                    column
                        .character_set
                        .clone()
                        .or_else(|| Some("utf8mb4".to_string())),
                );
                collations.push(
                    column
                        .collation
                        .clone()
                        .or_else(|| Some("utf8mb4_0900_ai_ci".to_string())),
                );
            } else {
                character_sets.push(Option::<String>::None);
                collations.push(Option::<String>::None);
            }
            column_types.push(column_type);
            column_keys.push(mysql_info_column_key(&table.schema, &indexes, &column.name));
            let mut extra_parts = Vec::new();
            if is_auto_increment {
                extra_parts.push("auto_increment".to_string());
            }
            if let Some(on_update) = &column.on_update {
                extra_parts.push(format!("on update {on_update}"));
            }
            extras.push(extra_parts.join(" "));
        }
    }

    let row_count = table_catalogs.len();
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
        Field::new("numeric_scale", ArrowDataType::Int32, true),
        Field::new("datetime_precision", ArrowDataType::Int32, true),
        Field::new("character_set_name", ArrowDataType::Utf8, true),
        Field::new("collation_name", ArrowDataType::Utf8, true),
        Field::new("column_type", ArrowDataType::Utf8, false),
        Field::new("column_key", ArrowDataType::Utf8, false),
        Field::new("extra", ArrowDataType::Utf8, false),
        Field::new("privileges", ArrowDataType::Utf8, false),
        Field::new("column_comment", ArrowDataType::Utf8, false),
        Field::new("generation_expression", ArrowDataType::Utf8, false),
        Field::new("srs_id", ArrowDataType::Int32, true),
    ]));
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
            Arc::new(Int32Array::from(char_lengths)),
            Arc::new(Int32Array::from(char_octet_lengths)),
            Arc::new(Int32Array::from(numeric_precisions)),
            Arc::new(Int32Array::from(numeric_scales)),
            Arc::new(Int32Array::from(datetime_precisions)),
            Arc::new(StringArray::from(character_sets)),
            Arc::new(StringArray::from(collations)),
            Arc::new(StringArray::from(column_types)),
            Arc::new(StringArray::from(column_keys)),
            Arc::new(StringArray::from(extras)),
            Arc::new(StringArray::from(vec![
                "select,insert,update,references";
                row_count
            ])),
            Arc::new(StringArray::from(vec![String::new(); row_count])),
            Arc::new(StringArray::from(vec![String::new(); row_count])),
            Arc::new(Int32Array::from(vec![Option::<i32>::None; row_count])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build mysql columns batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql columns: {e}")))?;
    schema_provider
        .register_table("columns".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql columns: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_schemata(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let databases = catalog.list_databases().await?;
    let row_count = databases.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("catalog_name", ArrowDataType::Utf8, false),
        Field::new("schema_name", ArrowDataType::Utf8, false),
        Field::new("default_character_set_name", ArrowDataType::Utf8, false),
        Field::new("default_collation_name", ArrowDataType::Utf8, false),
        Field::new("sql_path", ArrowDataType::Utf8, true),
        Field::new("default_encryption", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["def"; row_count])),
            Arc::new(StringArray::from(
                databases
                    .into_iter()
                    .map(|database| database.database_name)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["utf8mb4"; row_count])),
            Arc::new(StringArray::from(vec!["utf8mb4_0900_ai_ci"; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec!["NO"; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql schemata batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql schemata: {e}")))?;
    schema_provider
        .register_table("schemata".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql schemata: {e}")))?;
    let _ = database_name;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_statistics(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let tables = catalog.list_tables(database_name).await?;
    let mut table_catalogs = Vec::new();
    let mut table_schemas = Vec::new();
    let mut table_names = Vec::new();
    let mut non_uniques = Vec::new();
    let mut index_schemas = Vec::new();
    let mut index_names = Vec::new();
    let mut seq_in_indexes = Vec::new();
    let mut column_names = Vec::new();
    let mut nullables = Vec::new();

    for table in tables {
        if table.schema.has_user_primary_key() {
            table_catalogs.push("def".to_string());
            table_schemas.push(database_name.to_string());
            table_names.push(table.table_name.clone());
            non_uniques.push(0);
            index_schemas.push(database_name.to_string());
            index_names.push("PRIMARY".to_string());
            seq_in_indexes.push(1);
            column_names.push(table.schema.primary_key.clone());
            nullables.push(String::new());
        }
        for constraint in &table.schema.unique_constraints {
            if constraint.primary_key {
                continue;
            }
            for (idx, column_name) in constraint.columns.iter().enumerate() {
                table_catalogs.push("def".to_string());
                table_schemas.push(database_name.to_string());
                table_names.push(table.table_name.clone());
                non_uniques.push(0);
                index_schemas.push(database_name.to_string());
                index_names.push(constraint.name.clone());
                seq_in_indexes.push(idx as i32 + 1);
                column_names.push(column_name.clone());
                nullables.push(mysql_info_column_nullable(&table.schema, column_name));
            }
        }
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            for (idx, column_name) in index.column_names.iter().enumerate() {
                table_catalogs.push("def".to_string());
                table_schemas.push(database_name.to_string());
                table_names.push(table.table_name.clone());
                non_uniques.push(if index.unique { 0 } else { 1 });
                index_schemas.push(database_name.to_string());
                index_names.push(index.index_name.clone());
                seq_in_indexes.push(idx as i32 + 1);
                column_names.push(column_name.clone());
                nullables.push(mysql_info_column_nullable(&table.schema, column_name));
            }
        }
    }

    let row_count = table_catalogs.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("table_catalog", ArrowDataType::Utf8, false),
        Field::new("table_schema", ArrowDataType::Utf8, false),
        Field::new("table_name", ArrowDataType::Utf8, false),
        Field::new("non_unique", ArrowDataType::Int32, false),
        Field::new("index_schema", ArrowDataType::Utf8, false),
        Field::new("index_name", ArrowDataType::Utf8, false),
        Field::new("seq_in_index", ArrowDataType::Int32, false),
        Field::new("column_name", ArrowDataType::Utf8, true),
        Field::new("collation", ArrowDataType::Utf8, true),
        Field::new("cardinality", ArrowDataType::Int64, true),
        Field::new("sub_part", ArrowDataType::Int64, true),
        Field::new("packed", ArrowDataType::Utf8, true),
        Field::new("nullable", ArrowDataType::Utf8, false),
        Field::new("index_type", ArrowDataType::Utf8, false),
        Field::new("comment", ArrowDataType::Utf8, false),
        Field::new("index_comment", ArrowDataType::Utf8, false),
        Field::new("is_visible", ArrowDataType::Utf8, false),
        Field::new("expression", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(Int32Array::from(non_uniques)),
            Arc::new(StringArray::from(index_schemas)),
            Arc::new(StringArray::from(index_names)),
            Arc::new(Int32Array::from(seq_in_indexes)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(StringArray::from(vec![Some("A".to_string()); row_count])),
            Arc::new(Int64Array::from(vec![Option::<i64>::None; row_count])),
            Arc::new(Int64Array::from(vec![Option::<i64>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(nullables)),
            Arc::new(StringArray::from(vec!["BTREE"; row_count])),
            Arc::new(StringArray::from(vec![String::new(); row_count])),
            Arc::new(StringArray::from(vec![String::new(); row_count])),
            Arc::new(StringArray::from(vec!["YES"; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql statistics batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql statistics: {e}")))?;
    schema_provider
        .register_table("statistics".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql statistics: {e}")))?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_constraints(
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
            mysql_info_push_constraint(
                database_name,
                &table.table_name,
                "PRIMARY",
                "PRIMARY KEY",
                &table.schema.primary_key,
                1,
                &mut constraint_catalogs,
                &mut constraint_schemas,
                &mut constraint_names,
                &mut table_catalogs,
                &mut table_schemas,
                &mut table_names,
                &mut constraint_types,
                &mut column_names,
                &mut ordinal_positions,
            );
        }
        for constraint in &table.schema.unique_constraints {
            if constraint.primary_key {
                continue;
            }
            for (idx, column_name) in constraint.columns.iter().enumerate() {
                mysql_info_push_constraint(
                    database_name,
                    &table.table_name,
                    &constraint.name,
                    "UNIQUE",
                    column_name,
                    idx as i32 + 1,
                    &mut constraint_catalogs,
                    &mut constraint_schemas,
                    &mut constraint_names,
                    &mut table_catalogs,
                    &mut table_schemas,
                    &mut table_names,
                    &mut constraint_types,
                    &mut column_names,
                    &mut ordinal_positions,
                );
            }
        }
        for index in catalog
            .list_indexes_for_table(table.schema.table_id)
            .await?
        {
            if !index.unique {
                continue;
            }
            for (idx, column_name) in index.column_names.iter().enumerate() {
                mysql_info_push_constraint(
                    database_name,
                    &table.table_name,
                    &index.index_name,
                    "UNIQUE",
                    column_name,
                    idx as i32 + 1,
                    &mut constraint_catalogs,
                    &mut constraint_schemas,
                    &mut constraint_names,
                    &mut table_catalogs,
                    &mut table_schemas,
                    &mut table_names,
                    &mut constraint_types,
                    &mut column_names,
                    &mut ordinal_positions,
                );
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
            Arc::new(StringArray::from(vec!["YES"; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql table_constraints batch: {e}"),
        )
    })?;
    let table_constraints = MemTable::try_new(
        table_constraints_schema,
        vec![vec![table_constraints_batch]],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql table_constraints: {e}"),
        )
    })?;
    schema_provider
        .register_table("table_constraints".to_string(), Arc::new(table_constraints))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register mysql table_constraints: {e}"),
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
        Field::new("referenced_table_schema", ArrowDataType::Utf8, true),
        Field::new("referenced_table_name", ArrowDataType::Utf8, true),
        Field::new("referenced_column_name", ArrowDataType::Utf8, true),
    ]));
    let key_batch = RecordBatch::try_new(
        key_schema.clone(),
        vec![
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(Int32Array::from(vec![Option::<i32>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
            Arc::new(StringArray::from(vec![Option::<String>::None; row_count])),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql key_column_usage batch: {e}"),
        )
    })?;
    let key_table = MemTable::try_new(key_schema, vec![vec![key_batch]]).map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql key_column_usage: {e}"),
        )
    })?;
    schema_provider
        .register_table("key_column_usage".to_string(), Arc::new(key_table))
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register mysql key_column_usage: {e}"),
            )
        })?;

    register_empty_table(
        schema_provider,
        "referential_constraints",
        vec![
            Field::new("constraint_catalog", ArrowDataType::Utf8, false),
            Field::new("constraint_schema", ArrowDataType::Utf8, false),
            Field::new("constraint_name", ArrowDataType::Utf8, false),
            Field::new("unique_constraint_catalog", ArrowDataType::Utf8, false),
            Field::new("unique_constraint_schema", ArrowDataType::Utf8, false),
            Field::new("unique_constraint_name", ArrowDataType::Utf8, true),
            Field::new("match_option", ArrowDataType::Utf8, false),
            Field::new("update_rule", ArrowDataType::Utf8, false),
            Field::new("delete_rule", ArrowDataType::Utf8, false),
            Field::new("table_name", ArrowDataType::Utf8, false),
            Field::new("referenced_table_name", ArrowDataType::Utf8, false),
        ],
    )?;
    Ok(())
}

pub(in crate::datafusion_bridge) async fn register_mysql_information_schema_views(
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
        Field::new("definer", ArrowDataType::Utf8, false),
        Field::new("security_type", ArrowDataType::Utf8, false),
        Field::new("character_set_client", ArrowDataType::Utf8, false),
        Field::new("collation_connection", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["def"; row_count])),
            Arc::new(StringArray::from(vec![database_name; row_count])),
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
            Arc::new(StringArray::from(vec!["root@%"; row_count])),
            Arc::new(StringArray::from(vec!["DEFINER"; row_count])),
            Arc::new(StringArray::from(vec!["utf8mb4"; row_count])),
            Arc::new(StringArray::from(vec!["utf8mb4_0900_ai_ci"; row_count])),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build mysql views batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql views: {e}")))?;
    schema_provider
        .register_table("views".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql views: {e}")))?;
    Ok(())
}

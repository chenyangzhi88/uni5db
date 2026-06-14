use super::*;

pub(super) fn register_mysql_information_schema_static_tables(
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    register_one_row_table(
        schema_provider,
        "engines",
        vec![
            Field::new("engine", ArrowDataType::Utf8, false),
            Field::new("support", ArrowDataType::Utf8, false),
            Field::new("comment", ArrowDataType::Utf8, false),
            Field::new("transactions", ArrowDataType::Utf8, false),
            Field::new("xa", ArrowDataType::Utf8, false),
            Field::new("savepoints", ArrowDataType::Utf8, false),
        ],
        vec![
            Some("UniDB"),
            Some("DEFAULT"),
            Some("UniDB KV storage engine"),
            Some("YES"),
            Some("NO"),
            Some("YES"),
        ],
    )?;
    register_one_row_table(
        schema_provider,
        "character_sets",
        vec![
            Field::new("character_set_name", ArrowDataType::Utf8, false),
            Field::new("default_collate_name", ArrowDataType::Utf8, false),
            Field::new("description", ArrowDataType::Utf8, false),
            Field::new("maxlen", ArrowDataType::Int32, false),
        ],
        vec![
            Some("utf8mb4"),
            Some("utf8mb4_0900_ai_ci"),
            Some("UTF-8 Unicode"),
            Some("4"),
        ],
    )?;
    register_one_row_table(
        schema_provider,
        "collations",
        vec![
            Field::new("collation_name", ArrowDataType::Utf8, false),
            Field::new("character_set_name", ArrowDataType::Utf8, false),
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("is_default", ArrowDataType::Utf8, false),
            Field::new("is_compiled", ArrowDataType::Utf8, false),
            Field::new("sortlen", ArrowDataType::Int32, false),
            Field::new("pad_attribute", ArrowDataType::Utf8, false),
        ],
        vec![
            Some("utf8mb4_0900_ai_ci"),
            Some("utf8mb4"),
            Some("255"),
            Some("Yes"),
            Some("Yes"),
            Some("0"),
            Some("NO PAD"),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "processlist",
        vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("user", ArrowDataType::Utf8, false),
            Field::new("host", ArrowDataType::Utf8, false),
            Field::new("db", ArrowDataType::Utf8, true),
            Field::new("command", ArrowDataType::Utf8, false),
            Field::new("time", ArrowDataType::Int32, false),
            Field::new("state", ArrowDataType::Utf8, true),
            Field::new("info", ArrowDataType::Utf8, true),
        ],
    )?;
    register_mysql_variables_table(schema_provider, "global_variables")?;
    register_mysql_variables_table(schema_provider, "session_variables")?;
    register_empty_table(
        schema_provider,
        "routines",
        vec![
            Field::new("specific_name", ArrowDataType::Utf8, false),
            Field::new("routine_schema", ArrowDataType::Utf8, false),
            Field::new("routine_name", ArrowDataType::Utf8, false),
            Field::new("routine_type", ArrowDataType::Utf8, false),
            Field::new("data_type", ArrowDataType::Utf8, true),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "parameters",
        vec![
            Field::new("specific_name", ArrowDataType::Utf8, false),
            Field::new("ordinal_position", ArrowDataType::Int32, false),
            Field::new("parameter_mode", ArrowDataType::Utf8, true),
            Field::new("parameter_name", ArrowDataType::Utf8, true),
            Field::new("data_type", ArrowDataType::Utf8, true),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "table_privileges",
        vec![
            Field::new("grantee", ArrowDataType::Utf8, false),
            Field::new("table_catalog", ArrowDataType::Utf8, false),
            Field::new("table_schema", ArrowDataType::Utf8, false),
            Field::new("table_name", ArrowDataType::Utf8, false),
            Field::new("privilege_type", ArrowDataType::Utf8, false),
            Field::new("is_grantable", ArrowDataType::Utf8, false),
        ],
    )?;
    register_empty_table(
        schema_provider,
        "column_privileges",
        vec![
            Field::new("grantee", ArrowDataType::Utf8, false),
            Field::new("table_catalog", ArrowDataType::Utf8, false),
            Field::new("table_schema", ArrowDataType::Utf8, false),
            Field::new("table_name", ArrowDataType::Utf8, false),
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("privilege_type", ArrowDataType::Utf8, false),
            Field::new("is_grantable", ArrowDataType::Utf8, false),
        ],
    )?;
    Ok(())
}

pub(super) fn register_mysql_variables_table(
    schema_provider: &Arc<dyn SchemaProvider>,
    table_name: &str,
) -> PgWireResult<()> {
    let variables = mysql_info_variables();
    let row_count = variables.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("variable_name", ArrowDataType::Utf8, false),
        Field::new("variable_value", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                variables
                    .iter()
                    .map(|(name, _)| (*name).to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                variables
                    .iter()
                    .map(|(_, value)| (*value).to_string())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| {
        user_error(
            "XX000",
            format!("failed to build mysql variables batch: {e}"),
        )
    })?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build mysql variables: {e}")))?;
    schema_provider
        .register_table(table_name.to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register mysql variables: {e}")))?;
    let _ = row_count;
    Ok(())
}

pub(super) fn register_one_row_table(
    schema_provider: &Arc<dyn SchemaProvider>,
    table_name: &str,
    fields: Vec<Field>,
    values: Vec<Option<&str>>,
) -> PgWireResult<()> {
    let schema = Arc::new(ArrowSchema::new(fields));
    let arrays = schema
        .fields()
        .iter()
        .zip(values)
        .map(|(field, value)| match field.data_type() {
            ArrowDataType::Int32 => Arc::new(Int32Array::from(vec![
                value.and_then(|value| value.parse::<i32>().ok()),
            ])) as ArrayRef,
            ArrowDataType::Int64 => Arc::new(Int64Array::from(vec![
                value.and_then(|value| value.parse::<i64>().ok()),
            ])) as ArrayRef,
            _ => Arc::new(StringArray::from(vec![value.map(str::to_string)])) as ArrayRef,
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| user_error("XX000", format!("failed to build {table_name} batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build {table_name}: {e}")))?;
    schema_provider
        .register_table(table_name.to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register {table_name}: {e}")))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn mysql_info_push_constraint(
    database_name: &str,
    table_name: &str,
    constraint_name: &str,
    constraint_type: &str,
    column_name: &str,
    ordinal_position: i32,
    constraint_catalogs: &mut Vec<String>,
    constraint_schemas: &mut Vec<String>,
    constraint_names: &mut Vec<String>,
    table_catalogs: &mut Vec<String>,
    table_schemas: &mut Vec<String>,
    table_names: &mut Vec<String>,
    constraint_types: &mut Vec<String>,
    column_names: &mut Vec<String>,
    ordinal_positions: &mut Vec<i32>,
) {
    constraint_catalogs.push("def".to_string());
    constraint_schemas.push(database_name.to_string());
    constraint_names.push(constraint_name.to_string());
    table_catalogs.push("def".to_string());
    table_schemas.push(database_name.to_string());
    table_names.push(table_name.to_string());
    constraint_types.push(constraint_type.to_string());
    column_names.push(column_name.to_string());
    ordinal_positions.push(ordinal_position);
}

pub(super) fn mysql_info_data_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::MySqlInt { kind, .. } => match kind {
            crate::types::MySqlIntKind::Tiny => "tinyint",
            crate::types::MySqlIntKind::Small => "smallint",
            crate::types::MySqlIntKind::Medium => "mediumint",
            crate::types::MySqlIntKind::Int => "int",
            crate::types::MySqlIntKind::Big => "bigint",
        },
        DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::MySqlFloat { .. } => "float",
        DataType::MySqlDouble { .. } => "double",
        DataType::Numeric { .. } => "decimal",
        DataType::Text => "text",
        DataType::MySqlText { type_name, .. } => type_name,
        DataType::VarChar(_) => "varchar",
        DataType::Char(_) => "char",
        DataType::Binary(_) => "binary",
        DataType::VarBinary(_) => "varbinary",
        DataType::Blob { type_name, .. } => type_name,
        DataType::Boolean => "tinyint",
        DataType::Bit(_) => "bit",
        DataType::Year => "year",
        DataType::Date => "date",
        DataType::Time | DataType::TimeTz => "time",
        DataType::MySqlTime { .. } => "time",
        DataType::Timestamp | DataType::TimestampTz => "datetime",
        DataType::MySqlDateTime { .. } => "datetime",
        DataType::MySqlTimestamp { .. } => "timestamp",
        DataType::Bytea => "blob",
        DataType::Json | DataType::Jsonb | DataType::Array(_) => "json",
        DataType::Uuid => "char",
        DataType::Interval => "text",
        DataType::Domain(_) => "text",
        DataType::Enum(_) => "enum",
        DataType::Set(_) => "set",
        DataType::Geometry(name) => match name.as_str() {
            "point" => "point",
            "linestring" => "linestring",
            "polygon" => "polygon",
            "multipoint" => "multipoint",
            "multilinestring" => "multilinestring",
            "multipolygon" => "multipolygon",
            "geometrycollection" => "geometrycollection",
            _ => "geometry",
        },
    }
}

pub(super) fn mysql_info_column_type(data_type: &DataType) -> String {
    match data_type {
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::MySqlInt { kind, unsigned } => format!(
            "{}{}",
            kind.as_mysql_name().to_ascii_lowercase(),
            if *unsigned { " unsigned" } else { "" }
        ),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::MySqlFloat { precision } => match precision {
            Some(precision) => format!("float({precision})"),
            None => "float".to_string(),
        },
        DataType::MySqlDouble { precision } => match precision {
            Some(precision) => format!("double({precision})"),
            None => "double".to_string(),
        },
        DataType::Numeric { precision, scale } => match (precision, scale) {
            (Some(precision), Some(scale)) => format!("decimal({precision},{scale})"),
            (Some(precision), None) => format!("decimal({precision})"),
            _ => "decimal".to_string(),
        },
        DataType::Text => "text".to_string(),
        DataType::MySqlText { type_name, .. } => type_name.to_string(),
        DataType::VarChar(Some(len)) => format!("varchar({len})"),
        DataType::VarChar(None) => "varchar".to_string(),
        DataType::Char(Some(len)) => format!("char({len})"),
        DataType::Char(None) => "char(1)".to_string(),
        DataType::Binary(Some(len)) => format!("binary({len})"),
        DataType::Binary(None) => "binary".to_string(),
        DataType::VarBinary(Some(len)) => format!("varbinary({len})"),
        DataType::VarBinary(None) => "varbinary".to_string(),
        DataType::Blob { type_name, .. } => type_name.to_string(),
        DataType::Boolean => "tinyint(1)".to_string(),
        DataType::Bit(Some(len)) => format!("bit({len})"),
        DataType::Bit(None) => "bit".to_string(),
        DataType::Year => "year".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time | DataType::TimeTz => "time".to_string(),
        DataType::MySqlTime { fsp } => match fsp {
            Some(fsp) => format!("time({fsp})"),
            None => "time".to_string(),
        },
        DataType::Timestamp | DataType::TimestampTz => "datetime".to_string(),
        DataType::MySqlDateTime { fsp } => match fsp {
            Some(fsp) => format!("datetime({fsp})"),
            None => "datetime".to_string(),
        },
        DataType::MySqlTimestamp { fsp } => match fsp {
            Some(fsp) => format!("timestamp({fsp})"),
            None => "timestamp".to_string(),
        },
        DataType::Bytea => "blob".to_string(),
        DataType::Json | DataType::Jsonb | DataType::Array(_) => "json".to_string(),
        DataType::Uuid => "char(36)".to_string(),
        DataType::Interval => "text".to_string(),
        DataType::Domain(name) => name.to_ascii_lowercase(),
        DataType::Enum(values) => format!("enum({})", mysql_info_quote_values(values)),
        DataType::Set(values) => format!("set({})", mysql_info_quote_values(values)),
        DataType::Geometry(name) => name.clone(),
    }
}

pub(super) fn mysql_info_character_length(data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::MySqlText { max_len, .. } => max_len.and_then(|len| i32::try_from(len).ok()),
        DataType::VarChar(Some(len)) | DataType::Char(Some(len)) => Some(*len as i32),
        DataType::Char(None) => Some(1),
        DataType::Binary(Some(len)) | DataType::VarBinary(Some(len)) => Some(*len as i32),
        DataType::Binary(None) => Some(1),
        DataType::Uuid => Some(36),
        _ => None,
    }
}

pub(super) fn mysql_info_numeric_precision_scale(
    data_type: &DataType,
) -> (Option<i32>, Option<i32>) {
    match data_type {
        DataType::Int16 => (Some(5), Some(0)),
        DataType::Int32 => (Some(10), Some(0)),
        DataType::Int64 => (Some(19), Some(0)),
        DataType::MySqlInt { kind, unsigned } => {
            let precision = match (kind, unsigned) {
                (crate::types::MySqlIntKind::Tiny, false) => 3,
                (crate::types::MySqlIntKind::Tiny, true) => 3,
                (crate::types::MySqlIntKind::Small, false) => 5,
                (crate::types::MySqlIntKind::Small, true) => 5,
                (crate::types::MySqlIntKind::Medium, false) => 7,
                (crate::types::MySqlIntKind::Medium, true) => 8,
                (crate::types::MySqlIntKind::Int, false) => 10,
                (crate::types::MySqlIntKind::Int, true) => 10,
                (crate::types::MySqlIntKind::Big, false) => 19,
                (crate::types::MySqlIntKind::Big, true) => 20,
            };
            (Some(precision), Some(0))
        }
        DataType::Float32 => (Some(12), None),
        DataType::Float64 => (Some(22), None),
        DataType::MySqlFloat { precision } => (precision.map(|v| v as i32).or(Some(12)), None),
        DataType::MySqlDouble { precision } => (precision.map(|v| v as i32).or(Some(22)), None),
        DataType::Numeric { precision, scale } => (
            precision.map(|v| v as i32).or(Some(10)),
            scale.map(|v| v as i32).or(Some(0)),
        ),
        _ => (None, None),
    }
}

pub(super) fn mysql_info_datetime_precision(data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::Time | DataType::TimeTz | DataType::Timestamp | DataType::TimestampTz => Some(0),
        DataType::MySqlTime { fsp }
        | DataType::MySqlDateTime { fsp }
        | DataType::MySqlTimestamp { fsp } => fsp.map(|v| v as i32).or(Some(0)),
        _ => None,
    }
}

pub(super) fn mysql_info_quote_values(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn mysql_info_is_character_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Uuid
    )
}

pub(super) fn mysql_info_is_auto_increment(column: &crate::types::ColumnSchema) -> bool {
    column
        .default
        .as_deref()
        .map(|default| default.to_ascii_lowercase().contains("nextval("))
        .unwrap_or(false)
}

pub(super) fn mysql_info_column_key(
    schema: &TableSchema,
    indexes: &[IndexCatalog],
    column_name: &str,
) -> String {
    if schema.primary_key.eq_ignore_ascii_case(column_name)
        || schema
            .columns
            .iter()
            .any(|column| column.primary_key && column.name.eq_ignore_ascii_case(column_name))
    {
        "PRI".to_string()
    } else if schema.unique_constraints.iter().any(|constraint| {
        !constraint.primary_key
            && constraint.columns.len() == 1
            && constraint.columns[0].eq_ignore_ascii_case(column_name)
    }) || indexes.iter().any(|index| {
        index.unique
            && index.column_names.len() == 1
            && index.column_names[0].eq_ignore_ascii_case(column_name)
    }) {
        "UNI".to_string()
    } else if indexes.iter().any(|index| {
        index
            .column_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(column_name))
    }) {
        "MUL".to_string()
    } else {
        String::new()
    }
}

pub(super) fn mysql_info_column_nullable(schema: &TableSchema, column_name: &str) -> String {
    schema
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .map(|column| {
            if column.nullable && !column.primary_key {
                "YES".to_string()
            } else {
                String::new()
            }
        })
        .unwrap_or_default()
}

pub(super) fn mysql_info_variables() -> Vec<(&'static str, &'static str)> {
    vec![
        ("autocommit", "ON"),
        ("character_set_client", "utf8mb4"),
        ("character_set_connection", "utf8mb4"),
        ("character_set_database", "utf8mb4"),
        ("character_set_results", "utf8mb4"),
        ("character_set_server", "utf8mb4"),
        ("collation_connection", "utf8mb4_0900_ai_ci"),
        ("collation_database", "utf8mb4_0900_ai_ci"),
        ("collation_server", "utf8mb4_0900_ai_ci"),
        ("default_storage_engine", "UniDB"),
        ("lower_case_table_names", "0"),
        (
            "sql_mode",
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
        ),
        ("time_zone", "+00:00"),
        ("transaction_isolation", "REPEATABLE-READ"),
        ("tx_isolation", "REPEATABLE-READ"),
        ("version", "8.0.0-unidb"),
        ("version_comment", "UniDB MySQL compatibility layer"),
    ]
}

// ── Arrow → pgwire response ─────────────────────────────────────────

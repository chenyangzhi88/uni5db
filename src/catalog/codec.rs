use super::*;

pub fn decode_table_schema(_table_name: &str, bytes: &[u8]) -> PgWireResult<TableSchema> {
    let catalog = decode_table_catalog(bytes)?;
    Ok(catalog.schema)
}

pub fn resolve_table_reference(name: &str) -> PgWireResult<(String, String)> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.as_slice() {
        [table] => Ok((DEFAULT_SCHEMA_NAME.to_string(), (*table).to_string())),
        [schema, table] => Ok(((*schema).to_string(), (*table).to_string())),
        _ => Err(unsupported(
            "only table or schema.table references are supported",
        )),
    }
}

pub fn schema_name_to_string(schema_name: &SchemaName) -> PgWireResult<String> {
    match schema_name {
        SchemaName::Simple(name) => object_name_to_string(name),
        SchemaName::NamedAuthorization(name, _) => object_name_to_string(name),
        SchemaName::UnnamedAuthorization(ident) => Ok(ident.value.clone()),
    }
}

pub fn object_name_to_string(name: &ObjectName) -> PgWireResult<String> {
    if name.0.is_empty() {
        return Err(user_error("42601", "object name is empty"));
    }
    Ok(name.to_string())
}

pub(super) fn database_by_name_key(database_name: &str) -> String {
    format!("__catalog__/databases/by-name/{database_name}")
}

pub(super) fn database_by_id_key(database_id: u32) -> String {
    format!("__catalog__/databases/by-id/{database_id}")
}

pub(super) fn schema_by_name_key(database_id: u32, schema_name: &str) -> String {
    format!("__catalog__/schemas/by-name/{database_id}/{schema_name}")
}

pub(super) fn schema_by_id_key(schema_id: u32) -> String {
    format!("__catalog__/schemas/by-id/{schema_id}")
}

pub(super) fn table_by_name_key(database_id: u32, schema_id: u32, table_name: &str) -> String {
    format!("__catalog__/tables/by-name/{database_id}/{schema_id}/{table_name}")
}

pub(super) fn table_by_id_key(table_id: u32) -> String {
    format!("__catalog__/tables/by-id/{table_id}")
}

pub(super) fn index_by_name_key(database_id: u32, schema_id: u32, index_name: &str) -> String {
    format!("__catalog__/indexes/by-name/{database_id}/{schema_id}/{index_name}")
}

pub(super) fn index_by_id_key(index_id: u32) -> String {
    format!("__catalog__/indexes/by-id/{index_id}")
}

pub(super) fn index_by_table_prefix(table_id: u32) -> String {
    format!("__catalog__/indexes/by-table/{table_id}/")
}

pub(super) fn index_by_table_key(table_id: u32, index_id: u32) -> String {
    format!("{}{index_id}", index_by_table_prefix(table_id))
}

pub(super) fn view_by_name_key(database_id: u32, schema_id: u32, view_name: &str) -> String {
    format!("__catalog__/views/by-name/{database_id}/{schema_id}/{view_name}")
}

pub(super) fn view_by_id_key(view_id: u32) -> String {
    format!("__catalog__/views/by-id/{view_id}")
}

pub(super) fn encode_database_catalog(database: &DatabaseCatalog) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "database_id": database.database_id,
        "database_name": database.database_name,
    }))
    .map_err(|e| user_error("XX000", format!("database encode error: {e}")))
}

pub(super) fn decode_database_catalog(bytes: &[u8]) -> PgWireResult<DatabaseCatalog> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| user_error("XX000", format!("database decode error: {e}")))?;
    Ok(DatabaseCatalog {
        database_id: value
            .get("database_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog database_id is malformed"))?
            as u32,
        database_name: value
            .get("database_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog database_name is malformed"))?
            .to_string(),
    })
}

pub(super) fn encode_schema_catalog(schema: &SchemaCatalog) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "schema_id": schema.schema_id,
        "database_id": schema.database_id,
        "schema_name": schema.schema_name,
    }))
    .map_err(|e| user_error("XX000", format!("schema encode error: {e}")))
}

pub(super) fn decode_schema_catalog(bytes: &[u8]) -> PgWireResult<SchemaCatalog> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| user_error("XX000", format!("schema decode error: {e}")))?;
    Ok(SchemaCatalog {
        schema_id: value
            .get("schema_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog schema_id is malformed"))?
            as u32,
        database_id: value
            .get("database_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog database_id is malformed"))?
            as u32,
        schema_name: value
            .get("schema_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog schema_name is malformed"))?
            .to_string(),
    })
}

pub(super) fn encode_table_catalog(table: &TableCatalog) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "database_id": table.database_id,
        "schema_id": table.schema_id,
        "schema_name": table.schema_name,
        "table_name": table.table_name,
        "table_id": table.schema.table_id,
        "schema_version": table.schema.schema_version,
        "table_epoch": table.schema.table_epoch,
        "primary_key": table.schema.primary_key,
        "check_constraints": table.schema.check_constraints.iter().map(|c| json!({
            "name": c.name,
            "expr": c.expr,
        })).collect::<Vec<_>>(),
        "unique_constraints": table.schema.unique_constraints.iter().map(|c| json!({
            "name": c.name,
            "columns": c.columns,
            "primary_key": c.primary_key,
        })).collect::<Vec<_>>(),
        "foreign_keys": table.schema.foreign_keys.iter().map(|c| json!({
            "name": c.name,
            "columns": c.columns,
            "foreign_table": c.foreign_table,
            "referred_columns": c.referred_columns,
        })).collect::<Vec<_>>(),
        "columns": table.schema.columns.iter().map(|c| {
            json!({
                "column_id": c.column_id,
                "name": c.name,
                "data_type": c.data_type.to_str(),
                "primary_key": c.primary_key,
                "nullable": c.nullable,
                "default": c.default.clone(),
                "on_update": c.on_update.clone(),
                "character_set": c.character_set.clone(),
                "collation": c.collation.clone(),
            })
        }).collect::<Vec<_>>(),
    }))
    .map_err(|e| user_error("XX000", format!("table encode error: {e}")))
}

pub(super) fn encode_legacy_table_schema(schema: &TableSchema) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "table_name": schema.table_name,
        "table_id": schema.table_id,
        "schema_version": schema.schema_version,
        "table_epoch": schema.table_epoch,
        "primary_key": schema.primary_key,
        "check_constraints": schema.check_constraints.iter().map(|c| json!({
            "name": c.name,
            "expr": c.expr,
        })).collect::<Vec<_>>(),
        "unique_constraints": schema.unique_constraints.iter().map(|c| json!({
            "name": c.name,
            "columns": c.columns,
            "primary_key": c.primary_key,
        })).collect::<Vec<_>>(),
        "foreign_keys": schema.foreign_keys.iter().map(|c| json!({
            "name": c.name,
            "columns": c.columns,
            "foreign_table": c.foreign_table,
            "referred_columns": c.referred_columns,
        })).collect::<Vec<_>>(),
        "columns": schema.columns.iter().map(|c| {
            json!({
                "column_id": c.column_id,
                "name": c.name,
                "data_type": c.data_type.to_str(),
                "primary_key": c.primary_key,
                "nullable": c.nullable,
                "default": c.default.clone(),
                "on_update": c.on_update.clone(),
                "character_set": c.character_set.clone(),
                "collation": c.collation.clone(),
            })
        }).collect::<Vec<_>>(),
    }))
    .map_err(|e| user_error("XX000", format!("schema encode error: {e}")))
}

pub(super) fn decode_table_catalog(bytes: &[u8]) -> PgWireResult<TableCatalog> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| user_error("XX000", format!("table decode error: {e}")))?;
    let table_name = value
        .get("table_name")
        .and_then(Value::as_str)
        .ok_or_else(|| user_error("XX000", "catalog table_name is malformed"))?
        .to_string();
    let table_id = value
        .get("table_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| user_error("XX000", "catalog table_id is malformed"))?
        as u32;
    let primary_key = value
        .get("primary_key")
        .and_then(Value::as_str)
        .ok_or_else(|| user_error("XX000", "catalog primary_key is malformed"))?
        .to_string();
    let columns = value
        .get("columns")
        .and_then(Value::as_array)
        .ok_or_else(|| user_error("XX000", "catalog columns are malformed"))?
        .iter()
        .map(parse_column_schema)
        .collect::<PgWireResult<Vec<ColumnSchema>>>()?;
    let check_constraints = parse_check_constraints(&value)?;
    let unique_constraints = parse_unique_constraints(&value)?;
    let foreign_keys = parse_foreign_keys(&value)?;

    let mut schema = TableSchema {
        table_name: table_name.clone(),
        table_id,
        schema_version: value
            .get("schema_version")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
        table_epoch: value
            .get("table_epoch")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        primary_key,
        check_constraints,
        unique_constraints,
        foreign_keys,
        columns,
    };
    schema.normalize_descriptor();

    Ok(TableCatalog {
        database_id: value
            .get("database_id")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
        schema_id: value.get("schema_id").and_then(Value::as_u64).unwrap_or(1) as u32,
        schema_name: value
            .get("schema_name")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_SCHEMA_NAME)
            .to_string(),
        table_name: table_name.clone(),
        schema,
    })
}

pub(super) fn encode_index_catalog(index: &IndexCatalog) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "index_id": index.index_id,
        "database_id": index.database_id,
        "schema_id": index.schema_id,
        "table_id": index.table_id,
        "schema_name": index.schema_name,
        "table_name": index.table_name,
        "index_name": index.index_name,
        "column_name": index.column_name,
        "column_names": index.column_names,
        "unique": index.unique,
    }))
    .map_err(|e| user_error("XX000", format!("index encode error: {e}")))
}

pub(super) fn decode_index_catalog(bytes: &[u8]) -> PgWireResult<IndexCatalog> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| user_error("XX000", format!("index decode error: {e}")))?;
    Ok(IndexCatalog {
        index_id: value
            .get("index_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog index_id is malformed"))?
            as u32,
        database_id: value
            .get("database_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog database_id is malformed"))?
            as u32,
        schema_id: value
            .get("schema_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog schema_id is malformed"))?
            as u32,
        table_id: value
            .get("table_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog table_id is malformed"))?
            as u32,
        schema_name: value
            .get("schema_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog schema_name is malformed"))?
            .to_string(),
        table_name: value
            .get("table_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog table_name is malformed"))?
            .to_string(),
        index_name: value
            .get("index_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog index_name is malformed"))?
            .to_string(),
        column_name: value
            .get("column_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog column_name is malformed"))?
            .to_string(),
        column_names: value
            .get("column_names")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| {
                        item.as_str()
                            .map(ToString::to_string)
                            .ok_or_else(|| user_error("XX000", "catalog column_names is malformed"))
                    })
                    .collect::<PgWireResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_else(|| {
                value
                    .get("column_name")
                    .and_then(Value::as_str)
                    .map(|name| vec![name.to_string()])
                    .unwrap_or_default()
            }),
        unique: value
            .get("unique")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

pub(super) fn encode_view_catalog(view: &ViewCatalog) -> PgWireResult<Vec<u8>> {
    serde_json::to_vec(&json!({
        "view_id": view.view_id,
        "database_id": view.database_id,
        "schema_id": view.schema_id,
        "schema_name": view.schema_name,
        "view_name": view.view_name,
        "definition": view.definition,
    }))
    .map_err(|e| user_error("XX000", format!("view encode error: {e}")))
}

pub(super) fn decode_view_catalog(bytes: &[u8]) -> PgWireResult<ViewCatalog> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| user_error("XX000", format!("view decode error: {e}")))?;
    Ok(ViewCatalog {
        view_id: value
            .get("view_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog view_id is malformed"))?
            as u32,
        database_id: value
            .get("database_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog database_id is malformed"))?
            as u32,
        schema_id: value
            .get("schema_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| user_error("XX000", "catalog schema_id is malformed"))?
            as u32,
        schema_name: value
            .get("schema_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog schema_name is malformed"))?
            .to_string(),
        view_name: value
            .get("view_name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog view_name is malformed"))?
            .to_string(),
        definition: value
            .get("definition")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "catalog view definition is malformed"))?
            .to_string(),
    })
}

pub(super) fn parse_string_array(value: Option<&Value>, field: &str) -> PgWireResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .ok_or_else(|| user_error("XX000", format!("catalog {field} is malformed")))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| user_error("XX000", format!("catalog {field} is malformed")))
        })
        .collect()
}

pub(super) fn parse_check_constraints(value: &Value) -> PgWireResult<Vec<CheckConstraintSchema>> {
    let Some(items) = value.get("check_constraints").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|item| {
            Ok(CheckConstraintSchema {
                name: item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                expr: item
                    .get("expr")
                    .and_then(Value::as_str)
                    .ok_or_else(|| user_error("XX000", "catalog check constraint is malformed"))?
                    .to_string(),
            })
        })
        .collect()
}

pub(super) fn parse_unique_constraints(value: &Value) -> PgWireResult<Vec<UniqueConstraintSchema>> {
    let Some(items) = value.get("unique_constraints").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|item| {
            Ok(UniqueConstraintSchema {
                name: item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                columns: parse_string_array(item.get("columns"), "unique constraint columns")?,
                primary_key: item
                    .get("primary_key")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect()
}

pub(super) fn parse_foreign_keys(value: &Value) -> PgWireResult<Vec<ForeignKeyConstraintSchema>> {
    let Some(items) = value.get("foreign_keys").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|item| {
            Ok(ForeignKeyConstraintSchema {
                name: item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                columns: parse_string_array(item.get("columns"), "foreign key columns")?,
                foreign_table: item
                    .get("foreign_table")
                    .and_then(Value::as_str)
                    .ok_or_else(|| user_error("XX000", "catalog foreign key is malformed"))?
                    .to_string(),
                referred_columns: parse_string_array(
                    item.get("referred_columns"),
                    "foreign key referred columns",
                )?,
            })
        })
        .collect()
}

pub(super) async fn write_database_inner(
    store: &Arc<dyn KvStore>,
    database: &DatabaseCatalog,
) -> PgWireResult<()> {
    let payload = encode_database_catalog(database)?;
    store
        .put(
            database_by_id_key(database.database_id).as_bytes(),
            &payload,
        )
        .await
        .map_err(|e| user_error("XX000", e))?;
    store
        .put(
            database_by_name_key(&database.database_name).as_bytes(),
            &database.database_id.to_be_bytes(),
        )
        .await
        .map_err(|e| user_error("XX000", e))
}

pub(super) async fn write_schema_inner(
    store: &Arc<dyn KvStore>,
    schema: &SchemaCatalog,
) -> PgWireResult<()> {
    let payload = encode_schema_catalog(schema)?;
    store
        .put(schema_by_id_key(schema.schema_id).as_bytes(), &payload)
        .await
        .map_err(|e| user_error("XX000", e))?;
    store
        .put(
            schema_by_name_key(schema.database_id, &schema.schema_name).as_bytes(),
            &schema.schema_id.to_be_bytes(),
        )
        .await
        .map_err(|e| user_error("XX000", e))
}

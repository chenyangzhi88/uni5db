use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::memory::MemTable;
use pgwire::error::PgWireResult;

use crate::catalog::CatalogStore;
use crate::error::user_error;

use super::super::register_empty_table;

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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    ListBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider, TableProvider,
};
use datafusion::common::Result as DfResult;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr as DfExpr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use pgwire::error::PgWireResult;

use crate::catalog::{CatalogStore, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::codec::SCHEMA_PREFIX;
use crate::error::user_error;
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;
use crate::types::{ColumnValue, DataType, TableSchema};

use super::{
    INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_NAMESPACE_OID, KvTableProvider,
    PG_CATALOG_NAMESPACE_OID, PG_CATALOG_SCHEMA_NAME, POSTGRES_ROLE_OID,
    register_dependency_catalog_tables, register_index_support_catalog_tables,
    register_information_schema_columns, register_information_schema_constraints,
    register_information_schema_empty_views, register_information_schema_privileges,
    register_information_schema_schemata, register_information_schema_tables,
    register_information_schema_views, register_mysql_information_schema,
    register_pg_attribute_table, register_pg_cast_table,
    register_pg_catalog_functions_with_view_defs, register_pg_class_table,
    register_pg_constraint_table, register_pg_index_table, register_pg_indexes_view,
    register_pg_proc_table, register_pg_sequences_view, register_pg_settings_table,
    register_pg_tables_view, register_pg_type_table, register_pg_views_view,
    register_role_catalog_tables, register_sequence_view_rule_policy_catalog_tables,
    register_statistic_catalog_tables, view_relation_oid,
};

#[async_trait]
impl TableProvider for KvTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let output_schema = self.projected_arrow_schema(projection);
        let batches = self
            .load_batches_with_pushdown(projection, filters, limit)
            .await?;
        let mem = MemTable::try_new(output_schema, vec![batches])?;
        mem.scan(state, None, &[], None).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.filter_supported(filter) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

// ── Column builder ───────────────────────────────────────────────────

pub(super) enum ColumnBuilder {
    Int16(Vec<Option<i16>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
    Boolean(Vec<Option<bool>>),
    Binary(Vec<Option<Vec<u8>>>),
    Array(Vec<Option<Vec<Option<String>>>>),
}

impl ColumnBuilder {
    pub(super) fn new(dt: &DataType) -> Self {
        match dt {
            DataType::Int16 => ColumnBuilder::Int16(Vec::new()),
            DataType::Int32 => ColumnBuilder::Int32(Vec::new()),
            DataType::Int64 => ColumnBuilder::Int64(Vec::new()),
            DataType::MySqlInt {
                kind: crate::types::MySqlIntKind::Tiny | crate::types::MySqlIntKind::Small,
                unsigned: false,
            } => ColumnBuilder::Int16(Vec::new()),
            DataType::MySqlInt {
                kind: crate::types::MySqlIntKind::Big,
                unsigned: true,
            } => ColumnBuilder::Text(Vec::new()),
            DataType::MySqlInt {
                kind: crate::types::MySqlIntKind::Int | crate::types::MySqlIntKind::Big,
                ..
            } => ColumnBuilder::Int64(Vec::new()),
            DataType::MySqlInt { .. } | DataType::Bit(_) | DataType::Year => {
                ColumnBuilder::Int32(Vec::new())
            }
            DataType::Float32 => ColumnBuilder::Float32(Vec::new()),
            DataType::Float64 => ColumnBuilder::Float64(Vec::new()),
            DataType::MySqlFloat { .. } => ColumnBuilder::Float32(Vec::new()),
            DataType::MySqlDouble { .. } => ColumnBuilder::Float64(Vec::new()),
            DataType::Bytea
            | DataType::Binary(_)
            | DataType::VarBinary(_)
            | DataType::Blob { .. } => ColumnBuilder::Binary(Vec::new()),
            DataType::Array(_) => ColumnBuilder::Array(Vec::new()),
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
            | DataType::Domain(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_) => ColumnBuilder::Text(Vec::new()),
            DataType::Boolean => ColumnBuilder::Boolean(Vec::new()),
        }
    }

    pub(super) fn push(&mut self, value: &ColumnValue) {
        match self {
            ColumnBuilder::Int16(v) => match value {
                ColumnValue::Int16(n) => v.push(Some(*n)),
                ColumnValue::Int32(n) => v.push(i16::try_from(*n).ok()),
                ColumnValue::Int64(n) => v.push(i16::try_from(*n).ok()),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Int32(v) => match value {
                ColumnValue::Int16(n) => v.push(Some(*n as i32)),
                ColumnValue::Int32(n) => v.push(Some(*n)),
                ColumnValue::Int64(n) => v.push(Some(*n as i32)),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Int64(v) => match value {
                ColumnValue::Int64(n) => v.push(Some(*n)),
                ColumnValue::Int16(n) => v.push(Some(*n as i64)),
                ColumnValue::Int32(n) => v.push(Some(*n as i64)),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Float32(v) => match value {
                ColumnValue::Float32(n) => v.push(Some(*n)),
                ColumnValue::Int16(n) => v.push(Some(*n as f32)),
                ColumnValue::Float64(n) => v.push(Some(*n as f32)),
                ColumnValue::Int32(n) => v.push(Some(*n as f32)),
                ColumnValue::Int64(n) => v.push(Some(*n as f32)),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Float64(v) => match value {
                ColumnValue::Float64(n) => v.push(Some(*n)),
                ColumnValue::Int16(n) => v.push(Some(*n as f64)),
                ColumnValue::Float32(n) => v.push(Some(*n as f64)),
                ColumnValue::Int32(n) => v.push(Some(*n as f64)),
                ColumnValue::Int64(n) => v.push(Some(*n as f64)),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Text(v) => match value {
                ColumnValue::Text(s)
                | ColumnValue::Numeric(s)
                | ColumnValue::Date(s)
                | ColumnValue::Timestamp(s)
                | ColumnValue::TimestampTz(s)
                | ColumnValue::Uuid(s)
                | ColumnValue::Json(s)
                | ColumnValue::Jsonb(s) => v.push(Some(s.clone())),
                ColumnValue::Null => v.push(None),
                _ => v.push(Some(value.to_text().unwrap_or_default())),
            },
            ColumnBuilder::Boolean(v) => match value {
                ColumnValue::Boolean(b) => v.push(Some(*b)),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Binary(v) => match value {
                ColumnValue::Bytea(bytes) => v.push(Some(bytes.clone())),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
            ColumnBuilder::Array(v) => match value {
                ColumnValue::Array(values) => v.push(Some(
                    values
                        .iter()
                        .map(ColumnValue::to_text)
                        .collect::<Vec<Option<String>>>(),
                )),
                ColumnValue::Null => v.push(None),
                _ => v.push(None),
            },
        }
    }

    pub(super) fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int16(v) => Arc::new(arrow::array::Int16Array::from(v)),
            ColumnBuilder::Int32(v) => Arc::new(Int32Array::from(v)),
            ColumnBuilder::Int64(v) => Arc::new(Int64Array::from(v)),
            ColumnBuilder::Float32(v) => Arc::new(Float32Array::from(v)),
            ColumnBuilder::Float64(v) => Arc::new(Float64Array::from(v)),
            ColumnBuilder::Text(v) => Arc::new(StringArray::from(v)),
            ColumnBuilder::Boolean(v) => Arc::new(BooleanArray::from(v)),
            ColumnBuilder::Binary(v) => Arc::new(BinaryArray::from_opt_vec(
                v.iter().map(|value| value.as_deref()).collect::<Vec<_>>(),
            )),
            ColumnBuilder::Array(v) => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for row in v {
                    match row {
                        Some(values) => {
                            for value in values {
                                match value {
                                    Some(value) => builder.values().append_value(value),
                                    None => builder.values().append_null(),
                                }
                            }
                            builder.append(true);
                        }
                        None => builder.append(false),
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}

// ── Register all tables ──────────────────────────────────────────────

pub async fn register_all_tables(
    ctx: &SessionContext,
    store: Arc<dyn KvStore>,
    load_schema_fn: impl Fn(&str, &[u8]) -> PgWireResult<TableSchema>,
) -> PgWireResult<()> {
    let prefix = SCHEMA_PREFIX.as_bytes();
    let entries = store
        .scan_prefix(prefix)
        .await
        .map_err(|e| user_error("XX000", e))?;

    for (key, value) in entries {
        let key_str = String::from_utf8(key)
            .map_err(|e| user_error("XX000", format!("invalid schema key: {e}")))?;
        let table_name = key_str
            .strip_prefix(SCHEMA_PREFIX)
            .ok_or_else(|| user_error("XX000", "schema key missing prefix"))?;

        let schema = load_schema_fn(table_name, &value)?;
        let provider = KvTableProvider::new(
            DEFAULT_DATABASE_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            schema,
            store.clone(),
        );
        ctx.register_table(table_name, Arc::new(provider))
            .map_err(|e| user_error("XX000", format!("failed to register table: {e}")))?;
    }
    Ok(())
}

pub async fn register_catalog_tables(
    ctx: &SessionContext,
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    database_name: &str,
) -> PgWireResult<()> {
    register_catalog_tables_with_options(ctx, store, catalog, database_name, true).await
}

pub async fn register_catalog_tables_with_options(
    ctx: &SessionContext,
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    database_name: &str,
    include_system_catalogs: bool,
) -> PgWireResult<()> {
    register_catalog_tables_with_options_for_mode(
        ctx,
        store,
        catalog,
        database_name,
        include_system_catalogs,
        GatewayMode::Postgres,
    )
    .await
}

pub async fn register_catalog_tables_with_options_for_mode(
    ctx: &SessionContext,
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    database_name: &str,
    include_system_catalogs: bool,
    mode: GatewayMode,
) -> PgWireResult<()> {
    let view_defs = catalog
        .list_views(database_name)
        .await?
        .into_iter()
        .map(|view| (view_relation_oid(&view), view.definition))
        .collect::<Vec<_>>();
    register_pg_catalog_functions_with_view_defs(ctx, view_defs);

    let catalog_provider =
        build_user_catalog_provider(store.clone(), catalog, database_name).await?;

    if !include_system_catalogs {
        ctx.register_catalog(database_name, catalog_provider);
        return Ok(());
    }

    let pg_catalog_provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    register_pg_database_table(catalog, &pg_catalog_provider).await?;
    register_pg_namespace_table(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_type_table(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_class_table(store.clone(), catalog, &pg_catalog_provider, database_name).await?;
    register_pg_attribute_table(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_index_table(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_constraint_table(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_proc_table(&pg_catalog_provider).await?;
    register_pg_cast_table(&pg_catalog_provider).await?;
    register_pg_settings_table(&pg_catalog_provider).await?;
    register_role_catalog_tables(&pg_catalog_provider).await?;
    register_dependency_catalog_tables(&pg_catalog_provider).await?;
    register_index_support_catalog_tables(&pg_catalog_provider).await?;
    register_sequence_view_rule_policy_catalog_tables(&pg_catalog_provider).await?;
    register_statistic_catalog_tables(store.clone(), catalog, &pg_catalog_provider, database_name)
        .await?;
    register_pg_tables_view(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_indexes_view(catalog, &pg_catalog_provider, database_name).await?;
    register_pg_sequences_view(&pg_catalog_provider).await?;
    register_pg_views_view(catalog, &pg_catalog_provider, database_name).await?;
    catalog_provider
        .register_schema(PG_CATALOG_SCHEMA_NAME, pg_catalog_provider)
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register pg_catalog schema: {e}"),
            )
        })?;

    let information_schema_provider: Arc<dyn SchemaProvider> =
        Arc::new(MemorySchemaProvider::new());
    match mode {
        GatewayMode::Postgres => {
            register_information_schema_tables(
                catalog,
                &information_schema_provider,
                database_name,
            )
            .await?;
            register_information_schema_columns(
                catalog,
                &information_schema_provider,
                database_name,
            )
            .await?;
            register_information_schema_schemata(
                catalog,
                &information_schema_provider,
                database_name,
            )
            .await?;
            register_information_schema_constraints(
                catalog,
                &information_schema_provider,
                database_name,
            )
            .await?;
            register_information_schema_privileges(
                catalog,
                &information_schema_provider,
                database_name,
            )
            .await?;
            register_information_schema_views(catalog, &information_schema_provider, database_name)
                .await?;
            register_information_schema_empty_views(&information_schema_provider).await?;
        }
        GatewayMode::MySql => {
            register_mysql_information_schema(catalog, &information_schema_provider, database_name)
                .await?;
        }
    }
    catalog_provider
        .register_schema(INFORMATION_SCHEMA_NAME, information_schema_provider)
        .map_err(|e| {
            user_error(
                "XX000",
                format!("failed to register information_schema schema: {e}"),
            )
        })?;

    ctx.register_catalog(database_name, catalog_provider);
    Ok(())
}

pub async fn build_user_catalog_provider(
    store: Arc<dyn KvStore>,
    catalog: &CatalogStore,
    database_name: &str,
) -> PgWireResult<Arc<dyn CatalogProvider>> {
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let tables = catalog.list_tables(database_name).await?;

    for table in tables {
        let schema_provider = catalog_provider
            .schema(&table.schema_name)
            .unwrap_or_else(|| {
                let provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
                catalog_provider
                    .register_schema(&table.schema_name, provider.clone())
                    .expect("memory catalog can register schema");
                provider
            });

        schema_provider
            .register_table(
                table.table_name.clone(),
                Arc::new(KvTableProvider::new(
                    database_name.to_string(),
                    table.schema_name.clone(),
                    table.schema,
                    store.clone(),
                )),
            )
            .map_err(|e| user_error("XX000", format!("failed to register table: {e}")))?;
    }

    Ok(catalog_provider as Arc<dyn CatalogProvider>)
}

async fn register_pg_database_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
) -> PgWireResult<()> {
    let databases = catalog.list_databases().await?;
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("datname", ArrowDataType::Utf8, false),
        Field::new("datdba", ArrowDataType::Int32, false),
        Field::new("encoding", ArrowDataType::Int32, false),
        Field::new("datlocprovider", ArrowDataType::Utf8, false),
        Field::new("datistemplate", ArrowDataType::Boolean, false),
        Field::new("datallowconn", ArrowDataType::Boolean, false),
        Field::new("datcollate", ArrowDataType::Utf8, false),
        Field::new("datctype", ArrowDataType::Utf8, false),
        Field::new("daticulocale", ArrowDataType::Utf8, true),
        Field::new("datacl", ArrowDataType::Utf8, true),
    ]));
    let oids = Int32Array::from(
        databases
            .iter()
            .map(|db| db.database_id as i32)
            .collect::<Vec<_>>(),
    );
    let names = StringArray::from(
        databases
            .iter()
            .map(|db| db.database_name.clone())
            .collect::<Vec<_>>(),
    );
    let owners = Int32Array::from(vec![10; databases.len()]);
    let encodings = Int32Array::from(vec![6; databases.len()]);
    let providers = StringArray::from(vec!["c"; databases.len()]);
    let is_template = BooleanArray::from(vec![false; databases.len()]);
    let allow_conn = BooleanArray::from(vec![true; databases.len()]);
    let collate = StringArray::from(vec!["C.UTF-8"; databases.len()]);
    let ctype = StringArray::from(vec!["C.UTF-8"; databases.len()]);
    let icu_locale = StringArray::from(vec![Option::<String>::None; databases.len()]);
    let acl = StringArray::from(vec![Option::<String>::None; databases.len()]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(oids),
            Arc::new(names),
            Arc::new(owners),
            Arc::new(encodings),
            Arc::new(providers),
            Arc::new(is_template),
            Arc::new(allow_conn),
            Arc::new(collate),
            Arc::new(ctype),
            Arc::new(icu_locale),
            Arc::new(acl),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_database batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_database table: {e}")))?;
    schema_provider
        .register_table("pg_database".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_database: {e}")))?;
    Ok(())
}

async fn register_pg_namespace_table(
    catalog: &CatalogStore,
    schema_provider: &Arc<dyn SchemaProvider>,
    database_name: &str,
) -> PgWireResult<()> {
    let schemas = catalog.list_schemas(database_name).await?;
    let mut oids = vec![PG_CATALOG_NAMESPACE_OID, INFORMATION_SCHEMA_NAMESPACE_OID];
    let mut names = vec![
        PG_CATALOG_SCHEMA_NAME.to_string(),
        INFORMATION_SCHEMA_NAME.to_string(),
    ];
    for schema in schemas {
        oids.push(schema.schema_id as i32);
        names.push(schema.schema_name);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("oid", ArrowDataType::Int32, false),
        Field::new("nspname", ArrowDataType::Utf8, false),
        Field::new("nspowner", ArrowDataType::Int32, false),
        Field::new("nspacl", ArrowDataType::Utf8, true),
    ]));
    let owners = vec![POSTGRES_ROLE_OID; oids.len()];
    let acl = vec![Option::<String>::None; oids.len()];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(owners)),
            Arc::new(StringArray::from(acl)),
        ],
    )
    .map_err(|e| user_error("XX000", format!("failed to build pg_namespace batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| user_error("XX000", format!("failed to build pg_namespace table: {e}")))?;
    schema_provider
        .register_table("pg_namespace".to_string(), Arc::new(table))
        .map_err(|e| user_error("XX000", format!("failed to register pg_namespace: {e}")))?;
    Ok(())
}

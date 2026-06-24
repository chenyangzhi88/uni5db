use std::sync::Arc;

use crate::mem_store::KvStore;
use crate::types::TableSchema;

pub const DEFAULT_DATABASE_NAME: &str = "defaultdb";
pub const DEFAULT_SCHEMA_NAME: &str = "public";

pub(super) const NEXT_DATABASE_ID_KEY: &[u8] = b"__catalog__/next_database_id";
pub(super) const NEXT_SCHEMA_ID_KEY: &[u8] = b"__catalog__/next_schema_id";
pub(super) const NEXT_TABLE_ID_KEY: &[u8] = b"__catalog__/next_table_id";
pub(super) const NEXT_INDEX_ID_KEY: &[u8] = b"__catalog__/next_index_id";
pub(super) const NEXT_VIEW_ID_KEY: &[u8] = b"__catalog__/next_view_id";
pub(super) const COMPAT_MODE_KEY: &[u8] = b"__catalog__/compat_mode";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseCatalog {
    pub database_id: u32,
    pub database_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaCatalog {
    pub schema_id: u32,
    pub database_id: u32,
    pub schema_name: String,
}

#[derive(Clone, Debug)]
pub struct TableCatalog {
    pub database_id: u32,
    pub schema_id: u32,
    pub schema_name: String,
    pub table_name: String,
    pub schema: TableSchema,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexCatalog {
    pub index_id: u32,
    pub database_id: u32,
    pub schema_id: u32,
    pub table_id: u32,
    pub schema_name: String,
    pub table_name: String,
    pub index_name: String,
    pub column_name: String,
    pub column_names: Vec<String>,
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewCatalog {
    pub view_id: u32,
    pub database_id: u32,
    pub schema_id: u32,
    pub schema_name: String,
    pub view_name: String,
    pub definition: String,
}

pub struct CatalogStore {
    pub(super) store: Arc<dyn KvStore>,
}

mod types;
pub use types::{
    CatalogStore, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DatabaseCatalog, IndexCatalog,
    SchemaCatalog, TableCatalog, ViewCatalog,
};
mod codec;
pub use codec::{
    decode_table_schema, object_name_to_string, resolve_table_reference, schema_name_to_string,
};
mod legacy;
mod store;
#[cfg(test)]
mod tests;

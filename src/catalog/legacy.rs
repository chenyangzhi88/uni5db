use pgwire::error::PgWireResult;

use super::codec::{
    encode_index_catalog, encode_view_catalog, index_by_id_key, index_by_name_key,
    index_by_table_key, view_by_id_key, view_by_name_key, write_database_inner, write_schema_inner,
};
use super::types::{CatalogStore, DatabaseCatalog, IndexCatalog, SchemaCatalog, ViewCatalog};
use crate::error::user_error;

impl CatalogStore {
    pub(super) async fn allocate_id(&self, key: &[u8]) -> PgWireResult<u32> {
        let current = self
            .store
            .get(key)
            .await
            .map_err(|e| user_error("XX000", e))?;
        let id = match current {
            Some(bytes) if bytes.len() == 4 => u32::from_be_bytes(bytes.try_into().unwrap()),
            _ => 1,
        };
        self.store
            .put(key, &(id + 1).to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        Ok(id)
    }

    pub(super) async fn write_database(&self, database: &DatabaseCatalog) -> PgWireResult<()> {
        write_database_inner(&self.store, database).await
    }

    pub(super) async fn write_schema(&self, schema: &SchemaCatalog) -> PgWireResult<()> {
        write_schema_inner(&self.store, schema).await
    }

    pub(super) async fn write_index(&self, index: &IndexCatalog) -> PgWireResult<()> {
        let payload = encode_index_catalog(index)?;
        self.store
            .put(index_by_id_key(index.index_id).as_bytes(), &payload)
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(
                index_by_name_key(index.database_id, index.schema_id, &index.index_name).as_bytes(),
                &index.index_id.to_be_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(
                index_by_table_key(index.table_id, index.index_id).as_bytes(),
                &index.index_id.to_be_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))
    }

    pub(super) async fn write_view(&self, view: &ViewCatalog) -> PgWireResult<()> {
        let payload = encode_view_catalog(view)?;
        self.store
            .put(view_by_id_key(view.view_id).as_bytes(), &payload)
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .put(
                view_by_name_key(view.database_id, view.schema_id, &view.view_name).as_bytes(),
                &view.view_id.to_be_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))
    }

    pub(super) async fn drop_index_by_id(&self, index: &IndexCatalog) -> PgWireResult<()> {
        self.store
            .delete(index_by_id_key(index.index_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(
                index_by_name_key(index.database_id, index.schema_id, &index.index_name).as_bytes(),
            )
            .await
            .map_err(|e| user_error("XX000", e))?;
        self.store
            .delete(index_by_table_key(index.table_id, index.index_id).as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))
    }
}

pub(super) fn decode_u32(bytes: &[u8], field: &str) -> PgWireResult<u32> {
    if bytes.len() != 4 {
        return Err(user_error("XX000", format!("catalog {field} is malformed")));
    }
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

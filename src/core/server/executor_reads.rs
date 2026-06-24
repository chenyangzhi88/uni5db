use pgwire::error::PgWireResult;

use super::GatewayServer;
use crate::codec::{cell_key, decode_cell_value, row_marker_key};
use crate::error::user_error;
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableSchema};

impl GatewayServer {
    pub(super) async fn read_visible_row_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
    ) -> PgWireResult<Option<RowMap>> {
        let v2_key = storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value);
        let consistent_read = self.use_consistent_read(session_id).await;
        let has_active_txn = if let Some(session_id) = session_id {
            self.active_transactions
                .lock()
                .await
                .contains_key(&session_id)
        } else {
            false
        };
        if has_active_txn {
            if self
                .txn_has_pending_key(session_id, &v2_key)
                .await
                .map_err(|e| user_error("XX000", e))?
            {
                if let Some(bytes) = self
                    .txn_get(session_id, &v2_key)
                    .await
                    .map_err(|e| user_error("XX000", e))?
                {
                    return Ok(Some(storage_layout::decode_row_record(schema, &bytes)?));
                }
                return Ok(None);
            }
        }

        let v2_value = if consistent_read {
            self.txn_snapshot_get(session_id, &v2_key).await
        } else {
            self.txn_get(session_id, &v2_key).await
        }
        .map_err(|e| user_error("XX000", e))?;
        if let Some(bytes) = v2_value {
            return Ok(Some(storage_layout::decode_row_record(schema, &bytes)?));
        }

        let marker = row_marker_key(database_name, schema_name, &schema.table_name, pk_value);
        let exists = if consistent_read {
            self.txn_snapshot_get(session_id, &marker).await
        } else {
            self.txn_get(session_id, &marker).await
        }
        .map_err(|e| user_error("XX000", e))?;
        if exists.is_none() {
            return Ok(None);
        }

        let mut row = RowMap::new();
        for column in &schema.columns {
            let key = cell_key(
                database_name,
                schema_name,
                &schema.table_name,
                &column.name,
                pk_value,
            );
            let value = if consistent_read {
                self.txn_snapshot_get(session_id, &key).await
            } else {
                self.txn_get(session_id, &key).await
            }
            .map_err(|e| user_error("XX000", e))?;
            let decoded = match value {
                Some(bytes) => decode_cell_value(&column.data_type, &bytes)?,
                None => ColumnValue::Null,
            };
            row.insert(column.name.clone(), decoded);
        }
        Ok(Some(row))
    }
}

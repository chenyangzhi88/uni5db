use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::OLAP_CHUNK_ROW_TARGET;
use crate::error::user_error;
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableSchema};

impl GatewayServer {
    pub(super) fn zone_maps_for_rows(
        schema: &TableSchema,
        rows: &[RowMap],
    ) -> Vec<storage_layout::ColumnZoneMap> {
        let mut zones = schema
            .columns
            .iter()
            .map(|column| storage_layout::ColumnZoneMap {
                column_id: column.column_id,
                null_count: 0,
                min: None,
                max: None,
            })
            .collect::<Vec<_>>();
        for row in rows {
            for (idx, column) in schema.columns.iter().enumerate() {
                let value = row.get(&column.name).cloned().unwrap_or(ColumnValue::Null);
                Self::update_zone(&mut zones[idx], &value);
            }
        }
        zones
    }

    pub(super) async fn refresh_olap_storage_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
    ) -> PgWireResult<()> {
        let rows = self
            .scan_visible_row_entries_at(session_id, database_name, schema_name, schema, None)
            .await?
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();

        for prefix in [
            storage_layout::olap_chunk_meta_prefix(schema.table_id, schema.table_epoch),
            storage_layout::olap_chunk_column_prefix(schema.table_id, schema.table_epoch),
            storage_layout::stats_prefix(schema.table_id, schema.table_epoch),
        ] {
            for (key, _) in self
                .txn_scan_prefix(session_id, &prefix)
                .await
                .map_err(|e| user_error("XX000", e))?
            {
                self.txn_delete(session_id, &key)
                    .await
                    .map_err(|e| user_error("XX000", e))?;
            }
        }

        let mut entries = Vec::new();
        for (chunk_idx, chunk_rows) in rows.chunks(OLAP_CHUNK_ROW_TARGET).enumerate() {
            let chunk_id = chunk_idx as u64 + 1;
            let zones = Self::zone_maps_for_rows(schema, chunk_rows);
            let meta = storage_layout::OlapChunkMeta {
                chunk_id,
                row_count: chunk_rows.len() as u32,
                zones,
            };
            entries.push((
                storage_layout::olap_chunk_meta_key(schema.table_id, schema.table_epoch, chunk_id),
                storage_layout::encode_olap_chunk_meta(&meta),
            ));
            for column in &schema.columns {
                let values = chunk_rows
                    .iter()
                    .map(|row| row.get(&column.name).cloned().unwrap_or(ColumnValue::Null))
                    .collect::<Vec<_>>();
                entries.push((
                    storage_layout::olap_chunk_column_key(
                        schema.table_id,
                        schema.table_epoch,
                        chunk_id,
                        column.column_id,
                    ),
                    storage_layout::encode_olap_column_chunk(&values),
                ));
            }
        }

        let stats = storage_layout::TableStats {
            row_count: rows.len() as u64,
            zones: Self::zone_maps_for_rows(schema, &rows),
        };
        entries.push((
            storage_layout::stats_key(schema.table_id, schema.table_epoch, None),
            storage_layout::encode_table_stats(&stats),
        ));
        for zone in &stats.zones {
            entries.push((
                storage_layout::stats_key(
                    schema.table_id,
                    schema.table_epoch,
                    Some(zone.column_id),
                ),
                storage_layout::encode_table_stats(&storage_layout::TableStats {
                    row_count: rows.len() as u64,
                    zones: vec![zone.clone()],
                }),
            ));
        }

        if !entries.is_empty() {
            self.txn_put_batch(session_id, entries)
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }
}

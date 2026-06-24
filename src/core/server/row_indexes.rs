use std::collections::BTreeSet;

use pgwire::error::PgWireResult;

use super::GatewayServer;
use crate::catalog::IndexCatalog;
use crate::codec::{index_entry_key, index_entry_prefix, index_table_prefix};
use crate::error::user_error;
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableSchema};

impl GatewayServer {
    pub(super) fn indexed_value(row: &RowMap, index: &IndexCatalog) -> ColumnValue {
        Self::indexed_value_for_columns(row, &index.column_names)
    }

    pub(super) fn indexed_value_for_columns(row: &RowMap, column_names: &[String]) -> ColumnValue {
        if column_names.len() == 1 {
            return row
                .get(&column_names[0])
                .cloned()
                .unwrap_or(ColumnValue::Null);
        }
        ColumnValue::Array(
            column_names
                .iter()
                .map(|column_name| row.get(column_name).cloned().unwrap_or(ColumnValue::Null))
                .collect(),
        )
    }

    pub(super) fn index_value_has_null(value: &ColumnValue) -> bool {
        match value {
            ColumnValue::Null => true,
            ColumnValue::Array(values) => values.iter().any(ColumnValue::is_null),
            _ => false,
        }
    }

    pub(super) async fn write_index_entries_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            let index_value = Self::indexed_value(row, &index);
            let v2_key = storage_layout::index_entry_key(index.index_id, &index_value, pk_value);
            self.txn_put(session_id, &v2_key, &[1])
                .await
                .map_err(|e| user_error("XX000", e))?;
            let key = index_entry_key(
                database_name,
                schema_name,
                &schema.table_name,
                &index.index_name,
                &index_value,
                pk_value,
            );
            self.txn_put(session_id, &key, &[1])
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn validate_index_entries_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            let index_value = Self::indexed_value(row, &index);
            if !index.unique || index_value.is_null() {
                continue;
            }
            let v2_key = storage_layout::index_entry_key(index.index_id, &index_value, pk_value);
            let v2_prefix = storage_layout::index_prefix(index.index_id, &index_value);
            let v2_range = storage_layout::RangeScan {
                end: {
                    let mut upper = v2_prefix.clone();
                    for idx in (0..upper.len()).rev() {
                        if upper[idx] != u8::MAX {
                            upper[idx] += 1;
                            upper.truncate(idx + 1);
                            break;
                        }
                    }
                    Some(upper)
                },
                start: v2_prefix,
                limit: None,
                reverse: false,
            };
            for (existing_key, _) in self
                .txn_scan_range(session_id, &v2_range)
                .await
                .map_err(|e| user_error("XX000", e))?
            {
                if existing_key != v2_key {
                    return Err(user_error(
                        "23505",
                        format!(
                            "duplicate key value violates unique constraint '{}'",
                            index.index_name
                        ),
                    ));
                }
            }
            let key = index_entry_key(
                database_name,
                schema_name,
                &schema.table_name,
                &index.index_name,
                &index_value,
                pk_value,
            );
            let prefix = index_entry_prefix(
                database_name,
                schema_name,
                &schema.table_name,
                &index.index_name,
                &index_value,
            );
            for (existing_key, _) in self
                .txn_scan_prefix(session_id, &prefix)
                .await
                .map_err(|e| user_error("XX000", e))?
            {
                if existing_key != key {
                    return Err(user_error(
                        "23505",
                        format!(
                            "duplicate key value violates unique constraint '{}'",
                            index.index_name
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn delete_index_entries_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_value: &ColumnValue,
        row: &RowMap,
    ) -> PgWireResult<()> {
        for index in self.catalog.list_indexes_for_table(schema.table_id).await? {
            let index_value = Self::indexed_value(row, &index);
            let v2_key = storage_layout::index_entry_key(index.index_id, &index_value, pk_value);
            self.txn_delete(session_id, &v2_key)
                .await
                .map_err(|e| user_error("XX000", e))?;
            let key = index_entry_key(
                database_name,
                schema_name,
                &schema.table_name,
                &index.index_name,
                &index_value,
                pk_value,
            );
            self.txn_delete(session_id, &key)
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn delete_index_data_for_table(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> PgWireResult<()> {
        if let Some(table) = self
            .catalog
            .load_table(database_name, schema_name, table_name)
            .await?
        {
            for index in self
                .catalog
                .list_indexes_for_table(table.schema.table_id)
                .await?
            {
                let prefix = storage_layout::index_all_prefix(index.index_id);
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
        }
        let prefix = index_table_prefix(database_name, schema_name, table_name);
        for (key, _) in self
            .txn_scan_prefix(session_id, &prefix)
            .await
            .map_err(|e| user_error("XX000", e))?
        {
            self.txn_delete(session_id, &key)
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn create_index_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
        column_names: &[String],
        unique: bool,
        if_not_exists: bool,
    ) -> PgWireResult<()> {
        let table = self
            .catalog
            .load_table(database_name, schema_name, table_name)
            .await?
            .ok_or_else(|| user_error("42P01", format!("table '{table_name}' does not exist")))?;
        let schema = table.schema;
        for column_name in column_names {
            schema
                .find_column(column_name)
                .ok_or_else(|| user_error("42703", format!("column '{column_name}' not found")))?;
        }
        if self
            .catalog
            .get_index(table.database_id, table.schema_id, index_name)
            .await?
            .is_some()
        {
            if if_not_exists {
                return Ok(());
            }
            return Err(user_error(
                "42P07",
                format!("relation '{index_name}' already exists"),
            ));
        }

        let rows = self
            .scan_visible_row_entries_at(session_id, database_name, schema_name, &schema, None)
            .await?;
        let mut seen_unique_values = BTreeSet::new();
        for (_, row) in &rows {
            let index_value = Self::indexed_value_for_columns(row, column_names);
            if unique && !Self::index_value_has_null(&index_value) {
                let prefix = index_entry_prefix(
                    database_name,
                    schema_name,
                    table_name,
                    index_name,
                    &index_value,
                );
                if !seen_unique_values.insert(prefix) {
                    return Err(user_error(
                        "23505",
                        format!(
                            "could not create unique index '{index_name}': duplicate values exist"
                        ),
                    ));
                }
            }
        }

        let Some(index) = self
            .catalog
            .create_index(
                database_name,
                schema_name,
                table_name,
                index_name,
                column_names,
                unique,
                if_not_exists,
            )
            .await?
        else {
            return Ok(());
        };

        let mut entries = Vec::with_capacity(rows.len() * 2);
        for (pk_value, row) in &rows {
            let index_value = Self::indexed_value_for_columns(row, column_names);
            entries.push((
                storage_layout::index_entry_key(index.index_id, &index_value, pk_value),
                vec![1],
            ));
            entries.push((
                index_entry_key(
                    database_name,
                    schema_name,
                    table_name,
                    index_name,
                    &index_value,
                    pk_value,
                ),
                vec![1],
            ));
        }

        if let Err(err) = self
            .txn_put_batch(session_id, entries.clone())
            .await
            .map_err(|e| user_error("XX000", e))
        {
            let _ = self
                .catalog
                .drop_index(index.database_id, index.schema_id, &index.index_name)
                .await;
            return Err(err);
        }
        Ok(())
    }
}

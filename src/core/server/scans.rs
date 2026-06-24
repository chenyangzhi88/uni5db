use std::sync::{Arc, Mutex};

use pgwire::error::PgWireResult;
use sqlparser::ast::Expr;

use super::GatewayServer;
use super::shared::{
    FILTERED_LIMIT_SCAN_BATCH_SIZE, filtered_limit_scan_batch_size, next_key_prefix,
};
use crate::codec::{
    decode_pk_from_index_entry_key, decode_pk_from_row_marker_key, index_entry_prefix,
    row_marker_key, row_marker_prefix,
};
use crate::error::user_error;
use crate::filter::row_matches_filter;
use crate::mem_store::{KvRangeVisitor, KvScanProjection};
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableSchema};

impl GatewayServer {
    pub(super) async fn ensure_row_write_not_stale(
        &self,
        session_id: Option<i32>,
        schema: &TableSchema,
        pk_value: &ColumnValue,
    ) -> PgWireResult<()> {
        if !self.is_repeatable_read_transaction(session_id).await {
            return Ok(());
        }
        if self
            .txn_has_pending_key(
                session_id,
                &storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value),
            )
            .await
            .map_err(|e| user_error("XX000", e))?
        {
            return Ok(());
        }
        let key = storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value);
        let snapshot = self
            .txn_snapshot_get(session_id, &key)
            .await
            .map_err(|e| user_error("XX000", e))?;
        let latest = self
            .store
            .get(&key)
            .await
            .map_err(|e| user_error("XX000", e))?;
        if snapshot != latest {
            return Err(user_error(
                "40001",
                "could not serialize access due to concurrent update",
            ));
        }
        Ok(())
    }

    pub(super) async fn read_visible_rows_by_pk_in_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        pk_values: &[ColumnValue],
    ) -> PgWireResult<Vec<RowMap>> {
        if pk_values.is_empty() {
            return Ok(Vec::new());
        }

        let has_active_txn = if let Some(session_id) = session_id {
            self.active_transactions
                .lock()
                .await
                .contains_key(&session_id)
        } else {
            false
        };
        if has_active_txn {
            let mut rows = Vec::new();
            for pk_value in pk_values {
                if let Some(row) = self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, pk_value)
                    .await?
                {
                    rows.push(row);
                }
            }
            return Ok(rows);
        }

        let keys = pk_values
            .iter()
            .map(|pk_value| storage_layout::row_key(schema.table_id, schema.table_epoch, pk_value))
            .collect::<Vec<_>>();
        let values = self
            .store
            .multi_get(keys)
            .await
            .map_err(|e| user_error("XX000", e))?;
        let mut rows = Vec::new();
        for value in values.into_iter().flatten() {
            rows.push(storage_layout::decode_row_record(schema, &value)?);
        }
        Ok(rows)
    }

    pub(super) async fn scan_visible_rows_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<RowMap>> {
        Ok(self
            .scan_visible_row_entries_at(session_id, database_name, schema_name, schema, filter)
            .await?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    pub(super) async fn scan_visible_row_entries_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        let range = storage_layout::row_range(schema.table_id, schema.table_epoch, None);
        let consistent_read = self.use_consistent_read(session_id).await;
        let v2_entries = if consistent_read {
            self.txn_snapshot_scan_range(session_id, &range).await
        } else {
            self.txn_scan_range(session_id, &range).await
        }
        .map_err(|e| user_error("XX000", e))?;
        if !v2_entries.is_empty() {
            let mut rows = Vec::with_capacity(v2_entries.len());
            for (key, value) in v2_entries {
                let pk_value = storage_layout::decode_pk_from_row_key(
                    &key,
                    schema.table_id,
                    schema.table_epoch,
                    schema.pk_data_type(),
                )?;
                let row = storage_layout::decode_row_record(schema, &value)?;
                if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                    rows.push((pk_value, row));
                }
            }
            return Ok(rows);
        }

        let prefix = row_marker_prefix(database_name, schema_name, &schema.table_name);
        let entries = if consistent_read {
            self.txn_snapshot_scan_prefix(session_id, &prefix).await
        } else {
            self.txn_scan_prefix(session_id, &prefix).await
        }
        .map_err(|e| user_error("XX000", e))?;

        let mut rows = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            let pk_value = decode_pk_from_row_marker_key(
                &key,
                database_name,
                schema_name,
                &schema.table_name,
                schema.pk_data_type(),
            )?;
            let Some(row) = self
                .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                .await?
            else {
                continue;
            };
            if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                rows.push((pk_value, row));
            }
        }
        Ok(rows)
    }

    pub(super) async fn scan_visible_row_entries_by_pk_range_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
        filter: Option<&Expr>,
        scan_limit: Option<usize>,
        row_offset: usize,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        if filter.is_some() && scan_limit.is_some() {
            let (rows, saw_v2_entries) = self
                .scan_v2_row_entries_by_pk_range_batched_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    lower,
                    upper,
                    filter,
                    scan_limit,
                    row_offset,
                )
                .await?;
            if saw_v2_entries {
                return Ok(rows);
            }
            return self
                .scan_legacy_row_marker_entries_by_pk_range_batched_at(
                    session_id,
                    database_name,
                    schema_name,
                    schema,
                    lower,
                    upper,
                    filter,
                    scan_limit,
                    row_offset,
                )
                .await;
        }

        let storage_scan_limit = filter
            .is_none()
            .then(|| scan_limit.map(|limit| limit.saturating_add(row_offset)))
            .flatten();
        let range = storage_layout::row_range_between(
            schema.table_id,
            schema.table_epoch,
            lower,
            upper,
            storage_scan_limit,
        );
        let consistent_read = self.use_consistent_read(session_id).await;
        let v2_entries = if consistent_read {
            self.txn_snapshot_scan_range(session_id, &range).await
        } else {
            self.txn_scan_range(session_id, &range).await
        }
        .map_err(|e| user_error("XX000", e))?;
        if !v2_entries.is_empty() {
            let mut rows = Vec::with_capacity(scan_limit.unwrap_or(v2_entries.len()));
            let mut remaining_offset = row_offset;
            for (key, _) in v2_entries {
                let pk_value = storage_layout::decode_pk_from_row_key(
                    &key,
                    schema.table_id,
                    schema.table_epoch,
                    schema.pk_data_type(),
                )?;
                let Some(row) = self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                    .await?
                else {
                    continue;
                };
                if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                    if remaining_offset > 0 {
                        remaining_offset -= 1;
                        continue;
                    }
                    rows.push((pk_value, row));
                    if scan_limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
            }
            return Ok(rows);
        }

        self.scan_legacy_row_marker_entries_by_pk_range_at(
            session_id,
            database_name,
            schema_name,
            schema,
            lower,
            upper,
            filter,
            scan_limit,
            row_offset,
        )
        .await
    }

    pub(super) async fn scan_v2_row_entries_by_pk_range_batched_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
        filter: Option<&Expr>,
        scan_limit: Option<usize>,
        row_offset: usize,
    ) -> PgWireResult<(Vec<(ColumnValue, RowMap)>, bool)> {
        let mut rows = Vec::with_capacity(scan_limit.unwrap_or(FILTERED_LIMIT_SCAN_BATCH_SIZE));
        let mut remaining_offset = row_offset;
        let mut current_lower = lower.map(|(value, inclusive)| (value.clone(), inclusive));
        let mut saw_entries = false;
        let batch_size = filtered_limit_scan_batch_size(scan_limit, row_offset);
        let consistent_read = self.use_consistent_read(session_id).await;
        let has_active_txn = if let Some(session_id) = session_id {
            self.active_transactions
                .lock()
                .await
                .contains_key(&session_id)
        } else {
            false
        };

        if !has_active_txn {
            struct FilteredLimitVisitorState {
                rows: Vec<(ColumnValue, RowMap)>,
                remaining_offset: usize,
                saw_entries: bool,
            }

            let state = Arc::new(Mutex::new(FilteredLimitVisitorState {
                rows,
                remaining_offset,
                saw_entries: false,
            }));
            let visitor_state = Arc::clone(&state);
            let schema_for_visit = schema.clone();
            let filter_for_visit = filter.cloned();
            let range = storage_layout::row_range_between(
                schema.table_id,
                schema.table_epoch,
                lower,
                upper,
                None,
            );
            let visitor: KvRangeVisitor =
                Arc::new(Mutex::new(move |key: &[u8], value: Option<&[u8]>| {
                    let mut state = visitor_state.lock().map_err(|e| e.to_string())?;
                    state.saw_entries = true;
                    let pk_value = storage_layout::decode_pk_from_row_key(
                        key,
                        schema_for_visit.table_id,
                        schema_for_visit.table_epoch,
                        schema_for_visit.pk_data_type(),
                    )
                    .map_err(|e| e.to_string())?;
                    let value =
                        value.ok_or_else(|| "v2 row visitor missing row value".to_string())?;
                    let row = storage_layout::decode_row_record(&schema_for_visit, value)
                        .map_err(|e| e.to_string())?;
                    if filter_for_visit
                        .as_ref()
                        .is_none_or(|expr| row_matches_filter(&row, &schema_for_visit, expr))
                    {
                        if state.remaining_offset > 0 {
                            state.remaining_offset -= 1;
                            return Ok(true);
                        }
                        state.rows.push((pk_value, row));
                        if scan_limit.is_some_and(|limit| state.rows.len() >= limit) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }));
            self.txn_visit_range(None, &range, KvScanProjection::KeyValue, visitor)
                .await
                .map_err(|e| user_error("XX000", e))?;
            let mut state = state
                .lock()
                .map_err(|e| user_error("XX000", e.to_string()))?;
            let rows = std::mem::take(&mut state.rows);
            return Ok((rows, state.saw_entries));
        }

        loop {
            let range = storage_layout::row_range_between(
                schema.table_id,
                schema.table_epoch,
                current_lower
                    .as_ref()
                    .map(|(value, inclusive)| (value, *inclusive)),
                upper,
                Some(batch_size),
            );
            let v2_entries = if consistent_read {
                self.txn_snapshot_scan_range(session_id, &range).await
            } else {
                self.txn_scan_range(session_id, &range).await
            }
            .map_err(|e| user_error("XX000", e))?;
            if v2_entries.is_empty() {
                break;
            }
            saw_entries = true;
            let batch_len = v2_entries.len();
            let mut last_pk = None;

            for (key, value) in v2_entries {
                let pk_value = storage_layout::decode_pk_from_row_key(
                    &key,
                    schema.table_id,
                    schema.table_epoch,
                    schema.pk_data_type(),
                )?;
                last_pk = Some(pk_value.clone());
                let row = if has_active_txn {
                    self.read_visible_row_at(
                        session_id,
                        database_name,
                        schema_name,
                        schema,
                        &pk_value,
                    )
                    .await?
                } else {
                    Some(storage_layout::decode_row_record(schema, &value)?)
                };
                let Some(row) = row else { continue };
                if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                    if remaining_offset > 0 {
                        remaining_offset -= 1;
                        continue;
                    }
                    rows.push((pk_value, row));
                    if scan_limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok((rows, true));
                    }
                }
            }

            if batch_len < batch_size {
                break;
            }
            let Some(last_pk) = last_pk else {
                break;
            };
            current_lower = Some((last_pk, false));
        }

        Ok((rows, saw_entries))
    }

    pub(super) async fn scan_legacy_row_marker_entries_by_pk_range_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
        filter: Option<&Expr>,
        scan_limit: Option<usize>,
        row_offset: usize,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        let prefix = row_marker_prefix(database_name, schema_name, &schema.table_name);
        let storage_scan_limit = filter
            .is_none()
            .then(|| scan_limit.map(|limit| limit.saturating_add(row_offset)))
            .flatten();
        let start = match lower {
            Some((value, true)) => {
                row_marker_key(database_name, schema_name, &schema.table_name, value)
            }
            Some((value, false)) => {
                let mut key = row_marker_key(database_name, schema_name, &schema.table_name, value);
                key.push(0);
                key
            }
            None => prefix.clone(),
        };
        let end = match upper {
            Some((value, true)) => {
                let mut key = row_marker_key(database_name, schema_name, &schema.table_name, value);
                key.push(0);
                Some(key)
            }
            Some((value, false)) => Some(row_marker_key(
                database_name,
                schema_name,
                &schema.table_name,
                value,
            )),
            None => next_key_prefix(&prefix),
        };
        let range = storage_layout::RangeScan {
            start,
            end,
            limit: storage_scan_limit,
            reverse: false,
        };
        let entries = if self.use_consistent_read(session_id).await {
            self.txn_snapshot_scan_range(session_id, &range).await
        } else {
            self.txn_scan_range(session_id, &range).await
        }
        .map_err(|e| user_error("XX000", e))?;

        let mut rows = Vec::with_capacity(scan_limit.unwrap_or(entries.len()));
        let mut remaining_offset = row_offset;
        for (key, _) in entries {
            let pk_value = decode_pk_from_row_marker_key(
                &key,
                database_name,
                schema_name,
                &schema.table_name,
                schema.pk_data_type(),
            )?;
            let Some(row) = self
                .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                .await?
            else {
                continue;
            };
            if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                if remaining_offset > 0 {
                    remaining_offset -= 1;
                    continue;
                }
                rows.push((pk_value, row));
                if scan_limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn scan_legacy_row_marker_entries_by_pk_range_batched_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
        filter: Option<&Expr>,
        scan_limit: Option<usize>,
        row_offset: usize,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        let prefix = row_marker_prefix(database_name, schema_name, &schema.table_name);
        let mut start = match lower {
            Some((value, true)) => {
                row_marker_key(database_name, schema_name, &schema.table_name, value)
            }
            Some((value, false)) => {
                let mut key = row_marker_key(database_name, schema_name, &schema.table_name, value);
                key.push(0);
                key
            }
            None => prefix.clone(),
        };
        let end = match upper {
            Some((value, true)) => {
                let mut key = row_marker_key(database_name, schema_name, &schema.table_name, value);
                key.push(0);
                Some(key)
            }
            Some((value, false)) => Some(row_marker_key(
                database_name,
                schema_name,
                &schema.table_name,
                value,
            )),
            None => next_key_prefix(&prefix),
        };

        let mut rows = Vec::with_capacity(scan_limit.unwrap_or(FILTERED_LIMIT_SCAN_BATCH_SIZE));
        let mut remaining_offset = row_offset;
        let batch_size = filtered_limit_scan_batch_size(scan_limit, row_offset);

        loop {
            let range = storage_layout::RangeScan {
                start: start.clone(),
                end: end.clone(),
                limit: Some(batch_size),
                reverse: false,
            };
            let entries = if self.use_consistent_read(session_id).await {
                self.txn_snapshot_scan_range(session_id, &range).await
            } else {
                self.txn_scan_range(session_id, &range).await
            }
            .map_err(|e| user_error("XX000", e))?;
            if entries.is_empty() {
                break;
            }
            let batch_len = entries.len();
            let mut last_key = None;

            for (key, _) in entries {
                last_key = Some(key.clone());
                let pk_value = decode_pk_from_row_marker_key(
                    &key,
                    database_name,
                    schema_name,
                    &schema.table_name,
                    schema.pk_data_type(),
                )?;
                let Some(row) = self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                    .await?
                else {
                    continue;
                };
                if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                    if remaining_offset > 0 {
                        remaining_offset -= 1;
                        continue;
                    }
                    rows.push((pk_value, row));
                    if scan_limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(rows);
                    }
                }
            }

            if batch_len < batch_size {
                break;
            }
            let Some(mut last_key) = last_key else {
                break;
            };
            last_key.push(0);
            start = last_key;
        }

        Ok(rows)
    }

    pub(super) async fn scan_index_rows_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        index_name: &str,
        index_value: &ColumnValue,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<RowMap>> {
        Ok(self
            .scan_index_row_entries_at(
                session_id,
                database_name,
                schema_name,
                schema,
                index_name,
                index_value,
                filter,
            )
            .await?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    pub(super) async fn scan_index_row_entries_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        index_name: &str,
        index_value: &ColumnValue,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        let index_meta = self
            .catalog
            .list_indexes_for_table(schema.table_id)
            .await?
            .into_iter()
            .find(|index| index.index_name == index_name);
        if let Some(index_meta) = index_meta {
            let prefix = storage_layout::index_prefix(index_meta.index_id, index_value);
            let range = storage_layout::RangeScan {
                end: {
                    let mut upper = prefix.clone();
                    for idx in (0..upper.len()).rev() {
                        if upper[idx] != u8::MAX {
                            upper[idx] += 1;
                            upper.truncate(idx + 1);
                            break;
                        }
                    }
                    Some(upper)
                },
                start: prefix,
                limit: None,
                reverse: false,
            };
            let entries = if self.use_consistent_read(session_id).await {
                self.txn_snapshot_scan_range(session_id, &range).await
            } else {
                self.txn_scan_range(session_id, &range).await
            }
            .map_err(|e| user_error("XX000", e))?;
            if !entries.is_empty() {
                let mut rows = Vec::with_capacity(entries.len());
                for (key, _) in entries {
                    let pk_value = storage_layout::decode_pk_from_index_entry_key(
                        &key,
                        index_meta.index_id,
                        index_value,
                        schema.pk_data_type(),
                    )?;
                    let Some(row) = self
                        .read_visible_row_at(
                            session_id,
                            database_name,
                            schema_name,
                            schema,
                            &pk_value,
                        )
                        .await?
                    else {
                        continue;
                    };
                    if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                        rows.push((pk_value, row));
                    }
                }
                return Ok(rows);
            }
        }

        let prefix = index_entry_prefix(
            database_name,
            schema_name,
            &schema.table_name,
            index_name,
            index_value,
        );
        let entries = if self.use_consistent_read(session_id).await {
            self.txn_snapshot_scan_prefix(session_id, &prefix).await
        } else {
            self.txn_scan_prefix(session_id, &prefix).await
        }
        .map_err(|e| user_error("XX000", e))?;
        let mut rows = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            let pk_value = decode_pk_from_index_entry_key(
                &key,
                database_name,
                schema_name,
                &schema.table_name,
                index_name,
                index_value,
                schema.pk_data_type(),
            )?;
            let Some(row) = self
                .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                .await?
            else {
                continue;
            };
            if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                rows.push((pk_value, row));
            }
        }
        Ok(rows)
    }

    pub(super) fn value_in_bounds(
        value: &ColumnValue,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
    ) -> bool {
        let lower_ok = match lower.and_then(|(bound, inclusive)| {
            value
                .partial_cmp(bound)
                .map(|ordering| (ordering, inclusive))
        }) {
            Some((std::cmp::Ordering::Less, _)) => false,
            Some((std::cmp::Ordering::Equal, inclusive)) => inclusive,
            _ => true,
        };
        let upper_ok = match upper.and_then(|(bound, inclusive)| {
            value
                .partial_cmp(bound)
                .map(|ordering| (ordering, inclusive))
        }) {
            Some((std::cmp::Ordering::Greater, _)) => false,
            Some((std::cmp::Ordering::Equal, inclusive)) => inclusive,
            _ => true,
        };
        lower_ok && upper_ok
    }

    pub(super) async fn scan_index_row_entries_by_range_at(
        &self,
        session_id: Option<i32>,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        index_name: &str,
        lower: Option<(&ColumnValue, bool)>,
        upper: Option<(&ColumnValue, bool)>,
        filter: Option<&Expr>,
    ) -> PgWireResult<Vec<(ColumnValue, RowMap)>> {
        let index_meta = self
            .catalog
            .list_indexes_for_table(schema.table_id)
            .await?
            .into_iter()
            .find(|index| index.index_name == index_name);
        if let Some(index_meta) = index_meta {
            let Some(index_column) = index_meta
                .column_names
                .first()
                .and_then(|name| schema.find_column(name))
            else {
                return Ok(Vec::new());
            };
            let all_prefix = storage_layout::index_all_prefix(index_meta.index_id);
            let start = match lower {
                Some((value, true)) => storage_layout::index_prefix(index_meta.index_id, value),
                Some((value, false)) => {
                    next_key_prefix(&storage_layout::index_prefix(index_meta.index_id, value))
                        .unwrap_or_else(|| storage_layout::index_prefix(index_meta.index_id, value))
                }
                None => all_prefix.clone(),
            };
            let end = match upper {
                Some((value, true)) => {
                    next_key_prefix(&storage_layout::index_prefix(index_meta.index_id, value))
                }
                Some((value, false)) => {
                    Some(storage_layout::index_prefix(index_meta.index_id, value))
                }
                None => next_key_prefix(&all_prefix),
            };
            if end.as_ref().is_some_and(|end| start >= *end) {
                return Ok(Vec::new());
            }
            let range = storage_layout::RangeScan {
                start,
                end,
                limit: None,
                reverse: false,
            };
            let entries = if self.use_consistent_read(session_id).await {
                self.txn_snapshot_scan_range(session_id, &range).await
            } else {
                self.txn_scan_range(session_id, &range).await
            }
            .map_err(|e| user_error("XX000", e))?;
            let mut rows = Vec::with_capacity(entries.len());
            for (key, _) in entries {
                let suffix = key.strip_prefix(all_prefix.as_slice()).ok_or_else(|| {
                    user_error("XX000", "v2 index key missing expected all-index prefix")
                })?;
                let (index_value, consumed) =
                    storage_layout::decode_key_value(&index_column.data_type, suffix)?;
                if !Self::value_in_bounds(&index_value, lower, upper) {
                    continue;
                }
                let (pk_value, pk_consumed) =
                    storage_layout::decode_key_value(schema.pk_data_type(), &suffix[consumed..])?;
                if consumed + pk_consumed != suffix.len() {
                    return Err(user_error("XX000", "malformed v2 index range key suffix"));
                }
                let Some(row) = self
                    .read_visible_row_at(session_id, database_name, schema_name, schema, &pk_value)
                    .await?
                else {
                    continue;
                };
                if filter.is_none_or(|expr| row_matches_filter(&row, schema, expr)) {
                    rows.push((pk_value, row));
                }
            }
            return Ok(rows);
        }

        self.scan_visible_row_entries_at(session_id, database_name, schema_name, schema, filter)
            .await
    }
}

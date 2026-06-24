use std::collections::HashMap;
use std::sync::atomic::Ordering;

use pgwire::api::ClientInfo;
use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use sqlparser::ast::{Assignment, Expr, Ident};

use super::GatewayServer;
use super::shared::{
    METADATA_TRANSACTION_ISOLATION, TransactionIsolation, column_value_to_i64,
    decode_sequence_state, encode_sequence_state, mysql_auto_increment_column, sequence_state_key,
};
use crate::catalog::resolve_table_reference;
use crate::core::response::{command_complete, empty_query_response};
use crate::error::{unsupported, user_error};
use crate::mem_store::{KvRangeVisitor, KvScanProjection};
use crate::sql::{
    column_default_value, extract_assignment_column, is_default_expr, nextval_sequence_name,
    sql_expr_to_column_value,
};
use crate::storage_layout;
use crate::types::MySqlIntKind;
use crate::types::{ColumnSchema, ColumnValue, DataType, RowMap, TableSchema};

impl GatewayServer {
    pub(super) async fn savepoint_session_transaction<C>(
        &self,
        client: &mut C,
        name: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txns = self.active_transactions.lock().await;
        let Some(txn) = txns.get(&session_id) else {
            return Err(user_error(
                "25P01",
                "SAVEPOINT can only be used in transaction blocks",
            ));
        };
        txn.savepoint(name)
            .await
            .map_err(|e| user_error("0A000", e))?;
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(vec![command_complete("SAVEPOINT")])
    }

    pub(super) async fn rollback_to_session_savepoint<C>(
        &self,
        client: &mut C,
        name: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txns = self.active_transactions.lock().await;
        let Some(txn) = txns.get(&session_id) else {
            return Err(user_error(
                "25P01",
                "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
            ));
        };
        txn.rollback_to_savepoint(name)
            .await
            .map_err(|e| user_error("3B001", e))?;
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(vec![command_complete("ROLLBACK")])
    }

    pub(super) async fn release_session_savepoint<C>(
        &self,
        client: &mut C,
        name: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txns = self.active_transactions.lock().await;
        let Some(txn) = txns.get(&session_id) else {
            return Err(user_error(
                "25P01",
                "RELEASE SAVEPOINT can only be used in transaction blocks",
            ));
        };
        txn.release_savepoint(name)
            .await
            .map_err(|e| user_error("3B001", e))?;
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(vec![command_complete("RELEASE")])
    }

    pub(super) async fn has_active_transaction<C>(&self, client: &C) -> bool
    where
        C: ClientInfo,
    {
        self.active_transactions
            .lock()
            .await
            .contains_key(&self.session_id(client))
    }

    pub(super) async fn set_session_transaction_isolation<C>(
        &self,
        client: &mut C,
        isolation: TransactionIsolation,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        client.metadata_mut().insert(
            METADATA_TRANSACTION_ISOLATION.to_string(),
            isolation.as_pg_str().to_string(),
        );
        if self
            .active_transactions
            .lock()
            .await
            .contains_key(&session_id)
        {
            self.active_transaction_isolations
                .lock()
                .await
                .insert(session_id, isolation);
            self.active_transaction_snapshots
                .lock()
                .await
                .insert(session_id, self.mvcc_clock.load(Ordering::SeqCst));
        }
        Ok(vec![empty_query_response()])
    }

    pub(super) async fn txn_get(
        &self,
        session_id: Option<i32>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.get(key).await;
            }
        }
        self.store.get(key).await
    }

    pub(super) async fn txn_snapshot_get(
        &self,
        session_id: Option<i32>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.snapshot_get(key).await;
            }
        }
        self.store.get(key).await
    }

    pub(super) async fn is_repeatable_read_transaction(&self, session_id: Option<i32>) -> bool {
        let Some(session_id) = session_id else {
            return false;
        };
        self.active_transaction_isolations
            .lock()
            .await
            .get(&session_id)
            .is_some_and(|isolation| *isolation == TransactionIsolation::RepeatableRead)
    }

    pub(super) async fn use_consistent_read(&self, session_id: Option<i32>) -> bool {
        let Some(session_id) = session_id else {
            return false;
        };
        if !self.is_repeatable_read_transaction(Some(session_id)).await {
            return false;
        }
        !self
            .active_mysql_current_reads
            .lock()
            .await
            .contains(&session_id)
    }

    pub(super) async fn enter_mysql_current_read(&self, session_id: i32) {
        self.active_mysql_current_reads
            .lock()
            .await
            .insert(session_id);
    }

    pub(super) async fn exit_mysql_current_read(&self, session_id: i32) {
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
    }

    pub(super) async fn txn_has_pending_key(
        &self,
        session_id: Option<i32>,
        key: &[u8],
    ) -> Result<bool, String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.has_pending_key(key).await;
            }
        }
        Ok(false)
    }

    pub(super) async fn txn_put(
        &self,
        session_id: Option<i32>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.put(key, value).await;
            }
        }
        self.store.put(key, value).await
    }

    pub(super) async fn txn_put_batch(
        &self,
        session_id: Option<i32>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.put_batch(entries).await;
            }
        }
        self.store.put_batch(entries).await
    }

    pub(super) async fn txn_delete(
        &self,
        session_id: Option<i32>,
        key: &[u8],
    ) -> Result<(), String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.delete(key).await;
            }
        }
        self.store.delete(key).await
    }

    pub(super) async fn txn_scan_prefix(
        &self,
        session_id: Option<i32>,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.scan_prefix(prefix).await;
            }
        }
        self.store.scan_prefix(prefix).await
    }

    pub(super) async fn txn_snapshot_scan_prefix(
        &self,
        session_id: Option<i32>,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if let Some(session_id) = session_id {
            let txns = self.active_transactions.lock().await;
            if let Some(txn) = txns.get(&session_id) {
                return txn.snapshot_scan_prefix(prefix).await;
            }
        }
        self.store.scan_prefix(prefix).await
    }

    pub(super) async fn txn_scan_range(
        &self,
        session_id: Option<i32>,
        range: &storage_layout::RangeScan,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if let Some(session_id) = session_id {
            let transactions = self.active_transactions.lock().await;
            if let Some(txn) = transactions.get(&session_id) {
                return txn
                    .scan_range(
                        &range.start,
                        range.end.as_deref(),
                        range.limit,
                        range.reverse,
                    )
                    .await;
            }
        }
        self.store
            .scan_range(
                &range.start,
                range.end.as_deref(),
                range.limit,
                range.reverse,
            )
            .await
    }

    pub(super) async fn txn_snapshot_scan_range(
        &self,
        session_id: Option<i32>,
        range: &storage_layout::RangeScan,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        if let Some(session_id) = session_id {
            let transactions = self.active_transactions.lock().await;
            if let Some(txn) = transactions.get(&session_id) {
                return txn
                    .snapshot_scan_range(
                        &range.start,
                        range.end.as_deref(),
                        range.limit,
                        range.reverse,
                    )
                    .await;
            }
        }
        self.store
            .scan_range(
                &range.start,
                range.end.as_deref(),
                range.limit,
                range.reverse,
            )
            .await
    }

    pub(super) async fn txn_visit_range(
        &self,
        session_id: Option<i32>,
        range: &storage_layout::RangeScan,
        projection: KvScanProjection,
        visitor: KvRangeVisitor,
    ) -> Result<(), String> {
        if let Some(session_id) = session_id {
            let transactions = self.active_transactions.lock().await;
            if let Some(txn) = transactions.get(&session_id) {
                return txn
                    .visit_range(
                        &range.start,
                        range.end.as_deref(),
                        range.reverse,
                        projection,
                        visitor,
                    )
                    .await;
            }
        }
        self.store
            .visit_range(
                &range.start,
                range.end.as_deref(),
                range.reverse,
                projection,
                visitor,
            )
            .await
    }

    pub(super) async fn allocate_internal_row_id(
        &self,
        table_id: u32,
    ) -> PgWireResult<ColumnValue> {
        let key = format!("__catalog__/tables/next-row-id/{table_id}");
        let current = self
            .store
            .get(key.as_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        let next = current
            .as_deref()
            .map(|bytes| {
                let raw: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| user_error("XX000", "row id counter is malformed"))?;
                Ok::<i64, PgWireError>(i64::from_be_bytes(raw) + 1)
            })
            .transpose()?
            .unwrap_or(1);
        self.store
            .put(key.as_bytes(), &next.to_be_bytes())
            .await
            .map_err(|e| user_error("XX000", e))?;
        Ok(ColumnValue::Int64(next))
    }

    pub(super) fn resolve_sequence_name(
        &self,
        default_schema_name: &str,
        sequence: &str,
    ) -> PgWireResult<(String, String)> {
        if sequence.contains('.') {
            resolve_table_reference(sequence)
        } else {
            Ok((default_schema_name.to_string(), sequence.to_string()))
        }
    }

    pub(super) async fn create_sequence_at(
        &self,
        database_name: &str,
        schema_name: &str,
        sequence_name: &str,
        if_not_exists: bool,
        start: i64,
        increment: i64,
    ) -> PgWireResult<()> {
        let key = sequence_state_key(database_name, schema_name, sequence_name);
        if self
            .store
            .get(&key)
            .await
            .map_err(|e| user_error("XX000", e))?
            .is_some()
        {
            if if_not_exists {
                return Ok(());
            }
            return Err(user_error(
                "42P07",
                format!("sequence '{schema_name}.{sequence_name}' already exists"),
            ));
        }
        self.store
            .put(&key, &encode_sequence_state(start, increment))
            .await
            .map_err(|e| user_error("XX000", e))
    }

    pub(super) async fn allocate_sequence_value(
        &self,
        database_name: &str,
        default_schema_name: &str,
        sequence: &str,
        data_type: &DataType,
    ) -> PgWireResult<ColumnValue> {
        let (schema_name, sequence_name) =
            self.resolve_sequence_name(default_schema_name, sequence)?;
        let key = sequence_state_key(database_name, &schema_name, &sequence_name);
        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(|e| user_error("XX000", e))?
            .ok_or_else(|| {
                user_error(
                    "42P01",
                    format!("sequence '{schema_name}.{sequence_name}' does not exist"),
                )
            })?;
        let (current, increment) = decode_sequence_state(&bytes)?;
        let next = current
            .checked_add(increment)
            .ok_or_else(|| user_error("2200H", "sequence generator limit exceeded"))?;
        self.store
            .put(&key, &encode_sequence_state(next, increment))
            .await
            .map_err(|e| user_error("XX000", e))?;
        match data_type {
            DataType::Int16 => i16::try_from(current)
                .map(ColumnValue::Int16)
                .map_err(|_| user_error("22003", "sequence value is out of range for INT2")),
            DataType::Int32 => i32::try_from(current)
                .map(ColumnValue::Int32)
                .map_err(|_| user_error("22003", "sequence value is out of range for INT4")),
            DataType::Int64 => Ok(ColumnValue::Int64(current)),
            DataType::MySqlInt {
                kind: MySqlIntKind::Tiny | MySqlIntKind::Small,
                unsigned: false,
            } => i16::try_from(current)
                .map(ColumnValue::Int16)
                .map_err(|_| user_error("22003", "sequence value is out of range")),
            DataType::MySqlInt {
                kind: MySqlIntKind::Medium | MySqlIntKind::Int,
                unsigned: false,
            } => i32::try_from(current)
                .map(ColumnValue::Int32)
                .map_err(|_| user_error("22003", "sequence value is out of range")),
            DataType::MySqlInt { .. } => Ok(ColumnValue::Int64(current)),
            _ => Ok(ColumnValue::Int64(current)),
        }
    }

    pub(super) async fn advance_sequence_to_at_least(
        &self,
        database_name: &str,
        default_schema_name: &str,
        sequence: &str,
        minimum_next: i64,
    ) -> PgWireResult<()> {
        let (schema_name, sequence_name) =
            self.resolve_sequence_name(default_schema_name, sequence)?;
        let key = sequence_state_key(database_name, &schema_name, &sequence_name);
        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(|e| user_error("XX000", e))?
            .ok_or_else(|| {
                user_error(
                    "42P01",
                    format!("sequence '{schema_name}.{sequence_name}' does not exist"),
                )
            })?;
        let (current, increment) = decode_sequence_state(&bytes)?;
        if minimum_next > current {
            self.store
                .put(&key, &encode_sequence_state(minimum_next, increment))
                .await
                .map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn advance_auto_increment_from_row(
        &self,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        row: &RowMap,
    ) -> PgWireResult<Option<i64>> {
        let Some(column) = mysql_auto_increment_column(schema) else {
            return Ok(None);
        };
        let Some(value) = row.get(&column.name).and_then(column_value_to_i64) else {
            return Ok(None);
        };
        if let Some(sequence) = column.default.as_deref().and_then(nextval_sequence_name) {
            self.advance_sequence_to_at_least(
                database_name,
                schema_name,
                &sequence,
                value.saturating_add(1),
            )
            .await?;
        }
        Ok(Some(value))
    }

    pub(super) async fn reset_auto_increment_sequence(
        &self,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
    ) -> PgWireResult<()> {
        let Some(column) = mysql_auto_increment_column(schema) else {
            return Ok(());
        };
        let Some(sequence) = column.default.as_deref().and_then(nextval_sequence_name) else {
            return Ok(());
        };
        let (sequence_schema, sequence_name) =
            self.resolve_sequence_name(schema_name, &sequence)?;
        let key = sequence_state_key(database_name, &sequence_schema, &sequence_name);
        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(|e| user_error("XX000", e))?
            .ok_or_else(|| {
                user_error(
                    "42P01",
                    format!("sequence '{sequence_schema}.{sequence_name}' does not exist"),
                )
            })?;
        let (_, increment) = decode_sequence_state(&bytes)?;
        self.store
            .put(&key, &encode_sequence_state(1, increment))
            .await
            .map_err(|e| user_error("XX000", e))
    }

    pub(super) async fn column_default_value_at(
        &self,
        database_name: &str,
        schema_name: &str,
        column: &ColumnSchema,
    ) -> PgWireResult<ColumnValue> {
        let Some(default_sql) = column.default.as_deref() else {
            return Ok(ColumnValue::Null);
        };
        if let Some(sequence) = nextval_sequence_name(default_sql) {
            self.allocate_sequence_value(database_name, schema_name, &sequence, &column.data_type)
                .await
        } else {
            column_default_value(default_sql, &column.data_type)
        }
    }

    pub(super) async fn build_insert_row_at(
        &self,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        columns: &[Ident],
        values: Vec<Expr>,
    ) -> PgWireResult<RowMap> {
        let source_columns = if columns.is_empty() {
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            columns
                .iter()
                .map(|column| column.value.clone())
                .collect::<Vec<_>>()
        };
        if source_columns.len() != values.len() {
            return Err(unsupported(
                "INSERT fast-path requires the same number of columns and values",
            ));
        }

        let mut provided = HashMap::new();
        for (column_name, expr) in source_columns.iter().zip(values.iter()) {
            let column = schema
                .find_column(column_name)
                .ok_or_else(|| user_error("42703", format!("column '{column_name}' not found")))?;
            let value = if is_default_expr(expr) {
                self.column_default_value_at(database_name, schema_name, column)
                    .await?
            } else {
                sql_expr_to_column_value(expr, &column.data_type)?
            };
            provided.insert(column_name.clone(), value);
        }

        let mut row = HashMap::new();
        for column in &schema.columns {
            let value = match provided.remove(&column.name) {
                Some(value) => value,
                None => {
                    self.column_default_value_at(database_name, schema_name, column)
                        .await?
                }
            };
            row.insert(column.name.clone(), value);
        }
        Ok(row)
    }

    pub(super) async fn build_insert_row_from_assignments_at(
        &self,
        database_name: &str,
        schema_name: &str,
        schema: &TableSchema,
        assignments: Vec<Assignment>,
    ) -> PgWireResult<RowMap> {
        let mut provided = HashMap::new();
        for assignment in assignments {
            let column_name = extract_assignment_column(&assignment)?;
            let column = schema
                .find_column(&column_name)
                .ok_or_else(|| user_error("42703", format!("column '{column_name}' not found")))?;
            let value = if is_default_expr(&assignment.value) {
                self.column_default_value_at(database_name, schema_name, column)
                    .await?
            } else {
                sql_expr_to_column_value(&assignment.value, &column.data_type)?
            };
            provided.insert(column_name, value);
        }

        let mut row = HashMap::new();
        for column in &schema.columns {
            let value = match provided.remove(&column.name) {
                Some(value) => value,
                None => {
                    self.column_default_value_at(database_name, schema_name, column)
                        .await?
                }
            };
            row.insert(column.name.clone(), value);
        }
        Ok(row)
    }
}

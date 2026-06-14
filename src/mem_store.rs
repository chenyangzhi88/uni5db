use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::storage_layout;
use crate::types::{ColumnValue, RowMap, TableSchema};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvScanProjection {
    KeyOnly,
    KeyValue,
}

pub type KvRangeVisitor =
    Arc<Mutex<dyn FnMut(&[u8], Option<&[u8]>) -> Result<bool, String> + Send + 'static>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvCompareOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Clone, Debug, PartialEq)]
pub enum KvPredicate {
    ColumnCompare {
        column_idx: usize,
        op: KvCompareOp,
        value: ColumnValue,
    },
    And(Vec<KvPredicate>),
    Or(Vec<KvPredicate>),
    Not(Box<KvPredicate>),
    IsNull {
        column_idx: usize,
    },
    IsNotNull {
        column_idx: usize,
    },
    Between {
        column_idx: usize,
        low: ColumnValue,
        high: ColumnValue,
        negated: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvAggregateOp {
    CountStar,
    CountColumn { column_idx: usize },
    MaxColumn { column_idx: usize },
    MinColumn { column_idx: usize },
    SumColumn { column_idx: usize },
    AvgColumn { column_idx: usize },
}

#[derive(Clone, Debug)]
pub struct KvAggregateScan {
    pub schema: TableSchema,
    pub range_start: Vec<u8>,
    pub range_end: Option<Vec<u8>>,
    pub scan_prefix: Option<Vec<u8>>,
    pub filters: Vec<KvPredicate>,
    pub group_indices: Vec<usize>,
    pub aggregates: Vec<KvAggregateOp>,
    pub required_indices: Vec<usize>,
    pub projection: KvScanProjection,
}

#[derive(Clone, Debug)]
pub enum KvAggregateState {
    Count(i64),
    Value(ColumnValue),
    Avg { sum: f64, count: i64 },
}

pub fn kv_aggregate_initial_state(op: &KvAggregateOp) -> KvAggregateState {
    match op {
        KvAggregateOp::CountStar | KvAggregateOp::CountColumn { .. } => KvAggregateState::Count(0),
        KvAggregateOp::MaxColumn { .. }
        | KvAggregateOp::MinColumn { .. }
        | KvAggregateOp::SumColumn { .. } => KvAggregateState::Value(ColumnValue::Null),
        KvAggregateOp::AvgColumn { .. } => KvAggregateState::Avg { sum: 0.0, count: 0 },
    }
}

fn compare_values(left: &ColumnValue, op: KvCompareOp, right: &ColumnValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    match op {
        KvCompareOp::Eq => left == right,
        KvCompareOp::NotEq => left != right,
        KvCompareOp::Gt => left.partial_cmp(right).is_some_and(|ord| ord.is_gt()),
        KvCompareOp::GtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_lt()),
        KvCompareOp::Lt => left.partial_cmp(right).is_some_and(|ord| ord.is_lt()),
        KvCompareOp::LtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_gt()),
    }
}

fn row_value(schema: &TableSchema, row: &RowMap, column_idx: usize) -> ColumnValue {
    schema
        .columns
        .get(column_idx)
        .and_then(|column| row.get(&column.name))
        .cloned()
        .unwrap_or(ColumnValue::Null)
}

fn sum_values(left: &ColumnValue, right: &ColumnValue) -> Option<ColumnValue> {
    match (left, right) {
        (ColumnValue::Null, value) => Some(value.clone()),
        (ColumnValue::Int32(left), ColumnValue::Int32(right)) => {
            Some(ColumnValue::Int64(*left as i64 + *right as i64))
        }
        (ColumnValue::Int32(left), ColumnValue::Int64(right)) => {
            Some(ColumnValue::Int64(*left as i64 + *right))
        }
        (ColumnValue::Int64(left), ColumnValue::Int32(right)) => {
            Some(ColumnValue::Int64(*left + *right as i64))
        }
        (ColumnValue::Int64(left), ColumnValue::Int64(right)) => {
            Some(ColumnValue::Int64(*left + *right))
        }
        (ColumnValue::Float32(left), ColumnValue::Float32(right)) => {
            Some(ColumnValue::Float64(*left as f64 + *right as f64))
        }
        (ColumnValue::Float32(left), ColumnValue::Float64(right)) => {
            Some(ColumnValue::Float64(*left as f64 + *right))
        }
        (ColumnValue::Float64(left), ColumnValue::Float32(right)) => {
            Some(ColumnValue::Float64(*left + *right as f64))
        }
        (ColumnValue::Float64(left), ColumnValue::Float64(right)) => {
            Some(ColumnValue::Float64(*left + *right))
        }
        _ => None,
    }
}

fn numeric_as_f64(value: &ColumnValue) -> Option<f64> {
    match value {
        ColumnValue::Int32(value) => Some(*value as f64),
        ColumnValue::Int64(value) => Some(*value as f64),
        ColumnValue::Float32(value) => Some(*value as f64),
        ColumnValue::Float64(value) => Some(*value),
        _ => None,
    }
}

pub fn kv_predicate_matches(schema: &TableSchema, row: &RowMap, predicate: &KvPredicate) -> bool {
    match predicate {
        KvPredicate::ColumnCompare {
            column_idx,
            op,
            value,
        } => compare_values(&row_value(schema, row, *column_idx), *op, value),
        KvPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| kv_predicate_matches(schema, row, predicate)),
        KvPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| kv_predicate_matches(schema, row, predicate)),
        KvPredicate::Not(predicate) => !kv_predicate_matches(schema, row, predicate),
        KvPredicate::IsNull { column_idx } => row_value(schema, row, *column_idx).is_null(),
        KvPredicate::IsNotNull { column_idx } => !row_value(schema, row, *column_idx).is_null(),
        KvPredicate::Between {
            column_idx,
            low,
            high,
            negated,
        } => {
            let value = row_value(schema, row, *column_idx);
            let inside = compare_values(&value, KvCompareOp::GtEq, low)
                && compare_values(&value, KvCompareOp::LtEq, high);
            if *negated { !inside } else { inside }
        }
    }
}

pub fn kv_row_matches(schema: &TableSchema, row: &RowMap, filters: &[KvPredicate]) -> bool {
    filters
        .iter()
        .all(|predicate| kv_predicate_matches(schema, row, predicate))
}

pub fn kv_apply_aggregate(
    schema: &TableSchema,
    row: &RowMap,
    states: &mut [KvAggregateState],
    aggregates: &[KvAggregateOp],
) {
    for (idx, aggregate) in aggregates.iter().enumerate() {
        match aggregate {
            KvAggregateOp::CountStar => {
                if let KvAggregateState::Count(count) = &mut states[idx] {
                    *count += 1;
                }
            }
            KvAggregateOp::CountColumn { column_idx } => {
                if !row_value(schema, row, *column_idx).is_null() {
                    if let KvAggregateState::Count(count) = &mut states[idx] {
                        *count += 1;
                    }
                }
            }
            KvAggregateOp::MaxColumn { column_idx } => {
                let value = row_value(schema, row, *column_idx);
                if value.is_null() {
                    continue;
                }
                let KvAggregateState::Value(state) = &mut states[idx] else {
                    continue;
                };
                if state.is_null()
                    || value
                        .partial_cmp(state)
                        .is_some_and(|ordering| ordering.is_gt())
                {
                    *state = value;
                }
            }
            KvAggregateOp::MinColumn { column_idx } => {
                let value = row_value(schema, row, *column_idx);
                if value.is_null() {
                    continue;
                }
                let KvAggregateState::Value(state) = &mut states[idx] else {
                    continue;
                };
                if state.is_null()
                    || value
                        .partial_cmp(state)
                        .is_some_and(|ordering| ordering.is_lt())
                {
                    *state = value;
                }
            }
            KvAggregateOp::SumColumn { column_idx } => {
                let value = row_value(schema, row, *column_idx);
                if value.is_null() {
                    continue;
                }
                let KvAggregateState::Value(state) = &mut states[idx] else {
                    continue;
                };
                if let Some(sum) = sum_values(state, &value) {
                    *state = sum;
                }
            }
            KvAggregateOp::AvgColumn { column_idx } => {
                let value = row_value(schema, row, *column_idx);
                let Some(value) = numeric_as_f64(&value) else {
                    continue;
                };
                if let KvAggregateState::Avg { sum, count } = &mut states[idx] {
                    *sum += value;
                    *count += 1;
                }
            }
        }
    }
}

pub fn kv_finish_aggregate_state(state: &KvAggregateState) -> ColumnValue {
    match state {
        KvAggregateState::Count(count) => ColumnValue::Int64(*count),
        KvAggregateState::Value(value) => value.clone(),
        KvAggregateState::Avg { sum, count } => {
            if *count == 0 {
                ColumnValue::Null
            } else {
                ColumnValue::Float64(*sum / *count as f64)
            }
        }
    }
}

pub fn kv_group_values(
    schema: &TableSchema,
    row: &RowMap,
    group_indices: &[usize],
) -> Vec<ColumnValue> {
    group_indices
        .iter()
        .map(|idx| row_value(schema, row, *idx))
        .collect()
}

pub fn kv_group_key(values: &[ColumnValue]) -> String {
    values
        .iter()
        .map(|value| value.to_text().unwrap_or_else(|| format!("{value:?}")))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

#[async_trait]
pub trait KvTransaction: Send + Sync {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String>;

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String>;

    async fn put_write_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
        self.put_batch(entries).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.get(key).await
    }

    async fn has_pending_key(&self, _key: &[u8]) -> Result<bool, String> {
        Ok(false)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;

    async fn snapshot_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        self.scan_prefix(prefix).await
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;

    async fn snapshot_scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        self.scan_range(start, end, limit, reverse).await
    }

    async fn visit_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        projection: KvScanProjection,
        visitor: KvRangeVisitor,
    ) -> Result<(), String> {
        let rows = self.scan_range(start, end, None, reverse).await?;
        for (key, value) in rows {
            let value = match projection {
                KvScanProjection::KeyOnly => None,
                KvScanProjection::KeyValue => Some(value.as_slice()),
            };
            let mut visitor = visitor.lock().map_err(|e| e.to_string())?;
            if !visitor(&key, value)? {
                break;
            }
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), String>;

    async fn rollback(&self) -> Result<(), String>;

    async fn savepoint(&self, _name: &str) -> Result<(), String> {
        Err("savepoints are not supported by this KV transaction".to_string())
    }

    async fn rollback_to_savepoint(&self, _name: &str) -> Result<(), String> {
        Err("savepoints are not supported by this KV transaction".to_string())
    }

    async fn release_savepoint(&self, _name: &str) -> Result<(), String> {
        Err("savepoints are not supported by this KV transaction".to_string())
    }
}

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String>;

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String>;

    async fn sync_wal(&self) -> Result<(), String> {
        Ok(())
    }

    async fn rewrite_table_rows_to_fast_format(
        &self,
        _schema: TableSchema,
        _batch_size: usize,
    ) -> Result<(usize, usize), String> {
        Err("row format rewrite is not supported by this KV store".to_string())
    }

    async fn compact_storage(&self) -> Result<(), String> {
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, String> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(&key).await?);
        }
        Ok(values)
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String>;

    async fn scan_range_projected(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
        projection: KvScanProjection,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, String> {
        self.scan_range(start, end, limit, reverse)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|(key, value)| {
                        let value = match projection {
                            KvScanProjection::KeyOnly => None,
                            KvScanProjection::KeyValue => Some(value),
                        };
                        (key, value)
                    })
                    .collect()
            })
    }

    async fn visit_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        projection: KvScanProjection,
        visitor: KvRangeVisitor,
    ) -> Result<(), String> {
        let rows = self
            .scan_range_projected(start, end, None, reverse, projection)
            .await?;
        for (key, value) in rows {
            let mut visitor = visitor.lock().map_err(|e| e.to_string())?;
            if !visitor(&key, value.as_deref())? {
                break;
            }
        }
        Ok(())
    }

    async fn count_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<u64, String> {
        self.scan_range_projected(start, end, None, false, KvScanProjection::KeyOnly)
            .await
            .map(|rows| rows.len() as u64)
    }

    async fn aggregate_scan(&self, plan: KvAggregateScan) -> Result<Vec<Vec<ColumnValue>>, String> {
        let rows = self
            .scan_range_projected(
                &plan.range_start,
                plan.range_end.as_deref(),
                None,
                false,
                plan.projection,
            )
            .await?;
        let pk_only = plan.required_indices.iter().all(|idx| {
            plan.schema
                .columns
                .get(*idx)
                .is_some_and(|column| column.name == plan.schema.primary_key)
        });
        let mut groups: BTreeMap<String, (Vec<ColumnValue>, Vec<KvAggregateState>)> =
            BTreeMap::new();
        for (key, value) in rows {
            let row = if pk_only {
                let pk_value = storage_layout::decode_pk_from_row_key(
                    &key,
                    plan.schema.table_id,
                    plan.schema.table_epoch,
                    plan.schema.pk_data_type(),
                )
                .map_err(|e| e.to_string())?;
                let mut row = RowMap::new();
                row.insert(plan.schema.primary_key.clone(), pk_value);
                row
            } else {
                let Some(value) = value else {
                    continue;
                };
                storage_layout::decode_row_record(&plan.schema, &value)
                    .map_err(|e| e.to_string())?
            };
            if kv_row_matches(&plan.schema, &row, &plan.filters) {
                let group_values = kv_group_values(&plan.schema, &row, &plan.group_indices);
                let group_key = kv_group_key(&group_values);
                let (_, states) = groups.entry(group_key).or_insert_with(|| {
                    (
                        group_values,
                        plan.aggregates
                            .iter()
                            .map(kv_aggregate_initial_state)
                            .collect(),
                    )
                });
                kv_apply_aggregate(&plan.schema, &row, states, &plan.aggregates);
            }
        }
        if groups.is_empty() && plan.group_indices.is_empty() {
            groups.insert(
                String::new(),
                (
                    Vec::new(),
                    plan.aggregates
                        .iter()
                        .map(kv_aggregate_initial_state)
                        .collect(),
                ),
            );
        }
        Ok(groups
            .into_values()
            .map(|(mut group_values, states)| {
                group_values.extend(states.iter().map(kv_finish_aggregate_state));
                group_values
            })
            .collect())
    }

    async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String>;
}

#[derive(Default)]
pub struct MemoryKvStore {
    inner: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryKvStore {
    pub fn new() -> Self {
        Self::default()
    }
}

struct MemoryTransaction {
    inner: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    snapshot: HashMap<Vec<u8>, Vec<u8>>,
    pending: RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    savepoints: RwLock<Vec<(String, BTreeMap<Vec<u8>, Option<Vec<u8>>>)>>,
}

impl MemoryTransaction {
    fn overlay_pending(
        &self,
        rows: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        in_range: impl Fn(&[u8]) -> bool,
    ) {
        if let Ok(pending) = self.pending.try_read() {
            for (key, value) in pending.iter() {
                if !in_range(key) {
                    continue;
                }
                match value {
                    Some(value) => {
                        rows.insert(key.clone(), value.clone());
                    }
                    None => {
                        rows.remove(key);
                    }
                }
            }
        }
    }

    fn collect_range(
        rows: BTreeMap<Vec<u8>, Vec<u8>>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let iter: Box<dyn Iterator<Item = _>> = if reverse {
            Box::new(rows.into_iter().rev())
        } else {
            Box::new(rows.into_iter())
        };
        match limit {
            Some(limit) => iter.take(limit).collect(),
            None => iter.collect(),
        }
    }

    async fn materialize_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = self
            .inner
            .read()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        self.overlay_pending(&mut rows, |key| key.starts_with(prefix));
        rows.into_iter().collect()
    }

    async fn materialize_snapshot_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = self
            .snapshot
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        self.overlay_pending(&mut rows, |key| key.starts_with(prefix));
        rows.into_iter().collect()
    }

    async fn materialize_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = self
            .inner
            .read()
            .await
            .iter()
            .filter(|(key, _)| {
                key.as_slice() >= start && end.is_none_or(|end| key.as_slice() < end)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        self.overlay_pending(&mut rows, |key| {
            key >= start && end.is_none_or(|end| key < end)
        });
        Self::collect_range(rows, limit, reverse)
    }

    async fn materialize_snapshot_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = self
            .snapshot
            .iter()
            .filter(|(key, _)| {
                key.as_slice() >= start && end.is_none_or(|end| key.as_slice() < end)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();

        self.overlay_pending(&mut rows, |key| {
            key >= start && end.is_none_or(|end| key < end)
        });
        Self::collect_range(rows, limit, reverse)
    }
}

#[async_trait]
impl KvStore for MemoryKvStore {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.inner
            .write()
            .await
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
        let mut guard = self.inner.write().await;
        for (key, value) in entries {
            guard.insert(key, value);
        }
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.inner.read().await.get(key).cloned())
    }

    async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, String> {
        let guard = self.inner.read().await;
        Ok(keys
            .iter()
            .map(|key| guard.get(key).cloned())
            .collect::<Vec<_>>())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.inner.write().await.remove(key);
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let guard = self.inner.read().await;
        let mut rows = guard
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let guard = self.inner.read().await;
        let mut rows = guard
            .iter()
            .filter(|(key, _)| {
                key.as_slice() >= start && end.is_none_or(|end| key.as_slice() < end)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        if reverse {
            rows.reverse();
        }
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
        let snapshot = self.inner.read().await.clone();
        Ok(Box::new(MemoryTransaction {
            inner: self.inner.clone(),
            snapshot,
            pending: RwLock::new(BTreeMap::new()),
            savepoints: RwLock::new(Vec::new()),
        }))
    }
}

#[async_trait]
impl KvTransaction for MemoryTransaction {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.pending
            .write()
            .await
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
        let mut pending = self.pending.write().await;
        for (key, value) in entries {
            pending.insert(key, Some(value));
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.pending.write().await.insert(key.to_vec(), None);
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some(value) = self.pending.read().await.get(key) {
            return Ok(value.clone());
        }
        Ok(self.inner.read().await.get(key).cloned())
    }

    async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some(value) = self.pending.read().await.get(key) {
            return Ok(value.clone());
        }
        Ok(self.snapshot.get(key).cloned())
    }

    async fn has_pending_key(&self, key: &[u8]) -> Result<bool, String> {
        Ok(self.pending.read().await.contains_key(key))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        Ok(self.materialize_prefix(prefix).await)
    }

    async fn snapshot_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        Ok(self.materialize_snapshot_prefix(prefix).await)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        Ok(self.materialize_range(start, end, limit, reverse).await)
    }

    async fn snapshot_scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        Ok(self
            .materialize_snapshot_range(start, end, limit, reverse)
            .await)
    }

    async fn commit(&self) -> Result<(), String> {
        let pending = self.pending.read().await;
        let mut guard = self.inner.write().await;
        for (key, value) in pending.iter() {
            match value {
                Some(value) => {
                    guard.insert(key.clone(), value.clone());
                }
                None => {
                    guard.remove(key);
                }
            }
        }
        Ok(())
    }

    async fn rollback(&self) -> Result<(), String> {
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<(), String> {
        let pending = self.pending.read().await.clone();
        self.savepoints
            .write()
            .await
            .push((name.to_ascii_lowercase(), pending));
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), String> {
        let name = name.to_ascii_lowercase();
        let mut savepoints = self.savepoints.write().await;
        let Some(pos) = savepoints.iter().rposition(|(saved, _)| saved == &name) else {
            return Err(format!("savepoint '{name}' does not exist"));
        };
        let snapshot = savepoints[pos].1.clone();
        savepoints.truncate(pos + 1);
        *self.pending.write().await = snapshot;
        Ok(())
    }

    async fn release_savepoint(&self, name: &str) -> Result<(), String> {
        let name = name.to_ascii_lowercase();
        let mut savepoints = self.savepoints.write().await;
        let Some(pos) = savepoints.iter().rposition(|(saved, _)| saved == &name) else {
            return Err(format!("savepoint '{name}' does not exist"));
        };
        savepoints.truncate(pos);
        Ok(())
    }
}

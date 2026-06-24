use std::collections::BTreeMap;
use std::time::Instant;

use async_trait::async_trait;
use kv_engine::db::{
    KeyOrder, KeyRange, ScanBudget, SchemalessRangeQuery, SchemalessWriteBatch, WriteOptions,
};

use super::profile::{KvEngineTransaction, log_copy_profile};
use crate::mem_store::KvTransaction;

#[async_trait]
impl KvTransaction for KvEngineTransaction {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.pending
            .lock()
            .map_err(|e| e.to_string())?
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
        let mut pending = self.pending.lock().map_err(|e| e.to_string())?;
        for (key, value) in entries {
            pending.insert(key, Some(value));
        }
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.pending
            .lock()
            .map_err(|e| e.to_string())?
            .insert(key.to_vec(), None);
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if let Some(value) = self
            .pending
            .lock()
            .map_err(|e| e.to_string())?
            .get(key)
            .cloned()
        {
            return Ok(value);
        }
        self.table
            .get_async(key)
            .await
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(|e| e.to_string())
    }

    async fn has_pending_key(&self, key: &[u8]) -> Result<bool, String> {
        Ok(self
            .pending
            .lock()
            .map_err(|e| e.to_string())?
            .contains_key(key))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let table = self.table.clone();
        let prefix = prefix.to_vec();
        let scan_prefix = prefix.clone();
        let mut rows = tokio::task::spawn_blocking(move || {
            let mut cursor = table
                .range_query(SchemalessRangeQuery {
                    scan_prefix: Some(scan_prefix),
                    ..SchemalessRangeQuery::default()
                })
                .map_err(|e| e.to_string())?;
            let mut rows = BTreeMap::new();
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                for record in batch.records {
                    if let (Some(key), Some(value)) = (record.key, record.value) {
                        rows.insert(key.to_vec(), value.to_vec());
                    }
                }
                if batch.exhausted {
                    break;
                }
            }
            Ok::<_, String>(rows)
        })
        .await
        .map_err(|e| e.to_string())??;
        for (key, value) in self.pending.lock().map_err(|e| e.to_string())?.iter() {
            if !key.starts_with(&prefix) {
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
        Ok(rows.into_iter().collect())
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let table = self.table.clone();
        let start = start.to_vec();
        let end = end.map(|end| end.to_vec());
        let mut rows = tokio::task::spawn_blocking({
            let start = start.clone();
            let end = end.clone();
            move || {
                let mut ctx = SchemalessRangeQuery {
                    bounds: KeyRange::new(Some(start), end),
                    order: if reverse {
                        KeyOrder::Desc
                    } else {
                        KeyOrder::Asc
                    },
                    ..SchemalessRangeQuery::default()
                };
                if let Some(limit) = limit {
                    ctx.budget = ScanBudget {
                        max_records_per_batch: limit.max(1),
                        ..ScanBudget::default()
                    };
                }
                let mut cursor = table.range_query(ctx).map_err(|e| e.to_string())?;
                let mut rows = BTreeMap::new();
                loop {
                    let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                    for record in batch.records {
                        if let (Some(key), Some(value)) = (record.key, record.value) {
                            rows.insert(key.to_vec(), value.to_vec());
                        }
                        if limit.is_some_and(|limit| rows.len() >= limit) {
                            break;
                        }
                    }
                    if batch.exhausted || limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
                Ok::<_, String>(rows)
            }
        })
        .await
        .map_err(|e| e.to_string())??;
        for (key, value) in self.pending.lock().map_err(|e| e.to_string())?.iter() {
            if key.as_slice() < start.as_slice()
                || end
                    .as_ref()
                    .is_some_and(|end| key.as_slice() >= end.as_slice())
            {
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
        let iter: Box<dyn Iterator<Item = _>> = if reverse {
            Box::new(rows.into_iter().rev())
        } else {
            Box::new(rows.into_iter())
        };
        Ok(match limit {
            Some(limit) => iter.take(limit).collect(),
            None => iter.collect(),
        })
    }

    async fn commit(&self) -> Result<(), String> {
        let pending = std::mem::take(&mut *self.pending.lock().map_err(|e| e.to_string())?);
        let staged_ops = pending.len();
        if pending.is_empty() {
            return Ok(());
        }
        let started_at = Instant::now();
        let mut batch = SchemalessWriteBatch::new();
        for (key, value) in pending {
            match value {
                Some(value) => batch.put(&key, &value).map_err(|e| e.to_string())?,
                None => batch.delete(&key).map_err(|e| e.to_string())?,
            }
        }
        let result = self
            .table
            .write_opt_async(&batch, &WriteOptions { sync: false })
            .await
            .map_err(|e| e.to_string());
        log_copy_profile(format!(
            "kv_txn.commit staged_ops={} elapsed_ms={} success={}",
            staged_ops,
            started_at.elapsed().as_millis(),
            result.is_ok()
        ));
        result
    }

    async fn rollback(&self) -> Result<(), String> {
        self.pending.lock().map_err(|e| e.to_string())?.clear();
        Ok(())
    }
}

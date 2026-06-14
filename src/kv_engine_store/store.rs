use super::*;

#[async_trait]
impl KvStore for KvEngineStore {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            db.put_opt(&key, &value, &WriteOptions { sync: false })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn put_batch(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::new();
            for (key, value) in entries {
                batch.put(&key, &value);
            }
            db.write_opt(&batch, &WriteOptions { sync: false })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn sync_wal(&self) -> Result<(), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.sync_wal().map_err(|e| e.to_string()))
            .await
            .map_err(|e| e.to_string())?
    }

    async fn rewrite_table_rows_to_fast_format(
        &self,
        schema: TableSchema,
        batch_size: usize,
    ) -> Result<(usize, usize), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let range = storage_layout::row_range(schema.table_id, schema.table_epoch, None);
            let mut cursor = RangeCursor::open(
                &db,
                RangeQueryContext {
                    bounds: RangeBounds::new(Some(range.start), range.end),
                    projection: RangeProjection::KeyValue,
                    ..RangeQueryContext::default()
                },
            )
            .map_err(|e| e.to_string())?;
            let mut scanned = 0usize;
            let mut rewritten = 0usize;
            let mut pending = Vec::<(Vec<u8>, Vec<u8>)>::with_capacity(batch_size.max(1));
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                for record in batch.records {
                    let (Some(key), Some(value)) = (record.key, record.value) else {
                        continue;
                    };
                    scanned = scanned.saturating_add(1);
                    if let Some(encoded) = storage_layout::reencode_row_record_fast(&schema, &value)
                        .map_err(|e| e.to_string())?
                    {
                        pending.push((key.to_vec(), encoded));
                        rewritten = rewritten.saturating_add(1);
                    }
                    if pending.len() >= batch_size.max(1) {
                        let mut write_batch = WriteBatch::new();
                        for (key, value) in pending.drain(..) {
                            write_batch.put(&key, &value);
                        }
                        db.write_opt(&write_batch, &WriteOptions { sync: false })
                            .map_err(|e| e.to_string())?;
                    }
                }
                if batch.exhausted {
                    break;
                }
            }
            if !pending.is_empty() {
                let mut write_batch = WriteBatch::new();
                for (key, value) in pending.drain(..) {
                    write_batch.put(&key, &value);
                }
                db.write_opt(&write_batch, &WriteOptions { sync: false })
                    .map_err(|e| e.to_string())?;
            }
            db.sync_wal().map_err(|e| e.to_string())?;
            Ok((scanned, rewritten))
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn compact_storage(&self) -> Result<(), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.run_manual_gc().map(|_| ()).map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn delete(&self, key: &[u8]) -> Result<(), String> {
        let db = self.db.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            db.delete_opt(&key, &WriteOptions { sync: false })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let db = self.db.clone();
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || {
            db.get(&key)
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.multi_get(&keys)
                .map(|values| {
                    values
                        .into_iter()
                        .map(|value| value.map(|bytes| bytes.to_vec()))
                        .collect::<Vec<_>>()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let db = self.db.clone();
        let prefix = prefix.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut cursor = RangeCursor::open(
                &db,
                RangeQueryContext {
                    bounds: RangeBounds::prefix(&prefix),
                    ..RangeQueryContext::default()
                },
            )
            .map_err(|e| e.to_string())?;
            let mut rows = Vec::new();
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                for record in batch.records {
                    if let (Some(key), Some(value)) = (record.key, record.value) {
                        rows.push((key.to_vec(), value.to_vec()));
                    }
                }
                if batch.exhausted {
                    break;
                }
            }
            Ok::<_, String>(rows)
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let db = self.db.clone();
        let start = start.to_vec();
        let end = end.map(|end| end.to_vec());
        tokio::task::spawn_blocking(move || {
            let mut ctx = RangeQueryContext {
                bounds: RangeBounds::new(Some(start), end),
                ..RangeQueryContext::default()
            };
            if reverse {
                ctx.direction = RangeDirection::Reverse;
            }
            if let Some(limit) = limit {
                ctx.budget = ScanBudget {
                    max_records_per_batch: limit.max(1),
                    ..ScanBudget::default()
                };
            }
            let mut cursor = RangeCursor::open(&db, ctx).map_err(|e| e.to_string())?;
            let mut rows = Vec::new();
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                for record in batch.records {
                    if let (Some(key), Some(value)) = (record.key, record.value) {
                        rows.push((key.to_vec(), value.to_vec()));
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
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn scan_range_projected(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
        projection: KvScanProjection,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, String> {
        use kv_engine::db::RangeProjection;

        let db = self.db.clone();
        let start = start.to_vec();
        let end = end.map(|end| end.to_vec());
        tokio::task::spawn_blocking(move || {
            let ctx = RangeQueryContext {
                bounds: RangeBounds::new(Some(start), end),
                projection: match projection {
                    KvScanProjection::KeyOnly => RangeProjection::KeyOnly,
                    KvScanProjection::KeyValue => RangeProjection::KeyValue,
                },
                direction: if reverse {
                    RangeDirection::Reverse
                } else {
                    RangeDirection::Forward
                },
                budget: ScanBudget {
                    max_records_per_batch: limit.unwrap_or(65_536).max(1),
                    max_bytes_per_batch: 8 * 1024 * 1024,
                    ..ScanBudget::default()
                },
                ..RangeQueryContext::default()
            };
            let mut cursor = RangeCursor::open(&db, ctx).map_err(|e| e.to_string())?;
            let mut rows = Vec::new();
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                for record in batch.records {
                    if let Some(key) = record.key {
                        rows.push((key.to_vec(), record.value.map(|value| value.to_vec())));
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
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn visit_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        reverse: bool,
        projection: KvScanProjection,
        visitor: KvRangeVisitor,
    ) -> Result<(), String> {
        let db = self.db.clone();
        let start = start.to_vec();
        let end = end.map(|end| end.to_vec());
        tokio::task::spawn_blocking(move || {
            let ctx = RangeQueryContext {
                bounds: RangeBounds::new(Some(start), end),
                projection: match projection {
                    KvScanProjection::KeyOnly => RangeProjection::KeyOnly,
                    KvScanProjection::KeyValue => RangeProjection::KeyValue,
                },
                direction: if reverse {
                    RangeDirection::Reverse
                } else {
                    RangeDirection::Forward
                },
                budget: ScanBudget {
                    max_records_per_batch: 65_536,
                    max_bytes_per_batch: 8 * 1024 * 1024,
                    ..ScanBudget::default()
                },
                ..RangeQueryContext::default()
            };
            let mut cursor = RangeCursor::open(&db, ctx).map_err(|e| e.to_string())?;
            let mut visitor_error = None;
            let mut visitor = visitor.lock().map_err(|e| e.to_string())?;
            cursor
                .scan_ref(&mut |key, value, _seq| match visitor(key, Some(value)) {
                    Ok(should_continue) => should_continue,
                    Err(error) => {
                        visitor_error = Some(error);
                        false
                    }
                })
                .map_err(|e| e.to_string())?;
            if let Some(error) = visitor_error {
                return Err(error);
            }
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn count_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<u64, String> {
        let db = self.db.clone();
        let start = start.to_vec();
        let end = end.map(|end| end.to_vec());
        tokio::task::spawn_blocking(move || {
            let mut cursor = RangeCursor::open(
                &db,
                RangeQueryContext {
                    bounds: RangeBounds::new(Some(start), end),
                    projection: RangeProjection::KeyOnly,
                    budget: ScanBudget {
                        max_records_per_batch: 65_536,
                        max_bytes_per_batch: 8 * 1024 * 1024,
                        ..ScanBudget::default()
                    },
                    ..RangeQueryContext::default()
                },
            )
            .map_err(|e| e.to_string())?;
            let mut count = 0u64;
            loop {
                let batch = cursor.next_batch().map_err(|e| e.to_string())?;
                count = count.saturating_add(batch.records.len() as u64);
                if batch.exhausted {
                    break;
                }
            }
            Ok(count)
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn aggregate_scan(&self, plan: KvAggregateScan) -> Result<Vec<Vec<ColumnValue>>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || execute_aggregate_scan(&db, plan))
            .await
            .map_err(|e| e.to_string())?
    }

    async fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
        Ok(Box::new(KvEngineTransaction {
            db: self.db.clone(),
            pending: Mutex::new(BTreeMap::new()),
        }))
    }
}

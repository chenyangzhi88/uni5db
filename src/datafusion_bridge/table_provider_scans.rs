use std::sync::{Arc, Mutex, atomic::Ordering as AtomicOrdering};
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{Expr as DfExpr, Operator};

use crate::codec::{cell_key, decode_cell_value, decode_pk_from_row_marker_key, row_marker_prefix};
use crate::mem_store::{KvAggregateScan, KvRangeVisitor, KvScanProjection};
use crate::storage_layout;
use crate::types::{ColumnValue, RowMap};

use super::{
    FastTopNCandidate, FastTopNScanPlan, KvScanPlan, KvTableProvider, KvTopNPlan, TopNCandidate,
    TopNScanProfile, compare_column_values, elapsed_ns_u64, reverse_operator,
    scalar_value_to_column_value, scan_profile_enabled,
};

impl KvTableProvider {
    pub(super) async fn execute_topn_scan(&self, plan: &KvTopNPlan) -> DfResult<RecordBatch> {
        if plan.primary_key_ordered {
            return self.execute_primary_key_ordered_topn_scan(plan).await;
        }
        if let Some(fast_plan) = self.compile_fast_topn_scan_plan(plan) {
            return self.execute_topn_fast_scan(plan, fast_plan).await;
        }

        let started_at = Instant::now();
        let profile_enabled = scan_profile_enabled();
        let profile = Arc::new(TopNScanProfile::default());
        let window = plan.fetch.saturating_add(plan.skip);
        let state = Arc::new(Mutex::new(Vec::<TopNCandidate>::with_capacity(
            window.min(1024),
        )));
        let visitor_state = Arc::clone(&state);
        let visitor_profile = Arc::clone(&profile);
        let provider = Arc::new(self.clone_for_pushdown());
        let plan_for_visit = plan.clone();
        let range = storage_layout::row_range(self.schema.table_id, self.schema.table_epoch, None);
        let projector = storage_layout::RowValueProjector::new(&self.schema, &plan.scan_indices);
        let mut scratch_values = Vec::with_capacity(plan.scan_indices.len());
        let visitor: KvRangeVisitor =
            Arc::new(Mutex::new(move |_key: &[u8], value: Option<&[u8]>| {
                if profile_enabled {
                    visitor_profile
                        .records
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
                let value = value.ok_or_else(|| "top-n visitor missing row value".to_string())?;
                let decode_started_at = profile_enabled.then(Instant::now);
                storage_layout::decode_row_record_projected_values_with(
                    &projector,
                    value,
                    &mut scratch_values,
                )
                .map_err(|e| e.to_string())?;
                if let Some(started_at) = decode_started_at {
                    visitor_profile
                        .decode_ns
                        .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                }
                let filter_started_at = profile_enabled.then(Instant::now);
                let matched = provider.kv_filters_match_values(
                    &scratch_values,
                    &plan_for_visit.scan_positions,
                    &plan_for_visit.kv_filters,
                );
                if let Some(started_at) = filter_started_at {
                    visitor_profile
                        .filter_ns
                        .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                }
                if matched {
                    if profile_enabled {
                        visitor_profile
                            .matched
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    let order_value = provider.scan_value(
                        &scratch_values,
                        &plan_for_visit.scan_positions,
                        plan_for_visit.order_idx,
                    );
                    let pk_value = plan_for_visit
                        .primary_key_idx
                        .map(|idx| {
                            provider.scan_value(
                                &scratch_values,
                                &plan_for_visit.scan_positions,
                                idx,
                            )
                        })
                        .unwrap_or(ColumnValue::Null);
                    let candidate = TopNCandidate {
                        values: if plan_for_visit.refetch_output {
                            Vec::new()
                        } else {
                            scratch_values.clone()
                        },
                        order_value,
                        pk_value,
                    };
                    let candidate_started_at = profile_enabled.then(Instant::now);
                    let mut candidates = visitor_state.lock().map_err(|e| e.to_string())?;
                    KvTableProvider::push_topn_candidate(
                        &mut candidates,
                        candidate,
                        plan_for_visit.descending,
                        plan_for_visit.nulls_first,
                        window,
                    );
                    if let Some(started_at) = candidate_started_at {
                        visitor_profile
                            .candidate_ns
                            .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                    }
                }
                Ok(true)
            }));
        self.store
            .visit_range(
                &range.start,
                range.end.as_deref(),
                range.reverse,
                KvScanProjection::KeyValue,
                visitor,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        let candidates = {
            let mut candidates = state
                .lock()
                .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))?;
            let mut candidates = std::mem::take(&mut *candidates);
            candidates.sort_by(|left, right| {
                Self::compare_topn_candidates(left, right, plan.descending, plan.nulls_first)
            });
            candidates
                .into_iter()
                .skip(plan.skip)
                .take(plan.fetch)
                .collect::<Vec<_>>()
        };
        let output_started_at = profile_enabled.then(Instant::now);
        let output_rows = if plan.refetch_output {
            let keys = candidates
                .iter()
                .map(|candidate| {
                    if candidate.pk_value.is_null() {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "KvTopN missing primary key column '{}'",
                            self.schema.primary_key
                        )));
                    }
                    Ok(storage_layout::row_key(
                        self.schema.table_id,
                        self.schema.table_epoch,
                        &candidate.pk_value,
                    ))
                })
                .collect::<DfResult<Vec<_>>>()?;
            let values = self
                .store
                .multi_get(keys)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
            values
                .into_iter()
                .map(|value| {
                    let value = value.ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "KvTopN refetch missing row value".to_string(),
                        )
                    })?;
                    storage_layout::decode_row_record_projected(
                        &self.schema,
                        &value,
                        &plan.output_indices,
                    )
                    .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))
                })
                .collect::<DfResult<Vec<_>>>()?
        } else {
            candidates
                .into_iter()
                .map(|candidate| self.projected_values_to_row(&plan.scan_indices, candidate.values))
                .collect::<Vec<_>>()
        };
        let output_decode_ms = output_started_at
            .map(|started_at| started_at.elapsed().as_millis())
            .unwrap_or(0);
        let mut batches = self.projected_batches_from_rows(
            output_rows,
            &plan.output_indices,
            plan.output_schema.clone(),
        )?;
        if profile_enabled {
            log::info!(
                "kv_topn.scan table={} records={} matched={} output_rows={} elapsed_ms={} decode_ms={} filter_ms={} candidate_ms={} output_decode_ms={} scan_cols={} output_cols={} refetch_output={} filters={} fetch={} skip={}",
                self.schema.table_name,
                profile.records.load(AtomicOrdering::Relaxed),
                profile.matched.load(AtomicOrdering::Relaxed),
                batches.first().map(|batch| batch.num_rows()).unwrap_or(0),
                started_at.elapsed().as_millis(),
                profile.decode_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                profile.filter_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                profile.candidate_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                output_decode_ms,
                plan.scan_indices.len(),
                plan.output_indices.len(),
                plan.refetch_output,
                plan.filters.len(),
                plan.fetch,
                plan.skip,
            );
        }
        Ok(batches
            .pop()
            .unwrap_or_else(|| RecordBatch::new_empty(plan.output_schema.clone())))
    }

    pub(super) async fn execute_topn_fast_scan(
        &self,
        plan: &KvTopNPlan,
        fast_plan: FastTopNScanPlan,
    ) -> DfResult<RecordBatch> {
        let started_at = Instant::now();
        let profile_enabled = scan_profile_enabled();
        let profile = Arc::new(TopNScanProfile::default());
        let window = plan.fetch.saturating_add(plan.skip);
        let state = Arc::new(Mutex::new(Vec::<FastTopNCandidate>::with_capacity(
            window.min(1024),
        )));
        let visitor_state = Arc::clone(&state);
        let visitor_profile = Arc::clone(&profile);
        let plan_for_visit = plan.clone();
        let range = storage_layout::row_range(self.schema.table_id, self.schema.table_epoch, None);
        let mut scratch_values =
            vec![storage_layout::FastNumericValue::Null; fast_plan.projector.output_len()];
        let visitor: KvRangeVisitor =
            Arc::new(Mutex::new(move |_key: &[u8], value: Option<&[u8]>| {
                if profile_enabled {
                    visitor_profile
                        .records
                        .fetch_add(1, AtomicOrdering::Relaxed);
                }
                let value =
                    value.ok_or_else(|| "top-n fast visitor missing row value".to_string())?;
                let decode_started_at = profile_enabled.then(Instant::now);
                storage_layout::decode_row_record_fast_numeric_slots_with(
                    &fast_plan.projector,
                    value,
                    &mut scratch_values,
                    &fast_plan.filter_slots,
                )
                .map_err(|e| e.to_string())?;
                if let Some(started_at) = decode_started_at {
                    visitor_profile
                        .decode_ns
                        .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                }
                let filter_started_at = profile_enabled.then(Instant::now);
                let matched =
                    KvTableProvider::fast_predicate_matches(&fast_plan.filter, &scratch_values);
                if let Some(started_at) = filter_started_at {
                    visitor_profile
                        .filter_ns
                        .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                }
                if matched {
                    if profile_enabled {
                        visitor_profile
                            .matched
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    let decode_started_at = profile_enabled.then(Instant::now);
                    storage_layout::decode_row_record_fast_numeric_slots_with(
                        &fast_plan.projector,
                        value,
                        &mut scratch_values,
                        &fast_plan.candidate_slots,
                    )
                    .map_err(|e| e.to_string())?;
                    if let Some(started_at) = decode_started_at {
                        visitor_profile
                            .decode_ns
                            .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                    }
                    let order_value = scratch_values
                        .get(fast_plan.order_slot)
                        .copied()
                        .unwrap_or(storage_layout::FastNumericValue::Null);
                    let pk_value = fast_plan
                        .pk_slot
                        .and_then(|slot| scratch_values.get(slot).copied())
                        .unwrap_or(storage_layout::FastNumericValue::Null);
                    let candidate_started_at = profile_enabled.then(Instant::now);
                    let mut candidates = visitor_state.lock().map_err(|e| e.to_string())?;
                    KvTableProvider::push_fast_topn_candidate(
                        &mut candidates,
                        value,
                        order_value,
                        pk_value,
                        plan_for_visit.descending,
                        plan_for_visit.nulls_first,
                        window,
                    );
                    if let Some(started_at) = candidate_started_at {
                        visitor_profile
                            .candidate_ns
                            .fetch_add(elapsed_ns_u64(started_at), AtomicOrdering::Relaxed);
                    }
                }
                Ok(true)
            }));
        self.store
            .visit_range(
                &range.start,
                range.end.as_deref(),
                range.reverse,
                KvScanProjection::KeyValue,
                visitor,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        let candidates = {
            let mut candidates = state
                .lock()
                .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))?;
            let mut candidates = std::mem::take(&mut *candidates);
            candidates.sort_by(|left, right| {
                Self::compare_fast_topn_candidates(left, right, plan.descending, plan.nulls_first)
            });
            candidates
                .into_iter()
                .skip(plan.skip)
                .take(plan.fetch)
                .collect::<Vec<_>>()
        };
        let output_started_at = profile_enabled.then(Instant::now);
        let output_rows = candidates
            .into_iter()
            .map(|candidate| {
                storage_layout::decode_row_record_projected(
                    &self.schema,
                    &candidate.raw_value,
                    &plan.output_indices,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let output_decode_ms = output_started_at
            .map(|started_at| started_at.elapsed().as_millis())
            .unwrap_or(0);
        let mut batches = self.projected_batches_from_rows(
            output_rows,
            &plan.output_indices,
            plan.output_schema.clone(),
        )?;
        if profile_enabled {
            log::info!(
                "kv_topn.fast_scan table={} records={} matched={} output_rows={} elapsed_ms={} decode_ms={} filter_ms={} candidate_ms={} output_decode_ms={} output_cols={} filters={} fetch={} skip={}",
                self.schema.table_name,
                profile.records.load(AtomicOrdering::Relaxed),
                profile.matched.load(AtomicOrdering::Relaxed),
                batches.first().map(|batch| batch.num_rows()).unwrap_or(0),
                started_at.elapsed().as_millis(),
                profile.decode_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                profile.filter_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                profile.candidate_ns.load(AtomicOrdering::Relaxed) / 1_000_000,
                output_decode_ms,
                plan.output_indices.len(),
                plan.filters.len(),
                plan.fetch,
                plan.skip,
            );
        }
        Ok(batches
            .pop()
            .unwrap_or_else(|| RecordBatch::new_empty(plan.output_schema.clone())))
    }

    pub(super) async fn execute_primary_key_ordered_topn_scan(
        &self,
        plan: &KvTopNPlan,
    ) -> DfResult<RecordBatch> {
        let window = plan.fetch.saturating_add(plan.skip);
        if window == 0 {
            return Ok(RecordBatch::new_empty(plan.output_schema.clone()));
        }

        let state = Arc::new(Mutex::new(Vec::<RowMap>::with_capacity(window.min(1024))));
        let visitor_state = Arc::clone(&state);
        let provider = Arc::new(self.clone_for_pushdown());
        let plan_for_visit = plan.clone();
        let mut range =
            storage_layout::row_range(self.schema.table_id, self.schema.table_epoch, None);
        range.reverse = plan.descending;
        let visitor: KvRangeVisitor =
            Arc::new(Mutex::new(move |_key: &[u8], value: Option<&[u8]>| {
                let value = value
                    .ok_or_else(|| "top-n primary key visitor missing row value".to_string())?;
                let output_row = storage_layout::decode_row_record_projected(
                    &provider.schema,
                    value,
                    &plan_for_visit.output_indices,
                )
                .map_err(|e| e.to_string())?;
                if !plan_for_visit.kv_filters.is_empty() {
                    let scan_values = storage_layout::decode_row_record_projected_values(
                        &provider.schema,
                        value,
                        &plan_for_visit.scan_indices,
                    )
                    .map_err(|e| e.to_string())?;
                    if !provider.kv_filters_match_values(
                        &scan_values,
                        &plan_for_visit.scan_positions,
                        &plan_for_visit.kv_filters,
                    ) {
                        return Ok(true);
                    }
                }
                let mut rows = visitor_state.lock().map_err(|e| e.to_string())?;
                rows.push(output_row);
                Ok(rows.len() < window)
            }));
        self.store
            .visit_range(
                &range.start,
                range.end.as_deref(),
                range.reverse,
                KvScanProjection::KeyValue,
                visitor,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        let output_rows = {
            let mut rows = state
                .lock()
                .map_err(|e| datafusion::error::DataFusionError::External(e.to_string().into()))?;
            std::mem::take(&mut *rows)
                .into_iter()
                .skip(plan.skip)
                .take(plan.fetch)
                .collect::<Vec<_>>()
        };
        let mut batches = self.projected_batches_from_rows(
            output_rows,
            &plan.output_indices,
            plan.output_schema.clone(),
        )?;
        Ok(batches
            .pop()
            .unwrap_or_else(|| RecordBatch::new_empty(plan.output_schema.clone())))
    }

    pub(super) async fn load_rows_with_columns(
        &self,
        needed_indices: &[usize],
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Vec<RowMap>> {
        let range = storage_layout::row_range(self.schema.table_id, self.schema.table_epoch, None);
        let v2_rows = self
            .store
            .scan_range(
                &range.start,
                range.end.as_deref(),
                range.limit,
                range.reverse,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        if !v2_rows.is_empty() {
            return self
                .load_rows_from_v2(v2_rows, needed_indices, filters, limit)
                .await;
        }

        let chunk_range =
            storage_layout::olap_chunk_meta_range(self.schema.table_id, self.schema.table_epoch);
        let chunk_meta = self
            .store
            .scan_range(
                &chunk_range.start,
                chunk_range.end.as_deref(),
                chunk_range.limit,
                chunk_range.reverse,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
        if !chunk_meta.is_empty() {
            return self
                .load_rows_from_chunks(chunk_meta, needed_indices, filters, limit)
                .await;
        }

        let marker_prefix = row_marker_prefix(
            &self.database_name,
            &self.schema_name,
            &self.schema.table_name,
        );
        let markers = self
            .store
            .scan_prefix(&marker_prefix)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let mut rows = Vec::new();
        for (key, _) in &markers {
            let pk_value = decode_pk_from_row_marker_key(
                key,
                &self.database_name,
                &self.schema_name,
                &self.schema.table_name,
                self.schema.pk_data_type(),
            )
            .map_err(|e| datafusion::error::DataFusionError::External(format!("{e}").into()))?;
            let mut row = RowMap::new();
            for idx in needed_indices {
                let col = &self.schema.columns[*idx];
                let cell = self
                    .store
                    .get(&cell_key(
                        &self.database_name,
                        &self.schema_name,
                        &self.schema.table_name,
                        &col.name,
                        &pk_value,
                    ))
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
                let value = match cell {
                    Some(bytes) => decode_cell_value(&col.data_type, &bytes).map_err(|e| {
                        datafusion::error::DataFusionError::External(format!("{e}").into())
                    })?,
                    None => ColumnValue::Null,
                };
                row.insert(col.name.clone(), value);
            }
            if self.row_matches_datafusion_filters(&row, filters) {
                rows.push(row);
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn execute_kv_aggregate_scan(
        &self,
        plan: &KvScanPlan,
    ) -> DfResult<Vec<Vec<ColumnValue>>> {
        self.store
            .aggregate_scan(KvAggregateScan {
                schema: self.schema.clone(),
                range_start: plan.range_start.clone(),
                range_end: plan.range_end.clone(),
                scan_prefix: plan.scan_prefix.clone(),
                filters: plan.filters.clone(),
                group_indices: plan.group_indices.clone(),
                aggregates: plan
                    .aggregates
                    .iter()
                    .map(|aggregate| aggregate.op.clone())
                    .collect(),
                required_indices: plan.required_indices.clone(),
                projection: plan.projection,
            })
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))
    }

    pub(super) async fn load_rows_from_chunks(
        &self,
        chunk_meta: Vec<(Vec<u8>, Vec<u8>)>,
        needed_indices: &[usize],
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Vec<RowMap>> {
        let mut rows = Vec::new();
        for (_, meta_bytes) in chunk_meta {
            let meta = storage_layout::decode_olap_chunk_meta(&meta_bytes)
                .map_err(|e| datafusion::error::DataFusionError::External(format!("{e}").into()))?;
            if !self.chunk_may_match_filters(&meta, filters) {
                continue;
            }
            let mut chunk_columns = Vec::with_capacity(needed_indices.len());
            for idx in needed_indices {
                let column = &self.schema.columns[*idx];
                let key = storage_layout::olap_chunk_column_key(
                    self.schema.table_id,
                    self.schema.table_epoch,
                    meta.chunk_id,
                    column.column_id,
                );
                let bytes = self
                    .store
                    .get(&key)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::External(
                            format!(
                                "missing OLAP column chunk table={} chunk={} column={}",
                                self.schema.table_name, meta.chunk_id, column.name
                            )
                            .into(),
                        )
                    })?;
                let values = storage_layout::decode_olap_column_chunk(&bytes).map_err(|e| {
                    datafusion::error::DataFusionError::External(format!("{e}").into())
                })?;
                chunk_columns.push((column.name.clone(), values));
            }
            for row_idx in 0..meta.row_count as usize {
                let mut row = RowMap::new();
                for (column_name, values) in &chunk_columns {
                    row.insert(
                        column_name.clone(),
                        values.get(row_idx).cloned().unwrap_or(ColumnValue::Null),
                    );
                }
                if self.row_matches_datafusion_filters(&row, filters) {
                    rows.push(row);
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(rows);
                    }
                }
            }
        }
        Ok(rows)
    }

    pub(super) async fn load_rows_from_v2(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        needed_indices: &[usize],
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DfResult<Vec<RowMap>> {
        let mut output_rows = Vec::new();
        let pk_only = self.can_project_from_primary_key(needed_indices, filters);
        for (key, value) in rows {
            let row = if pk_only {
                let pk_value = storage_layout::decode_pk_from_row_key(
                    &key,
                    self.schema.table_id,
                    self.schema.table_epoch,
                    self.schema.pk_data_type(),
                )
                .map_err(|e| datafusion::error::DataFusionError::External(format!("{e}").into()))?;
                self.projected_primary_key_row(needed_indices, pk_value)
            } else {
                storage_layout::decode_row_record(&self.schema, &value).map_err(|e| {
                    datafusion::error::DataFusionError::External(format!("{e}").into())
                })?
            };
            if self.row_matches_datafusion_filters(&row, filters) {
                output_rows.push(row);
                if limit.is_some_and(|limit| output_rows.len() >= limit) {
                    break;
                }
            }
        }
        Ok(output_rows)
    }

    pub(super) fn filter_supported(&self, expr: &DfExpr) -> bool {
        match expr {
            DfExpr::Column(column) => self
                .schema
                .columns
                .iter()
                .any(|schema_column| schema_column.name == column.name),
            DfExpr::Literal(..) => true,
            DfExpr::BinaryExpr(binary) => match binary.op {
                Operator::And | Operator::Or => {
                    self.filter_supported(&binary.left) && self.filter_supported(&binary.right)
                }
                Operator::Eq
                | Operator::NotEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Lt
                | Operator::LtEq => {
                    (matches!(binary.left.as_ref(), DfExpr::Column(_))
                        && matches!(binary.right.as_ref(), DfExpr::Literal(..)))
                        || (matches!(binary.right.as_ref(), DfExpr::Column(_))
                            && matches!(binary.left.as_ref(), DfExpr::Literal(..)))
                }
                _ => false,
            },
            DfExpr::Not(inner)
            | DfExpr::IsNull(inner)
            | DfExpr::IsNotNull(inner)
            | DfExpr::IsTrue(inner)
            | DfExpr::IsFalse(inner)
            | DfExpr::IsNotTrue(inner)
            | DfExpr::IsNotFalse(inner) => self.filter_supported(inner),
            DfExpr::Between(between) => {
                matches!(between.expr.as_ref(), DfExpr::Column(_))
                    && matches!(between.low.as_ref(), DfExpr::Literal(..))
                    && matches!(between.high.as_ref(), DfExpr::Literal(..))
            }
            DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
            | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
                self.filter_supported(inner)
            }
            _ => false,
        }
    }

    pub(super) fn row_matches_datafusion_filters(&self, row: &RowMap, filters: &[DfExpr]) -> bool {
        filters
            .iter()
            .all(|filter| self.eval_filter_bool(row, filter).unwrap_or(false))
    }

    pub(super) fn eval_filter_bool(&self, row: &RowMap, expr: &DfExpr) -> Option<bool> {
        match expr {
            DfExpr::Column(column) => match row.get(&column.name)? {
                ColumnValue::Boolean(value) => Some(*value),
                ColumnValue::Null => None,
                _ => None,
            },
            DfExpr::Literal(value, _) => match scalar_value_to_column_value(value)? {
                ColumnValue::Boolean(value) => Some(value),
                ColumnValue::Null => None,
                _ => None,
            },
            DfExpr::BinaryExpr(binary) => match binary.op {
                Operator::And => {
                    match (
                        self.eval_filter_bool(row, &binary.left),
                        self.eval_filter_bool(row, &binary.right),
                    ) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }
                }
                Operator::Or => {
                    match (
                        self.eval_filter_bool(row, &binary.left),
                        self.eval_filter_bool(row, &binary.right),
                    ) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }
                }
                Operator::Eq
                | Operator::NotEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Lt
                | Operator::LtEq => {
                    let left = self.eval_filter_value(row, &binary.left)?;
                    let right = self.eval_filter_value(row, &binary.right)?;
                    if left.is_null() || right.is_null() {
                        return None;
                    }
                    Some(compare_column_values(&left, &binary.op, &right))
                }
                _ => None,
            },
            DfExpr::Not(inner) => Some(!self.eval_filter_bool(row, inner)?),
            DfExpr::IsNull(inner) => Some(self.eval_filter_value(row, inner)?.is_null()),
            DfExpr::IsNotNull(inner) => Some(!self.eval_filter_value(row, inner)?.is_null()),
            DfExpr::IsTrue(inner) => Some(matches!(
                self.eval_filter_value(row, inner)?,
                ColumnValue::Boolean(true)
            )),
            DfExpr::IsFalse(inner) => Some(matches!(
                self.eval_filter_value(row, inner)?,
                ColumnValue::Boolean(false)
            )),
            DfExpr::IsNotTrue(inner) => Some(!matches!(
                self.eval_filter_value(row, inner)?,
                ColumnValue::Boolean(true)
            )),
            DfExpr::IsNotFalse(inner) => Some(!matches!(
                self.eval_filter_value(row, inner)?,
                ColumnValue::Boolean(false)
            )),
            DfExpr::Between(between) => {
                let value = self.eval_filter_value(row, &between.expr)?;
                let low = self.eval_filter_value(row, &between.low)?;
                let high = self.eval_filter_value(row, &between.high)?;
                if value.is_null() || low.is_null() || high.is_null() {
                    return None;
                }
                let inside = compare_column_values(&value, &Operator::GtEq, &low)
                    && compare_column_values(&value, &Operator::LtEq, &high);
                Some(if between.negated { !inside } else { inside })
            }
            DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
            | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
                self.eval_filter_bool(row, inner)
            }
            _ => None,
        }
    }

    pub(super) fn eval_filter_value(&self, row: &RowMap, expr: &DfExpr) -> Option<ColumnValue> {
        match expr {
            DfExpr::Column(column) => row.get(&column.name).cloned(),
            DfExpr::Literal(value, _) => scalar_value_to_column_value(value),
            DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
            | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
                self.eval_filter_value(row, inner)
            }
            _ => None,
        }
    }

    pub(super) fn chunk_may_match_filters(
        &self,
        meta: &storage_layout::OlapChunkMeta,
        filters: &[DfExpr],
    ) -> bool {
        filters
            .iter()
            .all(|filter| self.chunk_may_match_filter(meta, filter))
    }

    pub(super) fn chunk_may_match_filter(
        &self,
        meta: &storage_layout::OlapChunkMeta,
        filter: &DfExpr,
    ) -> bool {
        match filter {
            DfExpr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.chunk_may_match_filter(meta, &binary.left)
                    && self.chunk_may_match_filter(meta, &binary.right)
            }
            DfExpr::BinaryExpr(binary) if binary.op == Operator::Or => {
                self.chunk_may_match_filter(meta, &binary.left)
                    || self.chunk_may_match_filter(meta, &binary.right)
            }
            DfExpr::BinaryExpr(binary) => {
                self.chunk_may_match_binary_filter(meta, &binary.left, &binary.op, &binary.right)
            }
            DfExpr::Between(between) => {
                let Some((column_name, value)) =
                    self.literal_column_pair(&between.expr, &between.low)
                else {
                    return true;
                };
                if self.chunk_may_match_column_compare(meta, &column_name, &Operator::GtEq, &value)
                {
                    let Some((column_name, value)) =
                        self.literal_column_pair(&between.expr, &between.high)
                    else {
                        return true;
                    };
                    self.chunk_may_match_column_compare(meta, &column_name, &Operator::LtEq, &value)
                } else {
                    between.negated
                }
            }
            _ => true,
        }
    }

    pub(super) fn chunk_may_match_binary_filter(
        &self,
        meta: &storage_layout::OlapChunkMeta,
        left: &DfExpr,
        op: &Operator,
        right: &DfExpr,
    ) -> bool {
        if let Some((column_name, value)) = self.literal_column_pair(left, right) {
            return self.chunk_may_match_column_compare(meta, &column_name, op, &value);
        }
        if let Some((column_name, value)) = self.literal_column_pair(right, left) {
            return self.chunk_may_match_column_compare(
                meta,
                &column_name,
                &reverse_operator(op),
                &value,
            );
        }
        true
    }

    pub(super) fn literal_column_pair(
        &self,
        column_expr: &DfExpr,
        literal_expr: &DfExpr,
    ) -> Option<(String, ColumnValue)> {
        let DfExpr::Column(column) = column_expr else {
            return None;
        };
        let DfExpr::Literal(value, _) = literal_expr else {
            return None;
        };
        scalar_value_to_column_value(value).map(|value| (column.name.clone(), value))
    }

    pub(super) fn chunk_may_match_column_compare(
        &self,
        meta: &storage_layout::OlapChunkMeta,
        column_name: &str,
        op: &Operator,
        value: &ColumnValue,
    ) -> bool {
        let Some(column) = self.schema.find_column(column_name) else {
            return true;
        };
        let Some(zone) = meta
            .zones
            .iter()
            .find(|zone| zone.column_id == column.column_id)
        else {
            return true;
        };
        match op {
            Operator::Eq => {
                zone.min
                    .as_ref()
                    .is_none_or(|min| value.partial_cmp(min).is_some_and(|ord| !ord.is_lt()))
                    && zone
                        .max
                        .as_ref()
                        .is_none_or(|max| value.partial_cmp(max).is_some_and(|ord| !ord.is_gt()))
            }
            Operator::NotEq => true,
            Operator::Gt => zone
                .max
                .as_ref()
                .is_none_or(|max| max.partial_cmp(value).is_some_and(|ord| ord.is_gt())),
            Operator::GtEq => zone
                .max
                .as_ref()
                .is_none_or(|max| max.partial_cmp(value).is_some_and(|ord| !ord.is_lt())),
            Operator::Lt => zone
                .min
                .as_ref()
                .is_none_or(|min| min.partial_cmp(value).is_some_and(|ord| ord.is_lt())),
            Operator::LtEq => zone
                .min
                .as_ref()
                .is_none_or(|min| min.partial_cmp(value).is_some_and(|ord| !ord.is_gt())),
            _ => true,
        }
    }
}

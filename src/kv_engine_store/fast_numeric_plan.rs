use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use kv_engine::db::RangeCursor;

use super::fast_numeric_eval::{
    apply_fast_aggregates, fast_aggregate_initial_state, fast_aggregate_predicate_matches,
    fast_numeric_group_key, fast_numeric_group_values, finish_fast_aggregates,
    finish_fast_grouped_aggregate_row, initial_fast_aggregate_states,
};
use super::profile::{
    AggregateScanProfile, FastNumericAggregatePlan, FastNumericGroupMap, copy_profile_enabled,
    log_copy_profile, nanos_to_millis,
};
use super::projected_aggregate::{
    apply_projected_aggregate, projected_row_matches, projected_values_for_aggregate_record,
};
use crate::mem_store::{
    KvAggregateOp, KvAggregateScan, KvCompareOp, KvPredicate, KvScanProjection,
    kv_aggregate_initial_state, kv_finish_aggregate_state,
};
use crate::storage_layout;
use crate::types::ColumnValue;

pub(super) fn build_fast_numeric_aggregate_plan(
    plan: &KvAggregateScan,
) -> Option<FastNumericAggregatePlan> {
    if matches!(plan.projection, KvScanProjection::KeyOnly) {
        return None;
    }
    for idx in &plan.required_indices {
        let column = plan.schema.columns.get(*idx)?;
        if !fast_numeric_data_type(&column.data_type) {
            return None;
        }
    }
    let mut column_slots = vec![None; plan.schema.columns.len()];
    for (slot, column_idx) in plan.required_indices.iter().enumerate() {
        if let Some(entry) = column_slots.get_mut(*column_idx) {
            *entry = Some(slot);
        }
    }
    let mut filter_slots = BTreeSet::new();
    for filter in &plan.filters {
        collect_fast_aggregate_filter_slots(filter, &column_slots, &mut filter_slots)?;
    }
    let mut group_slots = Vec::with_capacity(plan.group_indices.len());
    for column_idx in &plan.group_indices {
        group_slots.push(column_slots.get(*column_idx).and_then(|slot| *slot)?);
    }
    let mut aggregate_slots = BTreeSet::new();
    for aggregate in &plan.aggregates {
        match aggregate {
            KvAggregateOp::CountStar => {}
            KvAggregateOp::CountColumn { column_idx }
            | KvAggregateOp::MaxColumn { column_idx }
            | KvAggregateOp::MinColumn { column_idx }
            | KvAggregateOp::SumColumn { column_idx }
            | KvAggregateOp::AvgColumn { column_idx } => {
                aggregate_slots.insert(column_slots.get(*column_idx).and_then(|slot| *slot)?);
            }
        }
    }
    let mut matched_slots = BTreeSet::new();
    matched_slots.extend(group_slots.iter().copied());
    matched_slots.extend(aggregate_slots.iter().copied());
    Some(FastNumericAggregatePlan {
        projector: storage_layout::FastNumericProjector::new(&plan.schema, &plan.required_indices),
        filter_slots: filter_slots.into_iter().collect(),
        group_slots,
        aggregate_slots: aggregate_slots.into_iter().collect(),
        matched_slots: matched_slots.into_iter().collect(),
        column_slots,
    })
}

pub(super) fn reduce_ungrouped_fast_numeric_scan(
    cursor: &mut RangeCursor,
    plan: &KvAggregateScan,
    fast_plan: FastNumericAggregatePlan,
    started_at: Instant,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile {
        groups_created: 1,
        ..AggregateScanProfile::default()
    };
    let mut values = vec![storage_layout::FastNumericValue::Null; fast_plan.projector.output_len()];
    let mut states = plan
        .aggregates
        .iter()
        .map(|aggregate| fast_aggregate_initial_state(plan, aggregate))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "unsupported fast aggregate state".to_string())?;
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_ref(&mut |_key, value, _seq| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            if let Err(error) = storage_layout::decode_row_record_fast_numeric_slots_with(
                &fast_plan.projector,
                value,
                &mut values,
                &fast_plan.filter_slots,
            ) {
                scan_error = Some(error.to_string());
                return false;
            }
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = plan
                .filters
                .iter()
                .all(|filter| fast_aggregate_predicate_matches(filter, &fast_plan, &values));
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if !matched {
                return true;
            }
            if profile_enabled {
                profile.matched += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            if let Err(error) = storage_layout::decode_row_record_fast_numeric_slots_with(
                &fast_plan.projector,
                value,
                &mut values,
                &fast_plan.aggregate_slots,
            ) {
                scan_error = Some(error.to_string());
                return false;
            }
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let aggregate_started_at = profile_enabled.then(Instant::now);
            apply_fast_aggregates(plan, &fast_plan, &values, &mut states);
            if let Some(aggregate_started_at) = aggregate_started_at {
                profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_fast_numeric projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    Ok(vec![finish_fast_aggregates(plan, states)])
}

pub(super) fn reduce_grouped_fast_numeric_scan(
    cursor: &mut RangeCursor,
    plan: &KvAggregateScan,
    fast_plan: FastNumericAggregatePlan,
    started_at: Instant,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile::default();
    let mut values = vec![storage_layout::FastNumericValue::Null; fast_plan.projector.output_len()];
    let mut groups: FastNumericGroupMap =
        HashMap::with_capacity_and_hasher(16_384, ahash::RandomState::new());
    let initial_states = initial_fast_aggregate_states(plan)
        .ok_or_else(|| "unsupported fast aggregate state".to_string())?;
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_ref(&mut |_key, value, _seq| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            if let Err(error) = storage_layout::decode_row_record_fast_numeric_slots_with(
                &fast_plan.projector,
                value,
                &mut values,
                &fast_plan.filter_slots,
            ) {
                scan_error = Some(error.to_string());
                return false;
            }
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = plan
                .filters
                .iter()
                .all(|filter| fast_aggregate_predicate_matches(filter, &fast_plan, &values));
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if !matched {
                return true;
            }
            if profile_enabled {
                profile.matched += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            if let Err(error) = storage_layout::decode_row_record_fast_numeric_slots_with(
                &fast_plan.projector,
                value,
                &mut values,
                &fast_plan.matched_slots,
            ) {
                scan_error = Some(error.to_string());
                return false;
            }
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let group_started_at = profile_enabled.then(Instant::now);
            let group_key = fast_numeric_group_key(&values, &fast_plan.group_slots);
            let states = match groups.entry(group_key) {
                std::collections::hash_map::Entry::Occupied(entry) => &mut entry.into_mut().1,
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if profile_enabled {
                        profile.groups_created += 1;
                    }
                    &mut entry
                        .insert((
                            fast_numeric_group_values(&values, &fast_plan.group_slots),
                            initial_states.clone(),
                        ))
                        .1
                }
            };
            if let Some(group_started_at) = group_started_at {
                profile.group_key_ns += group_started_at.elapsed().as_nanos();
            }
            let aggregate_started_at = profile_enabled.then(Instant::now);
            apply_fast_aggregates(plan, &fast_plan, &values, states);
            if let Some(aggregate_started_at) = aggregate_started_at {
                profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.group_key_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_fast_numeric_grouped projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    groups
        .into_values()
        .map(|(group_values, states)| finish_fast_grouped_aggregate_row(plan, group_values, states))
        .collect()
}

pub(super) fn reduce_ungrouped_projected_scan(
    cursor: &mut RangeCursor,
    plan: &KvAggregateScan,
    started_at: Instant,
) -> Result<Vec<Vec<ColumnValue>>, String> {
    let profile_enabled = copy_profile_enabled();
    let mut profile = AggregateScanProfile {
        groups_created: 1,
        ..AggregateScanProfile::default()
    };
    let mut states = plan
        .aggregates
        .iter()
        .map(kv_aggregate_initial_state)
        .collect::<Vec<_>>();
    let scan_started_at = profile_enabled.then(Instant::now);
    let mut scan_error = None;
    cursor
        .scan_ref(&mut |key, value, _seq| {
            if profile_enabled {
                profile.records += 1;
            }
            let decode_started_at = profile_enabled.then(Instant::now);
            let row = match projected_values_for_aggregate_record(&plan.schema, key, value, plan) {
                Ok(row) => row,
                Err(error) => {
                    scan_error = Some(error);
                    return false;
                }
            };
            if let Some(decode_started_at) = decode_started_at {
                profile.row_decode_ns += decode_started_at.elapsed().as_nanos();
            }
            let predicate_started_at = profile_enabled.then(Instant::now);
            let matched = projected_row_matches(plan, &row);
            if let Some(predicate_started_at) = predicate_started_at {
                profile.predicate_ns += predicate_started_at.elapsed().as_nanos();
            }
            if matched {
                if profile_enabled {
                    profile.matched += 1;
                }
                let aggregate_started_at = profile_enabled.then(Instant::now);
                apply_projected_aggregate(plan, &row, &mut states);
                if let Some(aggregate_started_at) = aggregate_started_at {
                    profile.aggregate_ns += aggregate_started_at.elapsed().as_nanos();
                }
            }
            true
        })
        .map_err(|e| e.to_string())?;
    if let Some(error) = scan_error {
        return Err(error);
    }
    if let Some(scan_started_at) = scan_started_at {
        profile.scan_ref_ns += scan_started_at
            .elapsed()
            .as_nanos()
            .saturating_sub(profile.row_decode_ns)
            .saturating_sub(profile.predicate_ns)
            .saturating_sub(profile.aggregate_ns);
        profile.batches = 1;
    }
    if profile_enabled {
        log_copy_profile(format!(
            "kv_aggregate.reduce_general projection={:?} aggregates={} groups={} records={} matched={} batches={} elapsed_ms={} scan_ref_ms={} row_decode_ms={} predicate_ms={} group_key_ms={} aggregate_ms={}",
            plan.projection,
            plan.aggregates.len(),
            profile.groups_created,
            profile.records,
            profile.matched,
            profile.batches,
            started_at.elapsed().as_millis(),
            nanos_to_millis(profile.scan_ref_ns),
            nanos_to_millis(profile.row_decode_ns),
            nanos_to_millis(profile.predicate_ns),
            nanos_to_millis(profile.group_key_ns),
            nanos_to_millis(profile.aggregate_ns)
        ));
    }
    Ok(vec![
        states
            .iter()
            .map(kv_finish_aggregate_state)
            .collect::<Vec<_>>(),
    ])
}

pub(super) fn required_slot(plan: &KvAggregateScan, column_idx: usize) -> Option<usize> {
    plan.required_indices
        .iter()
        .position(|required_idx| *required_idx == column_idx)
}

pub(super) fn fast_numeric_data_type(data_type: &crate::types::DataType) -> bool {
    matches!(
        data_type,
        crate::types::DataType::Int16
            | crate::types::DataType::Int32
            | crate::types::DataType::Int64
            | crate::types::DataType::Float32
            | crate::types::DataType::Float64
    )
}

pub(super) fn column_value_to_fast_numeric(
    value: &ColumnValue,
) -> Option<storage_layout::FastNumericValue> {
    match value {
        ColumnValue::Null => Some(storage_layout::FastNumericValue::Null),
        ColumnValue::Int16(value) => Some(storage_layout::FastNumericValue::I64(*value as i64)),
        ColumnValue::Int32(value) => Some(storage_layout::FastNumericValue::I64(*value as i64)),
        ColumnValue::Int64(value) => Some(storage_layout::FastNumericValue::I64(*value)),
        ColumnValue::Float32(value) => Some(storage_layout::FastNumericValue::F64(*value as f64)),
        ColumnValue::Float64(value) => Some(storage_layout::FastNumericValue::F64(*value)),
        _ => None,
    }
}

pub(super) fn fast_numeric_to_column_value(
    data_type: &crate::types::DataType,
    value: storage_layout::FastNumericValue,
) -> ColumnValue {
    match (data_type, value) {
        (_, storage_layout::FastNumericValue::Null) => ColumnValue::Null,
        (crate::types::DataType::Int16, storage_layout::FastNumericValue::I64(value)) => {
            ColumnValue::Int16(value as i16)
        }
        (crate::types::DataType::Int32, storage_layout::FastNumericValue::I64(value)) => {
            ColumnValue::Int32(value as i32)
        }
        (crate::types::DataType::Int64, storage_layout::FastNumericValue::I64(value)) => {
            ColumnValue::Int64(value)
        }
        (crate::types::DataType::Float32, storage_layout::FastNumericValue::F64(value)) => {
            ColumnValue::Float32(value as f32)
        }
        (crate::types::DataType::Float64, storage_layout::FastNumericValue::F64(value)) => {
            ColumnValue::Float64(value)
        }
        (_, storage_layout::FastNumericValue::I64(value)) => ColumnValue::Int64(value),
        (_, storage_layout::FastNumericValue::F64(value)) => ColumnValue::Float64(value),
    }
}

pub(super) fn fast_numeric_as_f64(value: storage_layout::FastNumericValue) -> Option<f64> {
    match value {
        storage_layout::FastNumericValue::Null => None,
        storage_layout::FastNumericValue::I64(value) => Some(value as f64),
        storage_layout::FastNumericValue::F64(value) => Some(value),
    }
}

pub(super) fn fast_numeric_cmp(
    left: storage_layout::FastNumericValue,
    right: storage_layout::FastNumericValue,
) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (storage_layout::FastNumericValue::Null, _)
        | (_, storage_layout::FastNumericValue::Null) => None,
        (
            storage_layout::FastNumericValue::I64(left),
            storage_layout::FastNumericValue::I64(right),
        ) => left.partial_cmp(&right),
        (left, right) => fast_numeric_as_f64(left)?.partial_cmp(&fast_numeric_as_f64(right)?),
    }
}

pub(super) fn compare_fast_numeric_values(
    left: storage_layout::FastNumericValue,
    op: KvCompareOp,
    right: storage_layout::FastNumericValue,
) -> bool {
    match op {
        KvCompareOp::Eq => fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_eq()),
        KvCompareOp::NotEq => fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_eq()),
        KvCompareOp::Gt => fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_gt()),
        KvCompareOp::GtEq => fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_lt()),
        KvCompareOp::Lt => fast_numeric_cmp(left, right).is_some_and(|ord| ord.is_lt()),
        KvCompareOp::LtEq => fast_numeric_cmp(left, right).is_some_and(|ord| !ord.is_gt()),
    }
}

pub(super) fn collect_fast_aggregate_filter_slots(
    predicate: &KvPredicate,
    column_slots: &[Option<usize>],
    slots: &mut BTreeSet<usize>,
) -> Option<()> {
    match predicate {
        KvPredicate::ColumnCompare {
            column_idx, value, ..
        } => {
            column_value_to_fast_numeric(value)?;
            slots.insert(column_slots.get(*column_idx).and_then(|slot| *slot)?);
        }
        KvPredicate::IsNull { column_idx } | KvPredicate::IsNotNull { column_idx } => {
            slots.insert(column_slots.get(*column_idx).and_then(|slot| *slot)?);
        }
        KvPredicate::Between {
            column_idx,
            low,
            high,
            ..
        } => {
            column_value_to_fast_numeric(low)?;
            column_value_to_fast_numeric(high)?;
            slots.insert(column_slots.get(*column_idx).and_then(|slot| *slot)?);
        }
        KvPredicate::And(predicates) | KvPredicate::Or(predicates) => {
            for predicate in predicates {
                collect_fast_aggregate_filter_slots(predicate, column_slots, slots)?;
            }
        }
        KvPredicate::Not(predicate) => {
            collect_fast_aggregate_filter_slots(predicate, column_slots, slots)?;
        }
    }
    Some(())
}

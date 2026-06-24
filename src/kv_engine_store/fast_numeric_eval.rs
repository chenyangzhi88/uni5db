use super::fast_numeric_plan::{
    column_value_to_fast_numeric, compare_fast_numeric_values, fast_numeric_as_f64,
    fast_numeric_cmp, fast_numeric_to_column_value,
};
use super::profile::{FastAggregateState, FastNumericAggregatePlan, FastNumericGroupKey};
use crate::mem_store::{KvAggregateOp, KvAggregateScan, KvCompareOp, KvPredicate};
use crate::storage_layout;
use crate::types::ColumnValue;

pub(super) fn fast_aggregate_predicate_matches(
    predicate: &KvPredicate,
    plan: &FastNumericAggregatePlan,
    values: &[storage_layout::FastNumericValue],
) -> bool {
    match predicate {
        KvPredicate::ColumnCompare {
            column_idx,
            op,
            value,
        } => plan
            .column_slots
            .get(*column_idx)
            .and_then(|slot| *slot)
            .and_then(|slot| values.get(slot).copied())
            .zip(column_value_to_fast_numeric(value))
            .is_some_and(|(left, right)| compare_fast_numeric_values(left, *op, right)),
        KvPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| fast_aggregate_predicate_matches(predicate, plan, values)),
        KvPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| fast_aggregate_predicate_matches(predicate, plan, values)),
        KvPredicate::Not(predicate) => !fast_aggregate_predicate_matches(predicate, plan, values),
        KvPredicate::IsNull { column_idx } => plan
            .column_slots
            .get(*column_idx)
            .and_then(|slot| *slot)
            .and_then(|slot| values.get(slot).copied())
            .is_none_or(|value| matches!(value, storage_layout::FastNumericValue::Null)),
        KvPredicate::IsNotNull { column_idx } => plan
            .column_slots
            .get(*column_idx)
            .and_then(|slot| *slot)
            .and_then(|slot| values.get(slot).copied())
            .is_some_and(|value| !matches!(value, storage_layout::FastNumericValue::Null)),
        KvPredicate::Between {
            column_idx,
            low,
            high,
            negated,
        } => {
            let inside = plan
                .column_slots
                .get(*column_idx)
                .and_then(|slot| *slot)
                .and_then(|slot| values.get(slot).copied())
                .zip(column_value_to_fast_numeric(low))
                .zip(column_value_to_fast_numeric(high))
                .is_some_and(|((value, low), high)| {
                    compare_fast_numeric_values(value, KvCompareOp::GtEq, low)
                        && compare_fast_numeric_values(value, KvCompareOp::LtEq, high)
                });
            if *negated { !inside } else { inside }
        }
    }
}

pub(super) fn fast_aggregate_initial_state(
    plan: &KvAggregateScan,
    aggregate: &KvAggregateOp,
) -> Option<FastAggregateState> {
    match aggregate {
        KvAggregateOp::CountStar | KvAggregateOp::CountColumn { .. } => {
            Some(FastAggregateState::Count(0))
        }
        KvAggregateOp::SumColumn { column_idx } => {
            let data_type = &plan.schema.columns.get(*column_idx)?.data_type;
            if matches!(
                data_type,
                crate::types::DataType::Float32 | crate::types::DataType::Float64
            ) {
                Some(FastAggregateState::SumF64(0.0))
            } else {
                Some(FastAggregateState::SumI64(0))
            }
        }
        KvAggregateOp::MaxColumn { .. } => Some(FastAggregateState::Max(
            storage_layout::FastNumericValue::Null,
        )),
        KvAggregateOp::MinColumn { .. } => Some(FastAggregateState::Min(
            storage_layout::FastNumericValue::Null,
        )),
        KvAggregateOp::AvgColumn { .. } => Some(FastAggregateState::Avg { sum: 0.0, count: 0 }),
    }
}

pub(super) fn initial_fast_aggregate_states(
    plan: &KvAggregateScan,
) -> Option<Vec<FastAggregateState>> {
    plan.aggregates
        .iter()
        .map(|aggregate| fast_aggregate_initial_state(plan, aggregate))
        .collect()
}

pub(super) fn apply_fast_aggregates(
    plan: &KvAggregateScan,
    fast_plan: &FastNumericAggregatePlan,
    values: &[storage_layout::FastNumericValue],
    states: &mut [FastAggregateState],
) {
    for (idx, aggregate) in plan.aggregates.iter().enumerate() {
        match aggregate {
            KvAggregateOp::CountStar => {
                if let FastAggregateState::Count(count) = &mut states[idx] {
                    *count += 1;
                }
            }
            KvAggregateOp::CountColumn { column_idx } => {
                let Some(value) = fast_aggregate_column_value(fast_plan, values, *column_idx)
                else {
                    continue;
                };
                if !matches!(value, storage_layout::FastNumericValue::Null)
                    && let FastAggregateState::Count(count) = &mut states[idx]
                {
                    *count += 1;
                }
            }
            KvAggregateOp::SumColumn { column_idx } => {
                let Some(value) = fast_aggregate_column_value(fast_plan, values, *column_idx)
                else {
                    continue;
                };
                match (&mut states[idx], value) {
                    (_, storage_layout::FastNumericValue::Null) => {}
                    (
                        FastAggregateState::SumI64(sum),
                        storage_layout::FastNumericValue::I64(value),
                    ) => {
                        *sum += value;
                    }
                    (FastAggregateState::SumF64(sum), value) => {
                        if let Some(value) = fast_numeric_as_f64(value) {
                            *sum += value;
                        }
                    }
                    _ => {}
                }
            }
            KvAggregateOp::AvgColumn { column_idx } => {
                let Some(value) = fast_aggregate_column_value(fast_plan, values, *column_idx)
                    .and_then(fast_numeric_as_f64)
                else {
                    continue;
                };
                if let FastAggregateState::Avg { sum, count } = &mut states[idx] {
                    *sum += value;
                    *count += 1;
                }
            }
            KvAggregateOp::MaxColumn { column_idx } => {
                let Some(value) = fast_aggregate_column_value(fast_plan, values, *column_idx)
                else {
                    continue;
                };
                if matches!(value, storage_layout::FastNumericValue::Null) {
                    continue;
                }
                if let FastAggregateState::Max(state) = &mut states[idx]
                    && (matches!(*state, storage_layout::FastNumericValue::Null)
                        || fast_numeric_cmp(value, *state).is_some_and(|ord| ord.is_gt()))
                {
                    *state = value;
                }
            }
            KvAggregateOp::MinColumn { column_idx } => {
                let Some(value) = fast_aggregate_column_value(fast_plan, values, *column_idx)
                else {
                    continue;
                };
                if matches!(value, storage_layout::FastNumericValue::Null) {
                    continue;
                }
                if let FastAggregateState::Min(state) = &mut states[idx]
                    && (matches!(*state, storage_layout::FastNumericValue::Null)
                        || fast_numeric_cmp(value, *state).is_some_and(|ord| ord.is_lt()))
                {
                    *state = value;
                }
            }
        }
    }
}

pub(super) fn fast_aggregate_column_value(
    plan: &FastNumericAggregatePlan,
    values: &[storage_layout::FastNumericValue],
    column_idx: usize,
) -> Option<storage_layout::FastNumericValue> {
    plan.column_slots
        .get(column_idx)
        .and_then(|slot| *slot)
        .and_then(|slot| values.get(slot).copied())
}

pub(super) fn finish_fast_aggregates(
    plan: &KvAggregateScan,
    states: Vec<FastAggregateState>,
) -> Vec<ColumnValue> {
    states
        .into_iter()
        .zip(plan.aggregates.iter())
        .map(|(state, aggregate)| match (state, aggregate) {
            (FastAggregateState::Count(count), _) => ColumnValue::Int64(count),
            (FastAggregateState::SumI64(sum), _) => ColumnValue::Int64(sum),
            (FastAggregateState::SumF64(sum), _) => ColumnValue::Float64(sum),
            (FastAggregateState::Avg { sum, count }, _) => {
                if count == 0 {
                    ColumnValue::Null
                } else {
                    ColumnValue::Float64(sum / count as f64)
                }
            }
            (FastAggregateState::Max(value), KvAggregateOp::MaxColumn { column_idx })
            | (FastAggregateState::Min(value), KvAggregateOp::MinColumn { column_idx }) => plan
                .schema
                .columns
                .get(*column_idx)
                .map(|column| fast_numeric_to_column_value(&column.data_type, value))
                .unwrap_or(ColumnValue::Null),
            _ => ColumnValue::Null,
        })
        .collect()
}

pub(super) fn fast_numeric_group_key(
    values: &[storage_layout::FastNumericValue],
    group_slots: &[usize],
) -> FastNumericGroupKey {
    if let [slot] = group_slots {
        return match values
            .get(*slot)
            .copied()
            .unwrap_or(storage_layout::FastNumericValue::Null)
        {
            storage_layout::FastNumericValue::Null => FastNumericGroupKey::SingleNull,
            storage_layout::FastNumericValue::I64(value) => FastNumericGroupKey::SingleI64(value),
            storage_layout::FastNumericValue::F64(value) => {
                FastNumericGroupKey::SingleF64(value.to_bits())
            }
        };
    }
    let mut out = Vec::with_capacity(group_slots.len() * 9);
    for slot in group_slots {
        match values
            .get(*slot)
            .copied()
            .unwrap_or(storage_layout::FastNumericValue::Null)
        {
            storage_layout::FastNumericValue::Null => out.push(0),
            storage_layout::FastNumericValue::I64(value) => {
                out.push(1);
                out.extend_from_slice(&value.to_be_bytes());
            }
            storage_layout::FastNumericValue::F64(value) => {
                out.push(2);
                out.extend_from_slice(&value.to_bits().to_be_bytes());
            }
        }
    }
    FastNumericGroupKey::Composite(out)
}

pub(super) fn fast_numeric_group_values(
    values: &[storage_layout::FastNumericValue],
    group_slots: &[usize],
) -> Vec<storage_layout::FastNumericValue> {
    group_slots
        .iter()
        .map(|slot| {
            values
                .get(*slot)
                .copied()
                .unwrap_or(storage_layout::FastNumericValue::Null)
        })
        .collect()
}

pub(super) fn finish_fast_grouped_aggregate_row(
    plan: &KvAggregateScan,
    group_values: Vec<storage_layout::FastNumericValue>,
    states: Vec<FastAggregateState>,
) -> Result<Vec<ColumnValue>, String> {
    let mut row = Vec::with_capacity(group_values.len() + states.len());
    for (value, column_idx) in group_values.into_iter().zip(plan.group_indices.iter()) {
        let column = plan
            .schema
            .columns
            .get(*column_idx)
            .ok_or_else(|| "group column index out of bounds".to_string())?;
        row.push(fast_numeric_to_column_value(&column.data_type, value));
    }
    row.extend(finish_fast_aggregates(plan, states));
    Ok(row)
}

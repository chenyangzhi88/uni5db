use super::*;

pub(super) fn projected_value<'a>(
    plan: &KvAggregateScan,
    row: &'a [ColumnValue],
    column_idx: usize,
) -> Option<&'a ColumnValue> {
    required_slot(plan, column_idx).and_then(|slot| row.get(slot))
}

pub(super) fn projected_values_for_aggregate_record(
    schema: &TableSchema,
    key: &[u8],
    value: &[u8],
    plan: &KvAggregateScan,
) -> Result<Vec<ColumnValue>, String> {
    let pk_only = plan.required_indices.iter().all(|idx| {
        schema
            .columns
            .get(*idx)
            .is_some_and(|column| column.name == schema.primary_key)
    });
    if pk_only {
        return plan
            .required_indices
            .iter()
            .map(|_| {
                storage_layout::decode_pk_from_row_key(
                    key,
                    schema.table_id,
                    schema.table_epoch,
                    schema.pk_data_type(),
                )
                .map_err(|e| e.to_string())
            })
            .collect();
    }

    let mut values =
        storage_layout::decode_row_record_projected_values(schema, value, &plan.required_indices)
            .map_err(|e| e.to_string())?;
    for (slot, column_idx) in plan.required_indices.iter().enumerate() {
        if schema
            .columns
            .get(*column_idx)
            .is_some_and(|column| column.name == schema.primary_key)
        {
            values[slot] = storage_layout::decode_pk_from_row_key(
                key,
                schema.table_id,
                schema.table_epoch,
                schema.pk_data_type(),
            )
            .map_err(|e| e.to_string())?;
        }
    }
    Ok(values)
}

pub(super) fn projected_row_matches(plan: &KvAggregateScan, row: &[ColumnValue]) -> bool {
    plan.filters
        .iter()
        .all(|predicate| projected_predicate_matches(plan, row, predicate))
}

pub(super) fn projected_predicate_matches(
    plan: &KvAggregateScan,
    row: &[ColumnValue],
    predicate: &KvPredicate,
) -> bool {
    match predicate {
        KvPredicate::ColumnCompare {
            column_idx,
            op,
            value,
        } => projected_value(plan, row, *column_idx)
            .is_some_and(|left| compare_values(left, *op, value)),
        KvPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| projected_predicate_matches(plan, row, predicate)),
        KvPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| projected_predicate_matches(plan, row, predicate)),
        KvPredicate::Not(predicate) => !projected_predicate_matches(plan, row, predicate),
        KvPredicate::IsNull { column_idx } => {
            projected_value(plan, row, *column_idx).is_none_or(ColumnValue::is_null)
        }
        KvPredicate::IsNotNull { column_idx } => {
            projected_value(plan, row, *column_idx).is_some_and(|value| !value.is_null())
        }
        KvPredicate::Between {
            column_idx,
            low,
            high,
            negated,
        } => {
            let inside = projected_value(plan, row, *column_idx).is_some_and(|value| {
                compare_values(value, KvCompareOp::GtEq, low)
                    && compare_values(value, KvCompareOp::LtEq, high)
            });
            if *negated { !inside } else { inside }
        }
    }
}

pub(super) fn compare_values(left: &ColumnValue, op: KvCompareOp, right: &ColumnValue) -> bool {
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

pub(super) fn projected_group_values(
    plan: &KvAggregateScan,
    row: &[ColumnValue],
) -> Vec<ColumnValue> {
    plan.group_indices
        .iter()
        .map(|column_idx| {
            projected_value(plan, row, *column_idx)
                .cloned()
                .unwrap_or(ColumnValue::Null)
        })
        .collect()
}

pub(super) fn typed_group_key(values: &[ColumnValue]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 16);
    for value in values {
        match value {
            ColumnValue::Null => out.push(0),
            ColumnValue::Int16(value) => {
                out.push(10);
                out.extend_from_slice(&value.to_be_bytes());
            }
            ColumnValue::Int32(value) => {
                out.push(1);
                out.extend_from_slice(&value.to_be_bytes());
            }
            ColumnValue::Int64(value) => {
                out.push(2);
                out.extend_from_slice(&value.to_be_bytes());
            }
            ColumnValue::Float32(value) => {
                out.push(3);
                out.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            ColumnValue::Float64(value) => {
                out.push(4);
                out.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            ColumnValue::Numeric(value) => push_group_key_bytes(&mut out, 5, value.as_bytes()),
            ColumnValue::Text(value)
            | ColumnValue::Date(value)
            | ColumnValue::Timestamp(value)
            | ColumnValue::TimestampTz(value)
            | ColumnValue::Uuid(value)
            | ColumnValue::Json(value)
            | ColumnValue::Jsonb(value) => push_group_key_bytes(&mut out, 6, value.as_bytes()),
            ColumnValue::Boolean(value) => {
                out.push(7);
                out.push(u8::from(*value));
            }
            ColumnValue::Bytea(value) => push_group_key_bytes(&mut out, 8, value),
            ColumnValue::Array(values) => {
                out.push(9);
                let nested = typed_group_key(values);
                out.extend_from_slice(&(nested.len() as u32).to_be_bytes());
                out.extend_from_slice(&nested);
            }
        }
    }
    out
}

pub(super) fn push_group_key_bytes(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

pub(super) fn apply_projected_aggregate(
    plan: &KvAggregateScan,
    row: &[ColumnValue],
    states: &mut [KvAggregateState],
) {
    for (idx, aggregate) in plan.aggregates.iter().enumerate() {
        match aggregate {
            KvAggregateOp::CountStar => {
                if let KvAggregateState::Count(count) = &mut states[idx] {
                    *count += 1;
                }
            }
            KvAggregateOp::CountColumn { column_idx } => {
                if projected_value(plan, row, *column_idx).is_some_and(|value| !value.is_null())
                    && let KvAggregateState::Count(count) = &mut states[idx]
                {
                    *count += 1;
                }
            }
            KvAggregateOp::MaxColumn { column_idx } => {
                let Some(value) = projected_value(plan, row, *column_idx) else {
                    continue;
                };
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
                    *state = value.clone();
                }
            }
            KvAggregateOp::MinColumn { column_idx } => {
                let Some(value) = projected_value(plan, row, *column_idx) else {
                    continue;
                };
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
                    *state = value.clone();
                }
            }
            KvAggregateOp::SumColumn { column_idx } => {
                let Some(value) = projected_value(plan, row, *column_idx) else {
                    continue;
                };
                if value.is_null() {
                    continue;
                }
                let KvAggregateState::Value(state) = &mut states[idx] else {
                    continue;
                };
                if let Some(sum) = sum_values(state, value) {
                    *state = sum;
                }
            }
            KvAggregateOp::AvgColumn { column_idx } => {
                let Some(value) = projected_value(plan, row, *column_idx).and_then(numeric_as_f64)
                else {
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

pub(super) fn sum_values(left: &ColumnValue, right: &ColumnValue) -> Option<ColumnValue> {
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

pub(super) fn numeric_as_f64(value: &ColumnValue) -> Option<f64> {
    match value {
        ColumnValue::Int32(value) => Some(*value as f64),
        ColumnValue::Int64(value) => Some(*value as f64),
        ColumnValue::Float32(value) => Some(*value as f64),
        ColumnValue::Float64(value) => Some(*value),
        _ => None,
    }
}

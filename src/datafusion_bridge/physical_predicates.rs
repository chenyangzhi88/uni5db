use std::cmp::Ordering;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr as DfExpr, Operator};

use crate::mem_store::{KvCompareOp, KvPredicate};
use crate::storage_layout;
use crate::types::{ColumnValue, TableSchema};

pub(super) fn scalar_value_to_column_value(value: &ScalarValue) -> Option<ColumnValue> {
    match value {
        ScalarValue::Boolean(value) => {
            Some(value.map(ColumnValue::Boolean).unwrap_or(ColumnValue::Null))
        }
        ScalarValue::Int32(value) => {
            Some(value.map(ColumnValue::Int32).unwrap_or(ColumnValue::Null))
        }
        ScalarValue::Int64(value) => {
            Some(value.map(ColumnValue::Int64).unwrap_or(ColumnValue::Null))
        }
        ScalarValue::Float32(value) => {
            Some(value.map(ColumnValue::Float32).unwrap_or(ColumnValue::Null))
        }
        ScalarValue::Float64(value) => {
            Some(value.map(ColumnValue::Float64).unwrap_or(ColumnValue::Null))
        }
        ScalarValue::Utf8(value) => Some(
            value
                .as_ref()
                .map(|value| ColumnValue::Text(value.clone()))
                .unwrap_or(ColumnValue::Null),
        ),
        ScalarValue::LargeUtf8(value) => Some(
            value
                .as_ref()
                .map(|value| ColumnValue::Text(value.clone()))
                .unwrap_or(ColumnValue::Null),
        ),
        ScalarValue::Null => Some(ColumnValue::Null),
        _ => None,
    }
}

pub(super) fn compare_column_values(
    left: &ColumnValue,
    op: &Operator,
    right: &ColumnValue,
) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    match op {
        Operator::Eq => left == right,
        Operator::NotEq => left != right,
        Operator::Gt => left.partial_cmp(right).is_some_and(|ord| ord.is_gt()),
        Operator::GtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_lt()),
        Operator::Lt => left.partial_cmp(right).is_some_and(|ord| ord.is_lt()),
        Operator::LtEq => left.partial_cmp(right).is_some_and(|ord| !ord.is_gt()),
        _ => false,
    }
}

pub(super) fn reverse_operator(op: &Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => *other,
    }
}

#[derive(Clone, Debug)]
pub(super) struct PrimaryKeyBounds {
    lower: Option<(ColumnValue, bool)>,
    upper: Option<(ColumnValue, bool)>,
}

impl PrimaryKeyBounds {
    fn new() -> Self {
        Self {
            lower: None,
            upper: None,
        }
    }

    fn apply(&mut self, op: KvCompareOp, value: ColumnValue) {
        match op {
            KvCompareOp::Eq => {
                self.tighten_lower(value.clone(), true);
                self.tighten_upper(value, true);
            }
            KvCompareOp::Gt => self.tighten_lower(value, false),
            KvCompareOp::GtEq => self.tighten_lower(value, true),
            KvCompareOp::Lt => self.tighten_upper(value, false),
            KvCompareOp::LtEq => self.tighten_upper(value, true),
            KvCompareOp::NotEq => {}
        }
    }

    fn tighten_lower(&mut self, value: ColumnValue, inclusive: bool) {
        let replace = match &self.lower {
            None => true,
            Some((current, current_inclusive)) => match value.partial_cmp(current) {
                Some(Ordering::Greater) => true,
                Some(Ordering::Equal) => !inclusive && *current_inclusive,
                _ => false,
            },
        };
        if replace {
            self.lower = Some((value, inclusive));
        }
    }

    fn tighten_upper(&mut self, value: ColumnValue, inclusive: bool) {
        let replace = match &self.upper {
            None => true,
            Some((current, current_inclusive)) => match value.partial_cmp(current) {
                Some(Ordering::Less) => true,
                Some(Ordering::Equal) => !inclusive && *current_inclusive,
                _ => false,
            },
        };
        if replace {
            self.upper = Some((value, inclusive));
        }
    }
}

pub(super) fn plan_primary_key_range(
    schema: &TableSchema,
    filters: Vec<KvPredicate>,
) -> Result<(storage_layout::RangeScan, Vec<KvPredicate>), Vec<KvPredicate>> {
    let Some(pk_idx) = schema
        .columns
        .iter()
        .position(|column| column.name == schema.primary_key)
    else {
        return Err(filters);
    };
    let mut bounds = PrimaryKeyBounds::new();
    let mut residual = Vec::new();
    let mut used_range = false;
    for filter in filters {
        collect_primary_key_bounds(filter, pk_idx, &mut bounds, &mut residual, &mut used_range);
    }
    if !used_range {
        return Err(residual);
    }
    let range = storage_layout::row_range_between(
        schema.table_id,
        schema.table_epoch,
        bounds
            .lower
            .as_ref()
            .map(|(value, inclusive)| (value, *inclusive)),
        bounds
            .upper
            .as_ref()
            .map(|(value, inclusive)| (value, *inclusive)),
        None,
    );
    Ok((range, residual))
}

pub(super) fn collect_primary_key_bounds(
    filter: KvPredicate,
    pk_idx: usize,
    bounds: &mut PrimaryKeyBounds,
    residual: &mut Vec<KvPredicate>,
    used_range: &mut bool,
) {
    match filter {
        KvPredicate::And(predicates) => {
            for predicate in predicates {
                collect_primary_key_bounds(predicate, pk_idx, bounds, residual, used_range);
            }
        }
        KvPredicate::ColumnCompare {
            column_idx,
            op,
            value,
        } if column_idx == pk_idx && op != KvCompareOp::NotEq => {
            bounds.apply(op, value);
            *used_range = true;
        }
        KvPredicate::Between {
            column_idx,
            low,
            high,
            negated: false,
        } if column_idx == pk_idx => {
            bounds.apply(KvCompareOp::GtEq, low);
            bounds.apply(KvCompareOp::LtEq, high);
            *used_range = true;
        }
        other => residual.push(other),
    }
}

pub(super) fn compile_kv_predicate(schema: &TableSchema, expr: &DfExpr) -> Option<KvPredicate> {
    match expr {
        DfExpr::BinaryExpr(binary) => match binary.op {
            Operator::And => Some(KvPredicate::And(vec![
                compile_kv_predicate(schema, &binary.left)?,
                compile_kv_predicate(schema, &binary.right)?,
            ])),
            Operator::Or => Some(KvPredicate::Or(vec![
                compile_kv_predicate(schema, &binary.left)?,
                compile_kv_predicate(schema, &binary.right)?,
            ])),
            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => compile_kv_compare(schema, &binary.left, binary.op, &binary.right),
            _ => None,
        },
        DfExpr::Not(inner) => Some(KvPredicate::Not(Box::new(compile_kv_predicate(
            schema, inner,
        )?))),
        DfExpr::IsNull(inner) => Some(KvPredicate::IsNull {
            column_idx: compile_filter_column_idx(schema, inner)?,
        }),
        DfExpr::IsNotNull(inner) => Some(KvPredicate::IsNotNull {
            column_idx: compile_filter_column_idx(schema, inner)?,
        }),
        DfExpr::Between(between) => Some(KvPredicate::Between {
            column_idx: compile_filter_column_idx(schema, &between.expr)?,
            low: compile_filter_literal_value(&between.low)?,
            high: compile_filter_literal_value(&between.high)?,
            negated: between.negated,
        }),
        DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
        | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
            compile_kv_predicate(schema, inner)
        }
        _ => None,
    }
}

pub(super) fn compile_kv_compare(
    schema: &TableSchema,
    left: &DfExpr,
    op: Operator,
    right: &DfExpr,
) -> Option<KvPredicate> {
    if let (Some(column_idx), Some(value)) = (
        compile_filter_column_idx(schema, left),
        compile_filter_literal_value(right),
    ) {
        return Some(KvPredicate::ColumnCompare {
            column_idx,
            op: compile_compare_op(op)?,
            value,
        });
    }
    if let (Some(value), Some(column_idx)) = (
        compile_filter_literal_value(left),
        compile_filter_column_idx(schema, right),
    ) {
        return Some(KvPredicate::ColumnCompare {
            column_idx,
            op: compile_compare_op(reverse_operator(&op))?,
            value,
        });
    }
    None
}

pub(super) fn compile_filter_column_idx(schema: &TableSchema, expr: &DfExpr) -> Option<usize> {
    match expr {
        DfExpr::Column(column) => schema
            .columns
            .iter()
            .position(|schema_column| schema_column.name == column.name),
        DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
        | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
            compile_filter_column_idx(schema, inner)
        }
        _ => None,
    }
}

pub(super) fn compile_filter_literal_value(expr: &DfExpr) -> Option<ColumnValue> {
    match expr {
        DfExpr::Literal(value, _) => scalar_value_to_column_value(value),
        DfExpr::Cast(datafusion::logical_expr::Cast { expr: inner, .. })
        | DfExpr::TryCast(datafusion::logical_expr::TryCast { expr: inner, .. }) => {
            compile_filter_literal_value(inner)
        }
        _ => None,
    }
}

pub(super) fn compile_compare_op(op: Operator) -> Option<KvCompareOp> {
    match op {
        Operator::Eq => Some(KvCompareOp::Eq),
        Operator::NotEq => Some(KvCompareOp::NotEq),
        Operator::Gt => Some(KvCompareOp::Gt),
        Operator::GtEq => Some(KvCompareOp::GtEq),
        Operator::Lt => Some(KvCompareOp::Lt),
        Operator::LtEq => Some(KvCompareOp::LtEq),
        _ => None,
    }
}

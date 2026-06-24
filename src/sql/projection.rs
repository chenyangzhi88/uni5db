use std::cmp::Ordering;
use std::collections::HashMap;

use pgwire::error::PgWireResult;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr,
    TableFactor, TableWithJoins,
};

use super::eval_core::evaluate_row_expression;
use super::fast_path::expr_identifier_name;
use super::values::{EvalContext, column_default_value, is_default_expr, sql_expr_to_column_value};
use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, DataType, ReturningProjection, RowMap, TableSchema};

pub fn extract_insert_values(query: &Query) -> PgWireResult<Vec<Vec<Expr>>> {
    match query.body.as_ref() {
        SetExpr::Values(values) => Ok(values.rows.iter().map(|row| row.content.clone()).collect()),
        _ => Err(unsupported("INSERT fast-path only supports VALUES")),
    }
}

pub fn build_insert_row(
    schema: &TableSchema,
    columns: &[Ident],
    values: Vec<Expr>,
) -> PgWireResult<RowMap> {
    let source_columns = if columns.is_empty() {
        schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
    } else {
        columns.iter().map(|c| c.value.clone()).collect::<Vec<_>>()
    };

    if source_columns.len() != values.len() {
        return Err(unsupported(
            "INSERT fast-path requires the same number of columns and values",
        ));
    }

    let mut provided = HashMap::new();
    for (col_name, expr) in source_columns.iter().zip(values.iter()) {
        let col = schema
            .find_column(col_name)
            .ok_or_else(|| user_error("42703", format!("column '{col_name}' not found")))?;
        let value = if is_default_expr(expr) {
            col.default
                .as_deref()
                .map(|default_sql| column_default_value(default_sql, &col.data_type))
                .transpose()?
                .unwrap_or(ColumnValue::Null)
        } else {
            sql_expr_to_column_value(expr, &col.data_type)?
        };
        provided.insert(col_name.clone(), value);
    }

    let mut row = HashMap::new();
    for col in &schema.columns {
        let value = match provided.remove(&col.name) {
            Some(value) => value,
            None => col
                .default
                .as_deref()
                .map(|default_sql| column_default_value(default_sql, &col.data_type))
                .transpose()?
                .unwrap_or(ColumnValue::Null),
        };
        row.insert(col.name.clone(), value);
    }
    Ok(row)
}

pub fn extract_single_table_name(select: &Select) -> PgWireResult<String> {
    if select.from.len() != 1 {
        return Err(unsupported("only single-table SELECT is supported"));
    }
    match &select.from[0].relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(unsupported("unsupported FROM clause in fast path")),
    }
}

pub fn extract_table_name_from_table_with_joins(table: &TableWithJoins) -> PgWireResult<String> {
    if !table.joins.is_empty() {
        return Err(unsupported("joins are not supported in fast path"));
    }
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(unsupported("unsupported table reference in fast path")),
    }
}

pub fn extract_primary_key_filter(
    selection: Option<&Expr>,
    schema: &TableSchema,
) -> PgWireResult<Option<ColumnValue>> {
    let Some(expr) = selection else {
        return Ok(None);
    };
    if !schema.has_user_primary_key() {
        return Ok(None);
    }
    let pk = &schema.primary_key;
    let pk_type = schema.pk_data_type();

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            let left_name = expr_identifier_name(left)?;
            if left_name == *pk {
                let value = sql_expr_to_column_value(right, pk_type)?;
                return Ok(Some(value));
            }
            Ok(None)
        }
        Expr::Nested(expr) => extract_primary_key_filter(Some(expr), schema),
        _ => Ok(None),
    }
}

pub type KeyBound = Option<(ColumnValue, bool)>;

pub(super) fn merge_lower_bound(current: &mut KeyBound, candidate: (ColumnValue, bool)) {
    match current {
        Some((value, inclusive)) => match candidate.0.partial_cmp(value) {
            Some(Ordering::Greater) => *current = Some(candidate),
            Some(Ordering::Equal) => *inclusive = *inclusive && candidate.1,
            _ => {}
        },
        None => *current = Some(candidate),
    }
}

pub(super) fn merge_upper_bound(current: &mut KeyBound, candidate: (ColumnValue, bool)) {
    match current {
        Some((value, inclusive)) => match candidate.0.partial_cmp(value) {
            Some(Ordering::Less) => *current = Some(candidate),
            Some(Ordering::Equal) => *inclusive = *inclusive && candidate.1,
            _ => {}
        },
        None => *current = Some(candidate),
    }
}

pub fn extract_primary_key_range_filter(
    selection: Option<&Expr>,
    schema: &TableSchema,
) -> PgWireResult<Option<(KeyBound, KeyBound)>> {
    if selection.is_none() {
        return Ok(None);
    }
    if !schema.has_user_primary_key() {
        return Ok(None);
    }
    let pk = &schema.primary_key;
    let pk_type = schema.pk_data_type();
    extract_column_range_filter(selection, pk, pk_type)
}

pub fn extract_column_range_filter(
    selection: Option<&Expr>,
    column_name: &str,
    data_type: &DataType,
) -> PgWireResult<Option<(KeyBound, KeyBound)>> {
    let Some(expr) = selection else {
        return Ok(None);
    };
    let mut lower = None;
    let mut upper = None;
    if collect_column_range_bounds(expr, column_name, data_type, &mut lower, &mut upper)? {
        Ok(Some((lower, upper)))
    } else {
        Ok(None)
    }
}

pub(super) fn collect_column_range_bounds(
    expr: &Expr,
    column_name: &str,
    data_type: &DataType,
    lower: &mut KeyBound,
    upper: &mut KeyBound,
) -> PgWireResult<bool> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Ok(
            collect_column_range_bounds(left, column_name, data_type, lower, upper)?
                && collect_column_range_bounds(right, column_name, data_type, lower, upper)?,
        ),
        Expr::BinaryOp { left, op, right } => {
            let Ok(actual_column_name) = expr_identifier_name(left) else {
                return Ok(false);
            };
            if actual_column_name != column_name {
                return Ok(false);
            }
            let value = sql_expr_to_column_value(right, data_type)?;
            match op {
                BinaryOperator::Gt => {
                    merge_lower_bound(lower, (value, false));
                    Ok(true)
                }
                BinaryOperator::GtEq => {
                    merge_lower_bound(lower, (value, true));
                    Ok(true)
                }
                BinaryOperator::Lt => {
                    merge_upper_bound(upper, (value, false));
                    Ok(true)
                }
                BinaryOperator::LtEq => {
                    merge_upper_bound(upper, (value, true));
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => {
            let Ok(actual_column_name) = expr_identifier_name(expr) else {
                return Ok(false);
            };
            if actual_column_name != column_name {
                return Ok(false);
            }
            merge_lower_bound(lower, (sql_expr_to_column_value(low, data_type)?, true));
            merge_upper_bound(upper, (sql_expr_to_column_value(high, data_type)?, true));
            Ok(true)
        }
        Expr::Nested(inner) => {
            collect_column_range_bounds(inner, column_name, data_type, lower, upper)
        }
        _ => Ok(false),
    }
}

pub fn extract_assignment_column(assignment: &Assignment) -> PgWireResult<String> {
    match &assignment.target {
        AssignmentTarget::ColumnName(name) if name.0.len() == 1 => name.0[0]
            .as_ident()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| unsupported("UPDATE fast-path only supports direct column assignments")),
        _ => Err(unsupported(
            "UPDATE fast-path only supports direct column assignments",
        )),
    }
}

pub fn resolve_projection(select: &Select, schema: &TableSchema) -> PgWireResult<Vec<String>> {
    if select.projection.len() == 1 && matches!(select.projection[0], SelectItem::Wildcard(_)) {
        return Ok(schema.column_names());
    }

    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr_identifier_name(expr),
            SelectItem::ExprWithAlias { alias, .. } => Ok(alias.value.clone()),
            _ => Err(unsupported(
                "fast-path only supports wildcard or simple column projections",
            )),
        })
        .collect()
}

pub fn resolve_returning_projection(
    returning: &[SelectItem],
    schema: &TableSchema,
) -> PgWireResult<Vec<ReturningProjection>> {
    if returning.is_empty() {
        return Err(unsupported(
            "RETURNING requires at least one projection item",
        ));
    }

    if returning.len() == 1 && matches!(returning[0], SelectItem::Wildcard(_)) {
        return Ok(vec![ReturningProjection::Wildcard]);
    }

    returning
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { expr, alias, .. } => Ok(ReturningProjection::Expr {
                expr: expr.clone(),
                output_name: alias.value.clone(),
            }),
            SelectItem::UnnamedExpr(expr) => {
                if let Ok(column) = expr_identifier_name(expr) {
                    if schema.find_column(&column).is_some() {
                        return Ok(ReturningProjection::Column(column));
                    }
                }
                Ok(ReturningProjection::Expr {
                    expr: expr.clone(),
                    output_name: expr.to_string(),
                })
            }
            SelectItem::Wildcard(_) => Err(unsupported(
                "RETURNING wildcard must be the only projection item",
            )),
            _ => Err(unsupported(
                "fast-path RETURNING only supports wildcard or expressions",
            )),
        })
        .collect()
}

pub fn evaluate_returning_row(
    projection: &[ReturningProjection],
    schema: &TableSchema,
    row: &RowMap,
    excluded_row: Option<&RowMap>,
) -> PgWireResult<Vec<ColumnValue>> {
    let mut output = Vec::new();
    for item in projection {
        match item {
            ReturningProjection::Wildcard => {
                for column in schema.column_names() {
                    output.push(row.get(&column).cloned().ok_or_else(|| {
                        user_error("42703", format!("column '{column}' not found"))
                    })?);
                }
            }
            ReturningProjection::Column(name) => {
                output.push(
                    row.get(name)
                        .cloned()
                        .ok_or_else(|| user_error("42703", format!("column '{name}' not found")))?,
                );
            }
            ReturningProjection::Expr { expr, .. } => {
                let ctx = EvalContext { row, excluded_row };
                output.push(evaluate_row_expression(expr, schema, &ctx)?);
            }
        }
    }
    Ok(output)
}

pub fn evaluate_returning_projection_output_names(
    schema: &TableSchema,
    projection: &[ReturningProjection],
) -> Vec<String> {
    let mut names = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            ReturningProjection::Wildcard => {
                names.extend(schema.column_names());
            }
            ReturningProjection::Column(name) => names.push(name.clone()),
            ReturningProjection::Expr { output_name, .. } => names.push(output_name.clone()),
        }
    }
    names
}

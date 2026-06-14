use std::cmp::Ordering;

use sqlparser::ast::{BinaryOperator, Expr};

use crate::sql::{
    EvalContext, evaluate_row_expression, expr_identifier_name, sql_expr_to_column_value,
};
use crate::types::{ColumnValue, DataType, RowMap, TableSchema};

pub fn row_matches_filter(row: &RowMap, schema: &TableSchema, expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                row_matches_filter(row, schema, left) && row_matches_filter(row, schema, right)
            }
            BinaryOperator::Or => {
                row_matches_filter(row, schema, left) || row_matches_filter(row, schema, right)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => {
                let Ok(column) = expr_identifier_name(left) else {
                    return evaluate_row_expression(
                        expr,
                        schema,
                        &EvalContext {
                            row,
                            excluded_row: None,
                        },
                    )
                    .ok()
                    .and_then(|value| match value {
                        ColumnValue::Boolean(value) => Some(value),
                        ColumnValue::Null => Some(false),
                        _ => None,
                    })
                    .unwrap_or(false);
                };
                let Some(row_value) = row.get(&column) else {
                    return false;
                };
                let col_type = schema
                    .find_column(&column)
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Text);
                let Ok(filter_value) = sql_expr_to_column_value(right, &col_type) else {
                    return false;
                };
                compare_values(row_value, &filter_value, op)
            }
            sqlparser::ast::BinaryOperator::Arrow
            | sqlparser::ast::BinaryOperator::LongArrow
            | sqlparser::ast::BinaryOperator::AtArrow
            | sqlparser::ast::BinaryOperator::Question => evaluate_row_expression(
                expr,
                schema,
                &EvalContext {
                    row,
                    excluded_row: None,
                },
            )
            .ok()
            .and_then(|value| match value {
                ColumnValue::Boolean(value) => Some(value),
                ColumnValue::Null => Some(false),
                _ => None,
            })
            .unwrap_or(false),
            _ => false,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let Ok(column) = expr_identifier_name(expr) else {
                return false;
            };
            let Some(row_value) = row.get(&column) else {
                return false;
            };
            if row_value.is_null() {
                return false;
            }
            let col_type = schema
                .find_column(&column)
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Text);
            let matched = list.iter().any(|value_expr| {
                sql_expr_to_column_value(value_expr, &col_type)
                    .ok()
                    .is_some_and(|filter_value| {
                        !filter_value.is_null() && *row_value == filter_value
                    })
            });
            if *negated { !matched } else { matched }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let Ok(column) = expr_identifier_name(expr) else {
                return false;
            };
            let Some(row_value) = row.get(&column) else {
                return false;
            };
            if row_value.is_null() {
                return false;
            }
            let col_type = schema
                .find_column(&column)
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Text);
            let Ok(low) = sql_expr_to_column_value(low, &col_type) else {
                return false;
            };
            let Ok(high) = sql_expr_to_column_value(high, &col_type) else {
                return false;
            };
            let inside = compare_values(row_value, &low, &BinaryOperator::GtEq)
                && compare_values(row_value, &high, &BinaryOperator::LtEq);
            if *negated { !inside } else { inside }
        }
        Expr::IsNull(expr) => match expr_identifier_name(expr) {
            Ok(column) => row.get(&column).is_none_or(ColumnValue::is_null),
            Err(_) => evaluate_row_expression(
                expr,
                schema,
                &EvalContext {
                    row,
                    excluded_row: None,
                },
            )
            .is_ok_and(|value| value.is_null()),
        },
        Expr::IsNotNull(expr) => match expr_identifier_name(expr) {
            Ok(column) => row.get(&column).is_some_and(|value| !value.is_null()),
            Err(_) => evaluate_row_expression(
                expr,
                schema,
                &EvalContext {
                    row,
                    excluded_row: None,
                },
            )
            .is_ok_and(|value| !value.is_null()),
        },
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            let matched = row_text_value(row, expr)
                .zip(literal_pattern(pattern))
                .is_some_and(|(value, pattern)| {
                    like_matches(&value, &pattern, escape_char_value(escape_char), false)
                });
            if *negated { !matched } else { matched }
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            let matched = row_text_value(row, expr)
                .zip(literal_pattern(pattern))
                .is_some_and(|(value, pattern)| {
                    like_matches(&value, &pattern, escape_char_value(escape_char), true)
                });
            if *negated { !matched } else { matched }
        }
        Expr::Nested(inner) => row_matches_filter(row, schema, inner),
        Expr::AnyOp { .. } | Expr::AllOp { .. } => evaluate_row_expression(
            expr,
            schema,
            &EvalContext {
                row,
                excluded_row: None,
            },
        )
        .ok()
        .and_then(|value| match value {
            ColumnValue::Boolean(value) => Some(value),
            ColumnValue::Null => Some(false),
            _ => None,
        })
        .unwrap_or(false),
        _ => false,
    }
}

fn row_text_value(row: &RowMap, expr: &Expr) -> Option<String> {
    let column = expr_identifier_name(expr).ok()?;
    row.get(&column)?.to_text()
}

fn literal_pattern(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            sqlparser::ast::Value::SingleQuotedString(value)
            | sqlparser::ast::Value::DoubleQuotedString(value) => Some(value.clone()),
            _ => None,
        },
        Expr::Nested(inner) => literal_pattern(inner),
        _ => None,
    }
}

fn escape_char_value(value: &Option<sqlparser::ast::ValueWithSpan>) -> Option<char> {
    let value = value.as_ref()?;
    match &value.value {
        sqlparser::ast::Value::SingleQuotedString(raw)
        | sqlparser::ast::Value::DoubleQuotedString(raw) => {
            let mut chars = raw.chars();
            let ch = chars.next()?;
            if chars.next().is_none() {
                Some(ch)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn like_matches(
    value: &str,
    pattern: &str,
    escape_char: Option<char>,
    case_insensitive: bool,
) -> bool {
    let value = if case_insensitive {
        value.to_lowercase()
    } else {
        value.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };
    like_match_chars(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        escape_char,
        0,
        0,
    )
}

fn like_match_chars(
    value: &[char],
    pattern: &[char],
    escape_char: Option<char>,
    value_idx: usize,
    pattern_idx: usize,
) -> bool {
    if pattern_idx == pattern.len() {
        return value_idx == value.len();
    }
    let current = pattern[pattern_idx];
    if escape_char == Some(current) && pattern_idx + 1 < pattern.len() {
        return value
            .get(value_idx)
            .is_some_and(|ch| *ch == pattern[pattern_idx + 1])
            && like_match_chars(value, pattern, escape_char, value_idx + 1, pattern_idx + 2);
    }
    match current {
        '%' => (value_idx..=value.len()).any(|next_value_idx| {
            like_match_chars(value, pattern, escape_char, next_value_idx, pattern_idx + 1)
        }),
        '_' => {
            value_idx < value.len()
                && like_match_chars(value, pattern, escape_char, value_idx + 1, pattern_idx + 1)
        }
        _ => {
            value.get(value_idx).is_some_and(|ch| *ch == current)
                && like_match_chars(value, pattern, escape_char, value_idx + 1, pattern_idx + 1)
        }
    }
}

fn compare_values(lhs: &ColumnValue, rhs: &ColumnValue, op: &BinaryOperator) -> bool {
    if lhs.is_null() || rhs.is_null() {
        return false;
    }
    match op {
        BinaryOperator::Eq => lhs == rhs,
        BinaryOperator::NotEq => lhs != rhs,
        BinaryOperator::Gt => lhs.partial_cmp(rhs) == Some(Ordering::Greater),
        BinaryOperator::Lt => lhs.partial_cmp(rhs) == Some(Ordering::Less),
        BinaryOperator::GtEq => matches!(
            lhs.partial_cmp(rhs),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        BinaryOperator::LtEq => {
            matches!(lhs.partial_cmp(rhs), Some(Ordering::Less | Ordering::Equal))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnSchema;
    use sqlparser::ast::{Ident, Value as SqlValue};

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            table_id: 1,
            schema_version: 1,
            table_epoch: 1,
            primary_key: "id".into(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            columns: vec![
                ColumnSchema {
                    column_id: 0,
                    name: "id".into(),
                    data_type: DataType::Int32,
                    primary_key: true,
                    nullable: false,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
                ColumnSchema {
                    column_id: 0,
                    name: "age".into(),
                    data_type: DataType::Int32,
                    primary_key: false,
                    nullable: true,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
                ColumnSchema {
                    column_id: 0,
                    name: "name".into(),
                    data_type: DataType::Text,
                    primary_key: false,
                    nullable: true,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
                ColumnSchema {
                    column_id: 0,
                    name: "active".into(),
                    data_type: DataType::Boolean,
                    primary_key: false,
                    nullable: true,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
            ],
        }
    }

    fn test_row() -> RowMap {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(1));
        row.insert("age".into(), ColumnValue::Int32(30));
        row.insert("name".into(), ColumnValue::Text("alice".into()));
        row.insert("active".into(), ColumnValue::Boolean(true));
        row
    }

    fn make_binary_op(col: &str, op: BinaryOperator, val: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(col))),
            op,
            right: Box::new(val),
        }
    }

    fn num(n: &str) -> Expr {
        Expr::Value(SqlValue::Number(n.into(), false).into())
    }

    fn str_val(s: &str) -> Expr {
        Expr::Value(SqlValue::SingleQuotedString(s.into()).into())
    }

    // ── Eq ────────────────────────────────────────────────────────────

    #[test]
    fn eq_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Eq, num("30"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn eq_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Eq, num("31"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn eq_text() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("name", BinaryOperator::Eq, str_val("alice"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    // ── NotEq ─────────────────────────────────────────────────────────

    #[test]
    fn not_eq_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::NotEq, num("99"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn not_eq_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::NotEq, num("30"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    // ── Gt / Lt ───────────────────────────────────────────────────────

    #[test]
    fn gt_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Gt, num("20"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn gt_equal_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Gt, num("30"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn lt_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Lt, num("40"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn lt_equal_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::Lt, num("30"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    // ── GtEq / LtEq ──────────────────────────────────────────────────

    #[test]
    fn gt_eq_match_equal() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::GtEq, num("30"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn gt_eq_match_greater() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::GtEq, num("29"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn gt_eq_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::GtEq, num("31"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn lt_eq_match_equal() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::LtEq, num("30"));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn lt_eq_no_match() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("age", BinaryOperator::LtEq, num("29"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    // ── And / Or ──────────────────────────────────────────────────────

    #[test]
    fn and_both_true() {
        let schema = test_schema();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(make_binary_op("age", BinaryOperator::GtEq, num("30"))),
            op: BinaryOperator::And,
            right: Box::new(make_binary_op("name", BinaryOperator::Eq, str_val("alice"))),
        };
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn and_one_false() {
        let schema = test_schema();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(make_binary_op("age", BinaryOperator::Gt, num("100"))),
            op: BinaryOperator::And,
            right: Box::new(make_binary_op("name", BinaryOperator::Eq, str_val("alice"))),
        };
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn or_one_true() {
        let schema = test_schema();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(make_binary_op("age", BinaryOperator::Gt, num("100"))),
            op: BinaryOperator::Or,
            right: Box::new(make_binary_op("name", BinaryOperator::Eq, str_val("alice"))),
        };
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn or_both_false() {
        let schema = test_schema();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(make_binary_op("age", BinaryOperator::Gt, num("100"))),
            op: BinaryOperator::Or,
            right: Box::new(make_binary_op("name", BinaryOperator::Eq, str_val("bob"))),
        };
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn eq_match_with_casted_literal() {
        let schema = test_schema();
        let row = test_row();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("age"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Cast {
                expr: Box::new(str_val("30")),
                data_type: sqlparser::ast::DataType::Int(None),
                kind: sqlparser::ast::CastKind::Cast,
                array: false,
                format: None,
            }),
        };
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    // ── Nested ────────────────────────────────────────────────────────

    #[test]
    fn nested_unwrap() {
        let schema = test_schema();
        let row = test_row();
        let inner = make_binary_op("age", BinaryOperator::Eq, num("30"));
        let expr = Expr::Nested(Box::new(inner));
        assert!(row_matches_filter(&row, &schema, &expr));
    }

    // ── Edge cases ────────────────────────────────────────────────────

    #[test]
    fn missing_column_returns_false() {
        let schema = test_schema();
        let row = test_row();
        let expr = make_binary_op("nonexistent", BinaryOperator::Eq, num("1"));
        assert!(!row_matches_filter(&row, &schema, &expr));
    }

    #[test]
    fn text_comparison_operators() {
        let schema = test_schema();
        let row = test_row();
        assert!(row_matches_filter(
            &row,
            &schema,
            &make_binary_op("name", BinaryOperator::Gt, str_val("aaa")),
        ));
        assert!(row_matches_filter(
            &row,
            &schema,
            &make_binary_op("name", BinaryOperator::Lt, str_val("zzz")),
        ));
    }
}

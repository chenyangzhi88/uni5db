use std::cmp::Ordering;

use pgwire::error::PgWireResult;
use sqlparser::ast::BinaryOperator;

use crate::error::{unsupported, user_error};
use crate::types::ColumnValue;

pub(super) fn negate_value(value: ColumnValue) -> PgWireResult<ColumnValue> {
    match value {
        ColumnValue::Int16(v) => Ok(ColumnValue::Int16(-v)),
        ColumnValue::Int32(v) => Ok(ColumnValue::Int32(-v)),
        ColumnValue::Int64(v) => Ok(ColumnValue::Int64(-v)),
        ColumnValue::Float32(v) => Ok(ColumnValue::Float32(-v)),
        ColumnValue::Float64(v) => Ok(ColumnValue::Float64(-v)),
        ColumnValue::Numeric(v) => v
            .parse::<f64>()
            .map(|value| ColumnValue::Numeric((-value).to_string()))
            .map_err(|_| user_error("22003", "numeric out of range")),
        _ => Err(unsupported("cannot apply unary '-' to non-numeric value")),
    }
}

pub(super) fn arithmetic_row_values(
    left: ColumnValue,
    right: ColumnValue,
    op: BinaryOperator,
) -> PgWireResult<ColumnValue> {
    if left.is_null() || right.is_null() {
        return Ok(ColumnValue::Null);
    }

    match (left, right) {
        (ColumnValue::Int16(lhs), ColumnValue::Int16(rhs)) => arithmetic_row_values(
            ColumnValue::Int32(lhs as i32),
            ColumnValue::Int32(rhs as i32),
            op,
        ),
        (ColumnValue::Int16(lhs), ColumnValue::Int32(rhs)) => {
            arithmetic_row_values(ColumnValue::Int32(lhs as i32), ColumnValue::Int32(rhs), op)
        }
        (ColumnValue::Int32(lhs), ColumnValue::Int16(rhs)) => {
            arithmetic_row_values(ColumnValue::Int32(lhs), ColumnValue::Int32(rhs as i32), op)
        }
        (ColumnValue::Int16(lhs), ColumnValue::Int64(rhs)) => {
            arithmetic_row_values(ColumnValue::Int64(lhs as i64), ColumnValue::Int64(rhs), op)
        }
        (ColumnValue::Int64(lhs), ColumnValue::Int16(rhs)) => {
            arithmetic_row_values(ColumnValue::Int64(lhs), ColumnValue::Int64(rhs as i64), op)
        }
        (ColumnValue::Int32(lhs), ColumnValue::Int32(rhs)) => {
            let value = match op {
                BinaryOperator::Plus => lhs.checked_add(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} + {rhs}"),
                    )
                })?,
                BinaryOperator::Minus => lhs.checked_sub(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} - {rhs}"),
                    )
                })?,
                BinaryOperator::Multiply => lhs.checked_mul(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} * {rhs}"),
                    )
                })?,
                BinaryOperator::Divide => {
                    if rhs == 0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs / rhs
                }
                BinaryOperator::Modulo => {
                    if rhs == 0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs % rhs
                }
                _ => return Err(unsupported("unsupported arithmetic operator")),
            };
            Ok(ColumnValue::Int32(value))
        }
        (ColumnValue::Int64(lhs), ColumnValue::Int64(rhs)) => {
            let value = match op {
                BinaryOperator::Plus => lhs.checked_add(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} + {rhs}"),
                    )
                })?,
                BinaryOperator::Minus => lhs.checked_sub(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} - {rhs}"),
                    )
                })?,
                BinaryOperator::Multiply => lhs.checked_mul(rhs).ok_or_else(|| {
                    user_error(
                        "22003",
                        format!("numeric value out of range: {lhs} * {rhs}"),
                    )
                })?,
                BinaryOperator::Divide => {
                    if rhs == 0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs / rhs
                }
                BinaryOperator::Modulo => {
                    if rhs == 0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs % rhs
                }
                _ => return Err(unsupported("unsupported arithmetic operator")),
            };
            Ok(ColumnValue::Int64(value))
        }
        (ColumnValue::Float32(lhs), ColumnValue::Float32(rhs)) => {
            Ok(ColumnValue::Float32(match op {
                BinaryOperator::Plus => lhs + rhs,
                BinaryOperator::Minus => lhs - rhs,
                BinaryOperator::Multiply => lhs * rhs,
                BinaryOperator::Divide => {
                    if rhs == 0.0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs / rhs
                }
                BinaryOperator::Modulo => lhs % rhs,
                _ => return Err(unsupported("unsupported arithmetic operator")),
            }))
        }
        (ColumnValue::Float64(lhs), ColumnValue::Float64(rhs)) => {
            Ok(ColumnValue::Float64(match op {
                BinaryOperator::Plus => lhs + rhs,
                BinaryOperator::Minus => lhs - rhs,
                BinaryOperator::Multiply => lhs * rhs,
                BinaryOperator::Divide => {
                    if rhs == 0.0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    lhs / rhs
                }
                BinaryOperator::Modulo => lhs % rhs,
                _ => return Err(unsupported("unsupported arithmetic operator")),
            }))
        }
        (ColumnValue::Int32(lhs), ColumnValue::Int64(rhs)) => {
            arithmetic_row_values(ColumnValue::Int64(lhs as i64), ColumnValue::Int64(rhs), op)
        }
        (ColumnValue::Int64(lhs), ColumnValue::Int32(rhs)) => {
            arithmetic_row_values(ColumnValue::Int64(lhs), ColumnValue::Int64(rhs as i64), op)
        }
        (ColumnValue::Int32(lhs), ColumnValue::Float32(rhs)) => arithmetic_row_values(
            ColumnValue::Float32(lhs as f32),
            ColumnValue::Float32(rhs),
            op,
        ),
        (ColumnValue::Int32(lhs), ColumnValue::Float64(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs as f64),
            ColumnValue::Float64(rhs),
            op,
        ),
        (ColumnValue::Float64(lhs), ColumnValue::Int32(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs),
            ColumnValue::Float64(rhs as f64),
            op,
        ),
        (ColumnValue::Int16(lhs), ColumnValue::Float32(rhs)) => arithmetic_row_values(
            ColumnValue::Float32(lhs as f32),
            ColumnValue::Float32(rhs),
            op,
        ),
        (ColumnValue::Float32(lhs), ColumnValue::Int16(rhs)) => arithmetic_row_values(
            ColumnValue::Float32(lhs),
            ColumnValue::Float32(rhs as f32),
            op,
        ),
        (ColumnValue::Int16(lhs), ColumnValue::Float64(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs as f64),
            ColumnValue::Float64(rhs),
            op,
        ),
        (ColumnValue::Float64(lhs), ColumnValue::Int16(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs),
            ColumnValue::Float64(rhs as f64),
            op,
        ),
        (ColumnValue::Float32(lhs), ColumnValue::Int32(rhs)) => arithmetic_row_values(
            ColumnValue::Float32(lhs),
            ColumnValue::Float32(rhs as f32),
            op,
        ),
        (ColumnValue::Int64(lhs), ColumnValue::Float64(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs as f64),
            ColumnValue::Float64(rhs),
            op,
        ),
        (ColumnValue::Float64(lhs), ColumnValue::Int64(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs),
            ColumnValue::Float64(rhs as f64),
            op,
        ),
        (ColumnValue::Float32(lhs), ColumnValue::Float64(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs as f64),
            ColumnValue::Float64(rhs),
            op,
        ),
        (ColumnValue::Float64(lhs), ColumnValue::Float32(rhs)) => arithmetic_row_values(
            ColumnValue::Float64(lhs),
            ColumnValue::Float64(rhs as f64),
            op,
        ),
        (ColumnValue::Numeric(lhs), ColumnValue::Numeric(rhs)) => {
            let left = lhs
                .parse::<f64>()
                .map_err(|_| user_error("22003", "numeric out of range"))?;
            let right = rhs
                .parse::<f64>()
                .map_err(|_| user_error("22003", "numeric out of range"))?;
            let value = match op {
                BinaryOperator::Plus => left + right,
                BinaryOperator::Minus => left - right,
                BinaryOperator::Multiply => left * right,
                BinaryOperator::Divide => {
                    if right == 0.0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    left / right
                }
                BinaryOperator::Modulo => {
                    if right == 0.0 {
                        return Err(user_error("22012", "division by zero"));
                    }
                    left % right
                }
                _ => return Err(unsupported("unsupported arithmetic operator")),
            };
            Ok(ColumnValue::Numeric(value.to_string()))
        }
        (lhs, rhs) => {
            if let (Some(lhs), Some(rhs)) =
                (mysql_value_to_number(&lhs), mysql_value_to_number(&rhs))
            {
                let value = match op {
                    BinaryOperator::Plus => lhs + rhs,
                    BinaryOperator::Minus => lhs - rhs,
                    BinaryOperator::Multiply => lhs * rhs,
                    BinaryOperator::Divide => {
                        if rhs == 0.0 {
                            return Err(user_error("22012", "division by zero"));
                        }
                        lhs / rhs
                    }
                    BinaryOperator::Modulo => {
                        if rhs == 0.0 {
                            return Err(user_error("22012", "division by zero"));
                        }
                        lhs % rhs
                    }
                    _ => return Err(unsupported("unsupported arithmetic operator")),
                };
                Ok(ColumnValue::Float64(value))
            } else {
                Err(unsupported(format!(
                    "unsupported value types for arithmetic: {lhs:?} and {rhs:?}",
                )))
            }
        }
    }
}

pub(super) fn compare_row_values(
    lhs: &ColumnValue,
    rhs: &ColumnValue,
    op: &BinaryOperator,
) -> bool {
    if matches!(op, BinaryOperator::Spaceship) {
        return match (lhs.is_null(), rhs.is_null()) {
            (true, true) => true,
            (true, false) | (false, true) => false,
            (false, false) => compare_row_values(lhs, rhs, &BinaryOperator::Eq),
        };
    }
    if lhs.is_null() || rhs.is_null() {
        return false;
    }
    if let (Some(lhs_num), Some(rhs_num)) = (mysql_value_to_number(lhs), mysql_value_to_number(rhs))
        && (mysql_is_numeric_like(lhs) || mysql_is_numeric_like(rhs))
    {
        return match op {
            BinaryOperator::Eq => lhs_num == rhs_num,
            BinaryOperator::NotEq => lhs_num != rhs_num,
            BinaryOperator::Gt => lhs_num > rhs_num,
            BinaryOperator::Lt => lhs_num < rhs_num,
            BinaryOperator::GtEq => lhs_num >= rhs_num,
            BinaryOperator::LtEq => lhs_num <= rhs_num,
            _ => false,
        };
    }
    match op {
        BinaryOperator::Eq => lhs == rhs,
        BinaryOperator::NotEq => lhs != rhs,
        BinaryOperator::Gt => lhs.partial_cmp(rhs) == Some(Ordering::Greater),
        BinaryOperator::Lt => lhs.partial_cmp(rhs) == Some(Ordering::Less),
        BinaryOperator::GtEq => matches!(
            lhs.partial_cmp(rhs),
            Some(Ordering::Equal | Ordering::Greater)
        ),
        BinaryOperator::LtEq => {
            matches!(lhs.partial_cmp(rhs), Some(Ordering::Equal | Ordering::Less))
        }
        _ => false,
    }
}

pub(super) fn mysql_is_numeric_like(value: &ColumnValue) -> bool {
    matches!(
        value,
        ColumnValue::Int16(_)
            | ColumnValue::Int32(_)
            | ColumnValue::Int64(_)
            | ColumnValue::Float32(_)
            | ColumnValue::Float64(_)
            | ColumnValue::Numeric(_)
            | ColumnValue::Boolean(_)
    )
}

pub(super) fn mysql_value_to_number(value: &ColumnValue) -> Option<f64> {
    match value {
        ColumnValue::Int16(value) => Some(*value as f64),
        ColumnValue::Int32(value) => Some(*value as f64),
        ColumnValue::Int64(value) => Some(*value as f64),
        ColumnValue::Float32(value) => Some(*value as f64),
        ColumnValue::Float64(value) => Some(*value),
        ColumnValue::Numeric(value) | ColumnValue::Text(value) => Some(mysql_text_to_number(value)),
        ColumnValue::Boolean(value) => Some(if *value { 1.0 } else { 0.0 }),
        _ => None,
    }
}

pub(super) fn mysql_text_to_number(value: &str) -> f64 {
    let value = value.trim_start();
    let mut end = 0usize;
    let mut chars = value.char_indices().peekable();
    if let Some((idx, ch)) = chars.peek().copied()
        && idx == 0
        && (ch == '+' || ch == '-')
    {
        end = ch.len_utf8();
        chars.next();
    }
    let mut saw_digit = false;
    let mut saw_dot = false;
    while let Some((idx, ch)) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            saw_digit = true;
            end = idx + ch.len_utf8();
            chars.next();
        } else if ch == '.' && !saw_dot {
            saw_dot = true;
            end = idx + ch.len_utf8();
            chars.next();
        } else {
            break;
        }
    }
    if saw_digit
        && let Some((idx, ch)) = chars.peek().copied()
        && (ch == 'e' || ch == 'E')
    {
        let exponent_start = idx;
        chars.next();
        let mut exponent_end = idx + ch.len_utf8();
        if let Some((sign_idx, sign)) = chars.peek().copied()
            && (sign == '+' || sign == '-')
        {
            exponent_end = sign_idx + sign.len_utf8();
            chars.next();
        }
        let mut exponent_digits = false;
        while let Some((digit_idx, digit)) = chars.peek().copied() {
            if digit.is_ascii_digit() {
                exponent_digits = true;
                exponent_end = digit_idx + digit.len_utf8();
                chars.next();
            } else {
                break;
            }
        }
        if exponent_digits {
            end = exponent_end;
        } else {
            end = exponent_start;
        }
    }
    if !saw_digit {
        return 0.0;
    }
    value[..end].parse::<f64>().unwrap_or(0.0)
}

use std::cmp::Ordering;

use pgwire::error::PgWireResult;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, TypedString,
    UnaryOperator, Value as SqlValue,
};

use super::functions::{
    eval_abs, eval_concat_ws, eval_date_add_sub, eval_date_format, eval_date_trunc, eval_elt,
    eval_extract, eval_field, eval_find_in_set, eval_format, eval_from_unixtime, eval_last_day,
    eval_locate, eval_numeric_unary, eval_regexp_like, eval_regexp_replace, eval_str_to_date,
    eval_substring, eval_substring_index, eval_timestampadd, eval_timestampdiff, eval_trim,
    eval_unix_timestamp, eval_week, exactly_one, unary_text,
};
use super::json::{
    JsonMutation, escape_char_value, eval_json_array, eval_json_contains, eval_json_extract,
    eval_json_mutate, eval_json_object, eval_json_unquote, function_args, json_contains_value,
    json_extract_value, json_has_key_value, matches_like, simple_regexp_match, value_to_i64,
};
use super::operators::{
    arithmetic_row_values, compare_row_values, mysql_text_to_number, negate_value,
};
use super::values::{EvalContext, coerce_column_value, current_timestamp_text};
use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, DataType, TableSchema};

pub fn evaluate_row_expression(
    expr: &Expr,
    schema: &TableSchema,
    ctx: &EvalContext<'_>,
) -> PgWireResult<ColumnValue> {
    match expr {
        Expr::Cast {
            expr,
            data_type: cast_type,
            ..
        } => {
            let target = DataType::from_sql(&cast_type.to_string());
            let value = evaluate_row_expression(expr, schema, ctx)?;
            coerce_column_value(value, &target)
        }
        Expr::TypedString(TypedString {
            value,
            data_type: typed_data_type,
            ..
        }) => {
            let target = DataType::from_sql(&typed_data_type.to_string());
            let value = evaluate_row_expression(&Expr::Value(value.clone()), schema, ctx)?;
            coerce_column_value(value, &target)
        }
        Expr::Identifier(ident) => {
            let column = &ident.value;
            if column.eq_ignore_ascii_case("current_timestamp")
                || column.eq_ignore_ascii_case("now")
            {
                return Ok(ColumnValue::Text(current_timestamp_text()));
            }
            if column.eq_ignore_ascii_case("excluded") && ctx.excluded_row.is_none() {
                return Err(unsupported(
                    "reference to EXCLUDED.* requires ON CONFLICT DO UPDATE",
                ));
            }
            ctx.row
                .get(column)
                .cloned()
                .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let column = &parts[1].value;
            if table.eq_ignore_ascii_case("excluded") {
                ctx.excluded_row
                    .and_then(|excluded| excluded.get(column))
                    .cloned()
                    .ok_or_else(|| {
                        user_error(
                            "42703",
                            format!("column '{column}' not found in excluded row"),
                        )
                    })
            } else {
                if schema.find_column(column).is_none() {
                    Err(user_error("42703", format!("column '{column}' not found")))
                } else {
                    ctx.row
                        .get(column)
                        .cloned()
                        .ok_or_else(|| user_error("42703", format!("column '{column}' not found")))
                }
            }
        }
        Expr::Function(function)
            if function.name.to_string().eq_ignore_ascii_case("values")
                && ctx.excluded_row.is_some() =>
        {
            let args = function_args(function.args.clone());
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(column)))] =
                args.as_slice()
            else {
                return Err(unsupported(
                    "VALUES() in ON DUPLICATE KEY UPDATE requires one column argument",
                ));
            };
            ctx.excluded_row
                .and_then(|excluded| excluded.get(&column.value))
                .cloned()
                .ok_or_else(|| {
                    user_error(
                        "42703",
                        format!("column '{}' not found in inserted row", column.value),
                    )
                })
        }
        Expr::Function(function)
            if function
                .name
                .to_string()
                .eq_ignore_ascii_case("current_timestamp")
                || function.name.to_string().eq_ignore_ascii_case("now") =>
        {
            if !function_args(function.args.clone()).is_empty() {
                return Err(unsupported(
                    "current_timestamp/now does not accept arguments in fast path",
                ));
            }
            Ok(ColumnValue::Text(current_timestamp_text()))
        }
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("coalesce") => {
            let args = evaluate_function_args(function.args.clone(), schema, ctx)?;
            for value in args {
                if !value.is_null() {
                    return Ok(value);
                }
            }
            Ok(ColumnValue::Null)
        }
        Expr::Function(function) => evaluate_scalar_function(
            &function.name.to_string(),
            evaluate_function_args(function.args.clone(), schema, ctx)?,
        ),
        Expr::Value(value) => match &value.value {
            SqlValue::Number(v, _) => {
                Ok(v.parse::<i64>().map(ColumnValue::Int64).or_else(|_| {
                    v.parse::<f64>()
                        .map(ColumnValue::Float64)
                        .map_err(|_| unsupported("invalid numeric literal"))
                })?)
            }
            SqlValue::SingleQuotedString(v) | SqlValue::DoubleQuotedString(v) => {
                Ok(ColumnValue::Text(v.clone()))
            }
            SqlValue::Boolean(v) => Ok(ColumnValue::Boolean(*v)),
            SqlValue::Null => Ok(ColumnValue::Null),
            _ => Err(unsupported("unsupported literal in fast-path DML")),
        },
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(ColumnValue::Boolean(!evaluate_row_bool(expr, schema, ctx)?)),
            UnaryOperator::Plus => evaluate_row_expression(expr, schema, ctx),
            UnaryOperator::Minus => {
                let value = evaluate_row_expression(expr, schema, ctx)?;
                negate_value(value)
            }
            UnaryOperator::BitwiseNot => {
                let value = evaluate_row_expression(expr, schema, ctx)?;
                Ok(ColumnValue::Int64(!value_to_i64(&value)?))
            }
            _ => Err(unsupported("unsupported unary operator")),
        },
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(ColumnValue::Boolean(
                evaluate_row_bool(left, schema, ctx)? && evaluate_row_bool(right, schema, ctx)?,
            )),
            BinaryOperator::Or => Ok(ColumnValue::Boolean(
                evaluate_row_bool(left, schema, ctx)? || evaluate_row_bool(right, schema, ctx)?,
            )),
            BinaryOperator::Plus => arithmetic_row_values(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                BinaryOperator::Plus,
            ),
            BinaryOperator::Minus => arithmetic_row_values(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                BinaryOperator::Minus,
            ),
            BinaryOperator::Multiply => arithmetic_row_values(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                BinaryOperator::Multiply,
            ),
            BinaryOperator::Divide => arithmetic_row_values(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                BinaryOperator::Divide,
            ),
            BinaryOperator::Modulo => arithmetic_row_values(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                BinaryOperator::Modulo,
            ),
            BinaryOperator::MyIntegerDivide => {
                let lhs = evaluate_row_expression(left, schema, ctx)?;
                let rhs = evaluate_row_expression(right, schema, ctx)?;
                let divisor = value_to_i64(&rhs)?;
                if divisor == 0 {
                    return Err(user_error("22012", "division by zero"));
                }
                Ok(ColumnValue::Int64(value_to_i64(&lhs)? / divisor))
            }
            BinaryOperator::Xor => Ok(ColumnValue::Boolean(
                evaluate_row_bool(left, schema, ctx)? ^ evaluate_row_bool(right, schema, ctx)?,
            )),
            BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseOr | BinaryOperator::BitwiseXor => {
                let lhs = value_to_i64(&evaluate_row_expression(left, schema, ctx)?)?;
                let rhs = value_to_i64(&evaluate_row_expression(right, schema, ctx)?)?;
                let value = match op {
                    BinaryOperator::BitwiseAnd => lhs & rhs,
                    BinaryOperator::BitwiseOr => lhs | rhs,
                    BinaryOperator::BitwiseXor => lhs ^ rhs,
                    _ => unreachable!(),
                };
                Ok(ColumnValue::Int64(value))
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Spaceship => {
                let lhs = evaluate_row_expression(left, schema, ctx)?;
                let rhs = evaluate_row_expression(right, schema, ctx)?;
                Ok(ColumnValue::Boolean(compare_row_values(&lhs, &rhs, op)))
            }
            BinaryOperator::Arrow | BinaryOperator::LongArrow => json_extract_value(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
                matches!(op, BinaryOperator::LongArrow),
            ),
            BinaryOperator::AtArrow => Ok(ColumnValue::Boolean(json_contains_value(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
            )?)),
            BinaryOperator::Question => Ok(ColumnValue::Boolean(json_has_key_value(
                evaluate_row_expression(left, schema, ctx)?,
                evaluate_row_expression(right, schema, ctx)?,
            )?)),
            BinaryOperator::Regexp => {
                let value = evaluate_row_expression(left, schema, ctx)?
                    .to_text()
                    .unwrap_or_default();
                let pattern = evaluate_row_expression(right, schema, ctx)?
                    .to_text()
                    .unwrap_or_default();
                Ok(ColumnValue::Boolean(simple_regexp_match(&value, &pattern)))
            }
            _ => Err(unsupported(
                "unsupported binary operator in fast-path expressions",
            )),
        },
        Expr::RLike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let value = evaluate_row_expression(expr, schema, ctx)?
                .to_text()
                .unwrap_or_default();
            let pattern = evaluate_row_expression(pattern, schema, ctx)?
                .to_text()
                .unwrap_or_default();
            let matched =
                simple_regexp_match(&value, &pattern) || simple_regexp_match(&pattern, &value);
            Ok(ColumnValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => {
            let lhs = evaluate_row_expression(left, schema, ctx)?;
            let rhs = evaluate_row_expression(right, schema, ctx)?;
            let ColumnValue::Array(values) = rhs else {
                return Err(unsupported("ANY requires an array expression"));
            };
            Ok(ColumnValue::Boolean(
                values
                    .iter()
                    .any(|value| compare_row_values(&lhs, value, compare_op)),
            ))
        }
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => {
            let lhs = evaluate_row_expression(left, schema, ctx)?;
            let rhs = evaluate_row_expression(right, schema, ctx)?;
            let ColumnValue::Array(values) = rhs else {
                return Err(unsupported("ALL requires an array expression"));
            };
            Ok(ColumnValue::Boolean(
                values
                    .iter()
                    .all(|value| compare_row_values(&lhs, value, compare_op)),
            ))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let left = evaluate_row_expression(expr, schema, ctx)?;
            let matched = list.iter().any(|item| {
                evaluate_row_expression(item, schema, ctx).is_ok_and(|value| value == left)
            });
            Ok(ColumnValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = evaluate_row_expression(expr, schema, ctx)?;
            let low = evaluate_row_expression(low, schema, ctx)?;
            let high = evaluate_row_expression(high, schema, ctx)?;
            let inside = compare_row_values(&value, &low, &BinaryOperator::GtEq)
                && compare_row_values(&value, &high, &BinaryOperator::LtEq);
            Ok(ColumnValue::Boolean(if *negated {
                !inside
            } else {
                inside
            }))
        }
        Expr::IsNull(expr) => Ok(ColumnValue::Boolean(
            evaluate_row_expression(expr, schema, ctx)?.is_null(),
        )),
        Expr::IsNotNull(expr) => Ok(ColumnValue::Boolean(
            !evaluate_row_expression(expr, schema, ctx)?.is_null(),
        )),
        Expr::Like {
            expr,
            pattern,
            escape_char,
            negated,
            ..
        } => {
            let left = evaluate_row_expression(expr, schema, ctx)?;
            let pattern = evaluate_row_expression(pattern, schema, ctx)?;
            let matched = matches_like(
                &left.to_text(),
                &pattern.to_text(),
                escape_char_value(escape_char)?,
                false,
            );
            Ok(ColumnValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::ILike {
            expr,
            pattern,
            escape_char,
            negated,
            ..
        } => {
            let left = evaluate_row_expression(expr, schema, ctx)?;
            let pattern = evaluate_row_expression(pattern, schema, ctx)?;
            let matched = matches_like(
                &left.to_text(),
                &pattern.to_text(),
                escape_char_value(escape_char)?,
                true,
            );
            Ok(ColumnValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => eval_substring(
            evaluate_row_expression(expr, schema, ctx)?,
            substring_from
                .as_ref()
                .map(|expr| evaluate_row_expression(expr, schema, ctx))
                .transpose()?,
            substring_for
                .as_ref()
                .map(|expr| evaluate_row_expression(expr, schema, ctx))
                .transpose()?,
        ),
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => eval_trim(
            evaluate_row_expression(expr, schema, ctx)?,
            *trim_where,
            if let Some(expr) = trim_what {
                Some(evaluate_row_expression(expr, schema, ctx)?)
            } else if let Some(chars) = trim_characters {
                chars
                    .first()
                    .map(|expr| evaluate_row_expression(expr, schema, ctx))
                    .transpose()?
            } else {
                None
            },
        ),
        Expr::Ceil { expr, .. } => {
            eval_numeric_unary(evaluate_row_expression(expr, schema, ctx)?, f64::ceil)
        }
        Expr::Floor { expr, .. } => {
            eval_numeric_unary(evaluate_row_expression(expr, schema, ctx)?, f64::floor)
        }
        Expr::Extract { field, expr, .. } => {
            eval_extract(field, evaluate_row_expression(expr, schema, ctx)?)
        }
        Expr::Interval(interval) => {
            let value = evaluate_row_expression(&interval.value, schema, ctx)?
                .to_text()
                .unwrap_or_default();
            let unit = interval
                .leading_field
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "DAY".to_string());
            Ok(ColumnValue::Text(format!("{value} {unit}")))
        }
        Expr::Collate { expr, .. } => evaluate_row_expression(expr, schema, ctx),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand_value = operand
                .as_ref()
                .map(|expr| evaluate_row_expression(expr, schema, ctx))
                .transpose()?;
            for branch in conditions {
                let matched = if let Some(operand_value) = &operand_value {
                    let condition = evaluate_row_expression(&branch.condition, schema, ctx)?;
                    compare_row_values(operand_value, &condition, &BinaryOperator::Eq)
                } else {
                    evaluate_row_bool(&branch.condition, schema, ctx)?
                };
                if matched {
                    return evaluate_row_expression(&branch.result, schema, ctx);
                }
            }
            match else_result {
                Some(expr) => evaluate_row_expression(expr, schema, ctx),
                None => Ok(ColumnValue::Null),
            }
        }
        Expr::Nested(inner) => evaluate_row_expression(inner, schema, ctx),
        Expr::Array(array) => array
            .elem
            .iter()
            .map(|expr| evaluate_row_expression(expr, schema, ctx))
            .collect::<PgWireResult<Vec<_>>>()
            .map(ColumnValue::Array),
        _ => Err(unsupported("unsupported expression in fast-path DML")),
    }
}

pub fn evaluate_row_bool(
    expr: &Expr,
    schema: &TableSchema,
    ctx: &EvalContext<'_>,
) -> PgWireResult<bool> {
    let value = evaluate_row_expression(expr, schema, ctx)?;
    match value {
        ColumnValue::Boolean(value) => Ok(value),
        ColumnValue::Null => Ok(false),
        ColumnValue::Int16(value) => Ok(value != 0),
        ColumnValue::Int32(value) => Ok(value != 0),
        ColumnValue::Int64(value) => Ok(value != 0),
        ColumnValue::Float32(value) => Ok(value != 0.0),
        ColumnValue::Float64(value) => Ok(value != 0.0),
        ColumnValue::Numeric(value) | ColumnValue::Text(value) => {
            Ok(mysql_text_to_number(&value) != 0.0)
        }
        _ => Err(unsupported("expression is not boolean")),
    }
}

pub(super) fn evaluate_function_args(
    args: FunctionArguments,
    schema: &TableSchema,
    ctx: &EvalContext<'_>,
) -> PgWireResult<Vec<ColumnValue>> {
    function_args(args)
        .into_iter()
        .map(|arg| {
            let expr = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
                FunctionArg::Named { arg, .. } => match arg {
                    FunctionArgExpr::Expr(expr) => expr,
                    _ => return Err(unsupported("unsupported function argument")),
                },
                _ => return Err(unsupported("unsupported function argument")),
            };
            evaluate_row_expression(&expr, schema, ctx)
        })
        .collect()
}

pub(super) fn evaluate_scalar_function(
    name: &str,
    args: Vec<ColumnValue>,
) -> PgWireResult<ColumnValue> {
    let name = name.to_ascii_lowercase();
    match name.as_str() {
        "lower" => unary_text(args, |s| s.to_lowercase()),
        "upper" => unary_text(args, |s| s.to_uppercase()),
        "length" => {
            let text = exactly_one(args, "length")?;
            Ok(ColumnValue::Int32(
                text.to_text().unwrap_or_default().chars().count() as i32,
            ))
        }
        "replace" => {
            if args.len() != 3 {
                return Err(unsupported("replace requires 3 arguments"));
            }
            let source = args[0].to_text().unwrap_or_default();
            let from = args[1].to_text().unwrap_or_default();
            let to = args[2].to_text().unwrap_or_default();
            Ok(ColumnValue::Text(source.replace(&from, &to)))
        }
        "concat" => Ok(ColumnValue::Text(
            args.into_iter()
                .filter_map(|value| value.to_text())
                .collect::<Vec<_>>()
                .join(""),
        )),
        "concat_ws" => eval_concat_ws(args),
        "substring_index" => eval_substring_index(args),
        "locate" | "instr" => eval_locate(name.as_str(), args),
        "field" => eval_field(args),
        "elt" => eval_elt(args),
        "find_in_set" => eval_find_in_set(args),
        "format" => eval_format(args),
        "regexp_like" => eval_regexp_like(args),
        "regexp_replace" => eval_regexp_replace(args),
        "charset" => Ok(ColumnValue::Text("utf8mb4".to_string())),
        "collation" => Ok(ColumnValue::Text("utf8mb4_0900_ai_ci".to_string())),
        "abs" => eval_abs(exactly_one(args, "abs")?),
        "round" => eval_numeric_unary(exactly_one(args, "round")?, f64::round),
        "ceil" | "ceiling" => eval_numeric_unary(exactly_one(args, "ceil")?, f64::ceil),
        "floor" => eval_numeric_unary(exactly_one(args, "floor")?, f64::floor),
        "curdate" | "current_date" => Ok(ColumnValue::Date(
            current_timestamp_text()[..10].to_string(),
        )),
        "curtime" | "current_time" => Ok(ColumnValue::Text(
            current_timestamp_text()[11..19].to_string(),
        )),
        "date_add" | "date_sub" => eval_date_add_sub(args, name == "date_sub"),
        "timestampadd" => eval_timestampadd(args),
        "timestampdiff" => eval_timestampdiff(args),
        "date_format" => eval_date_format(args),
        "str_to_date" => eval_str_to_date(args),
        "unix_timestamp" => eval_unix_timestamp(args),
        "from_unixtime" => eval_from_unixtime(args),
        "last_day" => eval_last_day(args),
        "week" => eval_week(args),
        "date_trunc" => {
            if args.len() != 2 {
                return Err(unsupported("date_trunc requires 2 arguments"));
            }
            eval_date_trunc(args[0].to_text().unwrap_or_default(), args[1].clone())
        }
        "json_extract" => eval_json_extract(args, false),
        "json_unquote" => eval_json_unquote(args),
        "json_contains" => eval_json_contains(args),
        "json_set" => eval_json_mutate(args, JsonMutation::Set),
        "json_replace" => eval_json_mutate(args, JsonMutation::Replace),
        "json_remove" => eval_json_mutate(args, JsonMutation::Remove),
        "json_object" => eval_json_object(args),
        "json_array" => eval_json_array(args),
        "last_insert_id" => Ok(ColumnValue::Int64(0)),
        "row_count" => Ok(ColumnValue::Int64(0)),
        "connection_id" => Ok(ColumnValue::Int64(0)),
        "current_user" | "user" | "session_user" => {
            Ok(ColumnValue::Text("root@localhost".to_string()))
        }
        "any_value" => Ok(args.into_iter().next().unwrap_or(ColumnValue::Null)),
        "nullif" => {
            if args.len() != 2 {
                return Err(unsupported("nullif requires 2 arguments"));
            }
            if compare_row_values(&args[0], &args[1], &BinaryOperator::Eq) {
                Ok(ColumnValue::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "greatest" => args
            .into_iter()
            .filter(|value| !value.is_null())
            .reduce(|best, value| {
                if value.partial_cmp(&best) == Some(Ordering::Greater) {
                    value
                } else {
                    best
                }
            })
            .map_or(Ok(ColumnValue::Null), Ok),
        "least" => args
            .into_iter()
            .filter(|value| !value.is_null())
            .reduce(|best, value| {
                if value.partial_cmp(&best) == Some(Ordering::Less) {
                    value
                } else {
                    best
                }
            })
            .map_or(Ok(ColumnValue::Null), Ok),
        _ => Err(unsupported(format!(
            "function '{name}' is not supported in fast-path DML expressions"
        ))),
    }
}

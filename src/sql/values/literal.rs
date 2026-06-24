use pgwire::error::PgWireResult;
use sqlparser::ast::{Expr, TypedString, Value as SqlValue};

use super::{coerce_column_value, current_timestamp_text, parse_text_for_type};
use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, DataType};

pub fn sql_expr_to_column_value(expr: &Expr, data_type: &DataType) -> PgWireResult<ColumnValue> {
    match expr {
        Expr::Cast {
            expr,
            data_type: cast_type,
            ..
        } => {
            let casted_type = DataType::from_sql(&cast_type.to_string());
            let value = sql_expr_to_column_value(expr, &casted_type)?;
            coerce_column_value(value, data_type)
        }
        Expr::TypedString(TypedString {
            value,
            data_type: typed_data_type,
            ..
        }) => {
            let casted_type = DataType::from_sql(&typed_data_type.to_string());
            let value = sql_expr_to_column_value(&Expr::Value(value.clone()), &casted_type)?;
            coerce_column_value(value, data_type)
        }
        Expr::Function(function) if is_current_timestamp_function(&function.name.to_string()) => {
            coerce_column_value(ColumnValue::Text(current_timestamp_text()), data_type)
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_timestamp") => {
            coerce_column_value(ColumnValue::Text(current_timestamp_text()), data_type)
        }
        Expr::Value(value) => {
            match &value.value {
                SqlValue::Number(v, _) => match data_type {
                    DataType::Int16 => v.parse::<i16>().map(ColumnValue::Int16).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT2"))
                    }),
                    DataType::Int32 => v.parse::<i32>().map(ColumnValue::Int32).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT4"))
                    }),
                    DataType::Int64 => v.parse::<i64>().map(ColumnValue::Int64).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT8"))
                    }),
                    DataType::Float32 => v.parse::<f32>().map(ColumnValue::Float32).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for FLOAT4"))
                    }),
                    DataType::Float64 => v.parse::<f64>().map(ColumnValue::Float64).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for FLOAT8"))
                    }),
                    DataType::MySqlInt { .. }
                    | DataType::Bit(_)
                    | DataType::Year
                    | DataType::Numeric { .. } => parse_text_for_type(v, data_type),
                    _ => parse_text_for_type(v, data_type),
                },
                SqlValue::SingleQuotedString(v) | SqlValue::DoubleQuotedString(v) => {
                    parse_text_for_type(v, data_type)
                }
                SqlValue::Boolean(v) => coerce_column_value(ColumnValue::Boolean(*v), data_type),
                SqlValue::Null => Ok(ColumnValue::Null),
                _ => Err(unsupported("only literal values are supported")),
            }
        }
        Expr::Array(array) => match data_type {
            DataType::Array(inner) => array
                .elem
                .iter()
                .map(|expr| sql_expr_to_column_value(expr, inner))
                .collect::<PgWireResult<Vec<_>>>()
                .map(ColumnValue::Array),
            _ => Err(unsupported("ARRAY literal requires an array column type")),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(value) if matches!(&value.value, SqlValue::Number(_, _)) => {
                let SqlValue::Number(v, _) = &value.value else {
                    unreachable!();
                };
                let neg = format!("-{v}");
                match data_type {
                    DataType::Int16 => neg.parse::<i16>().map(ColumnValue::Int16).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT2"))
                    }),
                    DataType::Int32 => neg.parse::<i32>().map(ColumnValue::Int32).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT4"))
                    }),
                    DataType::Int64 => neg.parse::<i64>().map(ColumnValue::Int64).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT8"))
                    }),
                    DataType::Float32 => {
                        neg.parse::<f32>().map(ColumnValue::Float32).map_err(|_| {
                            user_error("22003", format!("value '{neg}' out of range for FLOAT4"))
                        })
                    }
                    DataType::Float64 => {
                        neg.parse::<f64>().map(ColumnValue::Float64).map_err(|_| {
                            user_error("22003", format!("value '{neg}' out of range for FLOAT8"))
                        })
                    }
                    DataType::MySqlInt { .. } => parse_text_for_type(&neg, data_type),
                    _ => parse_text_for_type(&neg, data_type),
                }
            }
            _ => Err(unsupported("only literal values are supported")),
        },
        _ => Err(unsupported("only literal values are supported")),
    }
}

fn is_current_timestamp_function(function_name: &str) -> bool {
    function_name.eq_ignore_ascii_case("current_timestamp")
        || function_name.eq_ignore_ascii_case("now")
}

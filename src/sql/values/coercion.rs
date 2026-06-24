use pgwire::error::PgWireResult;

use super::{
    coerce_binary, coerce_blob, coerce_char, coerce_decimal_text, coerce_enum,
    coerce_mysql_integer, coerce_mysql_temporal, coerce_mysql_text, coerce_set, coerce_varbinary,
    coerce_varchar, parse_bit_value, parse_bool_text, parse_bytea, parse_int16_text,
    parse_int32_text, parse_int64_text, parse_text_for_type, parse_year_value,
};
use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, DataType};

pub fn coerce_column_value(value: ColumnValue, data_type: &DataType) -> PgWireResult<ColumnValue> {
    match (value, data_type) {
        (ColumnValue::Null, _) => Ok(ColumnValue::Null),
        (value, DataType::MySqlInt { kind, unsigned }) => {
            coerce_mysql_integer(value, *kind, *unsigned)
        }
        (ColumnValue::Int16(v), DataType::Int16) => Ok(ColumnValue::Int16(v)),
        (ColumnValue::Int16(v), DataType::Int32) => Ok(ColumnValue::Int32(v as i32)),
        (ColumnValue::Int16(v), DataType::Int64) => Ok(ColumnValue::Int64(v as i64)),
        (ColumnValue::Int16(v), DataType::Float32) => Ok(ColumnValue::Float32(v as f32)),
        (ColumnValue::Int16(v), DataType::Float64) => Ok(ColumnValue::Float64(v as f64)),
        (ColumnValue::Int16(v), DataType::Numeric { .. }) => {
            coerce_decimal_text(v.to_string(), data_type)
        }
        (
            ColumnValue::Int16(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Int32(v), DataType::Int32) => Ok(ColumnValue::Int32(v)),
        (ColumnValue::Int32(v), DataType::Int16) => i16::try_from(v)
            .map(ColumnValue::Int16)
            .map_err(|_| user_error("22003", format!("value '{v}' out of range for INT2"))),
        (ColumnValue::Int32(v), DataType::Int64) => Ok(ColumnValue::Int64(v as i64)),
        (ColumnValue::Int32(v), DataType::Float32) => Ok(ColumnValue::Float32(v as f32)),
        (ColumnValue::Int32(v), DataType::Float64) => Ok(ColumnValue::Float64(v as f64)),
        (ColumnValue::Int32(v), DataType::Numeric { .. }) => {
            coerce_decimal_text(v.to_string(), data_type)
        }
        (
            ColumnValue::Int32(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Int64(v), DataType::Int64) => Ok(ColumnValue::Int64(v)),
        (ColumnValue::Int64(v), DataType::Int16) => i16::try_from(v)
            .map(ColumnValue::Int16)
            .map_err(|_| user_error("22003", format!("value '{v}' out of range for INT2"))),
        (ColumnValue::Int64(v), DataType::Int32) => i32::try_from(v)
            .map(ColumnValue::Int32)
            .map_err(|_| user_error("22003", format!("value '{v}' out of range for INT4"))),
        (ColumnValue::Int64(v), DataType::Float32) => Ok(ColumnValue::Float32(v as f32)),
        (ColumnValue::Int64(v), DataType::Float64) => Ok(ColumnValue::Float64(v as f64)),
        (ColumnValue::Int64(v), DataType::Numeric { .. }) => {
            coerce_decimal_text(v.to_string(), data_type)
        }
        (
            ColumnValue::Int64(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Float32(v), DataType::Float32) => Ok(ColumnValue::Float32(v)),
        (ColumnValue::Float32(v), DataType::MySqlFloat { .. }) => Ok(ColumnValue::Float32(v)),
        (ColumnValue::Float32(v), DataType::Int16) => {
            if v < i16::MIN as f32 || v > i16::MAX as f32 {
                return Err(user_error(
                    "22003",
                    format!("value '{v}' out of range for INT2"),
                ));
            }
            Ok(ColumnValue::Int16(v as i16))
        }
        (ColumnValue::Float32(v), DataType::Int32) => {
            if v < i32::MIN as f32 || v > i32::MAX as f32 {
                return Err(user_error(
                    "22003",
                    format!("value '{v}' out of range for INT4"),
                ));
            }
            Ok(ColumnValue::Int32(v as i32))
        }
        (ColumnValue::Float32(v), DataType::Int64) => Ok(ColumnValue::Int64(v as i64)),
        (ColumnValue::Float32(v), DataType::Float64) => Ok(ColumnValue::Float64(v as f64)),
        (ColumnValue::Float32(v), DataType::Numeric { .. }) => {
            coerce_decimal_text(v.to_string(), data_type)
        }
        (
            ColumnValue::Float32(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Float64(v), DataType::Float64) => Ok(ColumnValue::Float64(v)),
        (ColumnValue::Float64(v), DataType::MySqlDouble { .. }) => Ok(ColumnValue::Float64(v)),
        (ColumnValue::Float64(v), DataType::Int16) => {
            if v < i16::MIN as f64 || v > i16::MAX as f64 {
                return Err(user_error(
                    "22003",
                    format!("value '{v}' out of range for INT2"),
                ));
            }
            Ok(ColumnValue::Int16(v as i16))
        }
        (ColumnValue::Float64(v), DataType::Int32) => {
            if v < i32::MIN as f64 || v > i32::MAX as f64 {
                return Err(user_error(
                    "22003",
                    format!("value '{v}' out of range for INT4"),
                ));
            }
            Ok(ColumnValue::Int32(v as i32))
        }
        (ColumnValue::Float64(v), DataType::Int64) => Ok(ColumnValue::Int64(v as i64)),
        (ColumnValue::Float64(v), DataType::Float32 | DataType::MySqlFloat { .. }) => {
            Ok(ColumnValue::Float32(v as f32))
        }
        (ColumnValue::Float64(v), DataType::Numeric { .. }) => {
            coerce_decimal_text(v.to_string(), data_type)
        }
        (
            ColumnValue::Float64(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Numeric(v), DataType::Numeric { .. }) => coerce_decimal_text(v, data_type),
        (ColumnValue::Numeric(v), DataType::Float64) => v
            .parse::<f64>()
            .map(ColumnValue::Float64)
            .map_err(|_| user_error("22P02", format!("invalid input for FLOAT8: '{v}'"))),
        (ColumnValue::Numeric(v), DataType::MySqlDouble { .. }) => v
            .parse::<f64>()
            .map(ColumnValue::Float64)
            .map_err(|_| user_error("22P02", format!("invalid input for DOUBLE: '{v}'"))),
        (ColumnValue::Numeric(v), DataType::Float32 | DataType::MySqlFloat { .. }) => v
            .parse::<f32>()
            .map(ColumnValue::Float32)
            .map_err(|_| user_error("22P02", format!("invalid input for FLOAT4: '{v}'"))),
        (
            ColumnValue::Numeric(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v)),
        (ColumnValue::Text(v), DataType::Text) => Ok(ColumnValue::Text(v)),
        (ColumnValue::Text(v), DataType::MySqlText { max_len, type_name }) => {
            coerce_mysql_text(v, *max_len, type_name)
        }
        (ColumnValue::Text(v), DataType::VarChar(limit)) => coerce_varchar(v, *limit),
        (ColumnValue::Text(v), DataType::Char(limit)) => coerce_char(v, *limit),
        (ColumnValue::Text(v), DataType::Binary(limit)) => coerce_binary(parse_bytea(&v)?, *limit),
        (ColumnValue::Text(v), DataType::VarBinary(limit)) => {
            coerce_varbinary(parse_bytea(&v)?, *limit)
        }
        (ColumnValue::Text(v), DataType::Blob { max_len, type_name }) => {
            coerce_blob(parse_bytea(&v)?, *max_len, type_name)
        }
        (ColumnValue::Text(v), DataType::Bit(limit)) => parse_bit_value(&v, *limit),
        (ColumnValue::Text(v), DataType::Year) => parse_year_value(&v),
        (ColumnValue::Text(v), DataType::Enum(values)) => coerce_enum(v, values),
        (ColumnValue::Text(v), DataType::Set(values)) => coerce_set(v, values),
        (ColumnValue::Text(v), DataType::Int16) => parse_int16_text(&v),
        (ColumnValue::Text(v), DataType::Int32) => parse_int32_text(&v),
        (ColumnValue::Text(v), DataType::Int64) => parse_int64_text(&v),
        (ColumnValue::Text(v), DataType::Boolean) => parse_bool_text(&v),
        (ColumnValue::Text(v), other) => parse_text_for_type(&v, other),
        (ColumnValue::Boolean(v), DataType::Boolean) => Ok(ColumnValue::Boolean(v)),
        (
            ColumnValue::Boolean(v),
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v.to_string())),
        (ColumnValue::Date(v), DataType::Date) => Ok(ColumnValue::Date(v)),
        (
            ColumnValue::Date(v),
            DataType::Text | DataType::VarChar(_) | DataType::Char(_) | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v)),
        (ColumnValue::Timestamp(v), DataType::Timestamp) => Ok(ColumnValue::Timestamp(v)),
        (ColumnValue::Timestamp(v), DataType::MySqlDateTime { fsp }) => {
            coerce_mysql_temporal(v, "DATETIME", *fsp).map(ColumnValue::Timestamp)
        }
        (ColumnValue::Timestamp(v), DataType::MySqlTimestamp { fsp }) => {
            coerce_mysql_temporal(v, "TIMESTAMP", *fsp).map(ColumnValue::Timestamp)
        }
        (
            ColumnValue::Timestamp(v),
            DataType::Text | DataType::VarChar(_) | DataType::Char(_) | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v)),
        (ColumnValue::TimestampTz(v), DataType::TimestampTz) => Ok(ColumnValue::TimestampTz(v)),
        (
            ColumnValue::TimestampTz(v),
            DataType::Text | DataType::VarChar(_) | DataType::Char(_) | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v)),
        (ColumnValue::Uuid(v), DataType::Uuid) => Ok(ColumnValue::Uuid(v)),
        (
            ColumnValue::Uuid(v),
            DataType::Text | DataType::VarChar(_) | DataType::Char(_) | DataType::Geometry(_),
        ) => Ok(ColumnValue::Text(v)),
        (ColumnValue::Bytea(v), DataType::Bytea) => Ok(ColumnValue::Bytea(v)),
        (ColumnValue::Bytea(v), DataType::Binary(limit)) => coerce_binary(v, *limit),
        (ColumnValue::Bytea(v), DataType::VarBinary(limit)) => coerce_varbinary(v, *limit),
        (ColumnValue::Bytea(v), DataType::Blob { max_len, type_name }) => {
            coerce_blob(v, *max_len, type_name)
        }
        (ColumnValue::Json(v), DataType::Json) => Ok(ColumnValue::Json(v)),
        (ColumnValue::Json(v), DataType::Jsonb) => Ok(ColumnValue::Jsonb(v)),
        (ColumnValue::Jsonb(v), DataType::Jsonb) => Ok(ColumnValue::Jsonb(v)),
        (ColumnValue::Jsonb(v), DataType::Json) => Ok(ColumnValue::Json(v)),
        (ColumnValue::Array(values), DataType::Array(inner)) => values
            .into_iter()
            .map(|value| coerce_column_value(value, inner))
            .collect::<PgWireResult<Vec<_>>>()
            .map(ColumnValue::Array),
        (value, DataType::Domain(_)) => value
            .to_text()
            .map(ColumnValue::Text)
            .ok_or_else(|| unsupported("domain values cannot be NULL here")),
        (other, _) => Err(unsupported(format!(
            "unsupported cast/coercion for value {other:?}"
        ))),
    }
}

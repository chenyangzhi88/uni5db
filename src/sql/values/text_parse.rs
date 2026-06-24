use pgwire::error::PgWireResult;

use super::{
    coerce_binary, coerce_blob, coerce_char, coerce_decimal_text, coerce_enum,
    coerce_mysql_temporal, coerce_mysql_temporal_fraction, coerce_mysql_text, coerce_set,
    coerce_varbinary, coerce_varchar, parse_array_text, parse_bit_value, parse_bool_text,
    parse_bytea, parse_float32_text, parse_float64_text, parse_int16_text, parse_int32_text,
    parse_int64_text, parse_mysql_integer, parse_year_value, validate_date, validate_interval,
    validate_json, validate_time, validate_timestamp, validate_uuid,
};
use crate::types::{ColumnValue, DataType};

pub(crate) fn parse_text_for_type(raw: &str, data_type: &DataType) -> PgWireResult<ColumnValue> {
    match data_type {
        DataType::MySqlInt { kind, unsigned } => parse_mysql_integer(raw, *kind, *unsigned),
        DataType::Int16 => parse_int16_text(raw),
        DataType::Int32 => parse_int32_text(raw),
        DataType::Int64 => parse_int64_text(raw),
        DataType::Float32 | DataType::MySqlFloat { .. } => parse_float32_text(raw),
        DataType::Float64 | DataType::MySqlDouble { .. } => parse_float64_text(raw),
        DataType::Numeric { .. } => coerce_decimal_text(raw.to_string(), data_type),
        DataType::Boolean => parse_bool_text(raw),
        DataType::Date => {
            validate_date(raw)?;
            Ok(ColumnValue::Date(raw.to_string()))
        }
        DataType::Timestamp => {
            validate_timestamp(raw)?;
            Ok(ColumnValue::Timestamp(raw.to_string()))
        }
        DataType::MySqlDateTime { fsp } => {
            coerce_mysql_temporal(raw.to_string(), "DATETIME", *fsp).map(ColumnValue::Timestamp)
        }
        DataType::MySqlTimestamp { fsp } => {
            coerce_mysql_temporal(raw.to_string(), "TIMESTAMP", *fsp).map(ColumnValue::Timestamp)
        }
        DataType::TimestampTz => {
            validate_timestamp(raw)?;
            Ok(ColumnValue::TimestampTz(raw.to_string()))
        }
        DataType::Time => {
            validate_time(raw, false)?;
            Ok(ColumnValue::Text(raw.to_string()))
        }
        DataType::MySqlTime { fsp } => {
            validate_time(raw, false)?;
            coerce_mysql_temporal_fraction(raw.to_string(), "TIME", *fsp).map(ColumnValue::Text)
        }
        DataType::TimeTz => {
            validate_time(raw, true)?;
            Ok(ColumnValue::Text(raw.to_string()))
        }
        DataType::Interval => {
            validate_interval(raw)?;
            Ok(ColumnValue::Text(raw.to_string()))
        }
        DataType::Uuid => {
            validate_uuid(raw)?;
            Ok(ColumnValue::Uuid(raw.to_ascii_lowercase()))
        }
        DataType::Bytea => parse_bytea(raw).map(ColumnValue::Bytea),
        DataType::Binary(limit) => coerce_binary(parse_bytea(raw)?, *limit),
        DataType::VarBinary(limit) => coerce_varbinary(parse_bytea(raw)?, *limit),
        DataType::Blob { max_len, type_name } => {
            coerce_blob(parse_bytea(raw)?, *max_len, type_name)
        }
        DataType::Bit(limit) => parse_bit_value(raw, *limit),
        DataType::Year => parse_year_value(raw),
        DataType::Json => {
            validate_json(raw)?;
            Ok(ColumnValue::Json(raw.to_string()))
        }
        DataType::Jsonb => {
            validate_json(raw)?;
            Ok(ColumnValue::Jsonb(raw.to_string()))
        }
        DataType::Array(inner) => parse_array_text(raw, inner),
        DataType::VarChar(limit) => coerce_varchar(raw.to_string(), *limit),
        DataType::Char(limit) => coerce_char(raw.to_string(), *limit),
        DataType::Enum(values) => coerce_enum(raw.to_string(), values),
        DataType::Set(values) => coerce_set(raw.to_string(), values),
        DataType::Geometry(_) => Ok(ColumnValue::Text(raw.to_string())),
        DataType::Text | DataType::Domain(_) => Ok(ColumnValue::Text(raw.to_string())),
        DataType::MySqlText { max_len, type_name } => {
            coerce_mysql_text(raw.to_string(), *max_len, type_name)
        }
    }
}

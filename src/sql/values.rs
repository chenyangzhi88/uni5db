use crate::types::RowMap;

mod coercion;
mod default_expr;
mod literal;
mod mysql;
mod scalar_parsers;
mod text_parse;

pub use coercion::coerce_column_value;
pub(in crate::sql) use default_expr::{civil_from_days, current_timestamp_text};
pub use default_expr::{column_default_value, is_default_expr, nextval_sequence_name};
pub use literal::sql_expr_to_column_value;
use mysql::{
    coerce_enum, coerce_mysql_integer, coerce_mysql_temporal, coerce_mysql_temporal_fraction,
    coerce_set, parse_bit_value, parse_mysql_integer, parse_year_value,
};
use scalar_parsers::{
    coerce_binary, coerce_blob, coerce_char, coerce_decimal_text, coerce_mysql_text,
    coerce_varbinary, coerce_varchar, parse_array_text, parse_bool_text, parse_bytea,
    parse_float32_text, parse_float64_text, parse_int16_text, parse_int32_text, parse_int64_text,
    validate_date, validate_interval, validate_json, validate_time, validate_timestamp,
    validate_uuid,
};
pub(crate) use text_parse::parse_text_for_type;

#[derive(Clone, Copy)]
pub struct EvalContext<'a> {
    pub row: &'a RowMap,
    pub excluded_row: Option<&'a RowMap>,
}

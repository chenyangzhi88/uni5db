use pgwire::error::PgWireResult;

use super::{validate_date, validate_timestamp};
use crate::error::{unsupported, user_error};
use crate::types::{ColumnValue, MySqlIntKind};

pub(super) fn parse_mysql_integer(
    raw: &str,
    kind: MySqlIntKind,
    unsigned: bool,
) -> PgWireResult<ColumnValue> {
    let value = raw
        .parse::<i128>()
        .map_err(|_| user_error("22P02", format!("invalid integer value '{raw}'")))?;
    mysql_integer_from_i128(value, kind, unsigned)
}

pub(super) fn coerce_mysql_integer(
    value: ColumnValue,
    kind: MySqlIntKind,
    unsigned: bool,
) -> PgWireResult<ColumnValue> {
    let int = match value {
        ColumnValue::Int16(v) => v as i128,
        ColumnValue::Int32(v) => v as i128,
        ColumnValue::Int64(v) => v as i128,
        ColumnValue::Float32(v) => v as i128,
        ColumnValue::Float64(v) => v as i128,
        ColumnValue::Numeric(v) | ColumnValue::Text(v) => {
            return parse_mysql_integer(&v, kind, unsigned);
        }
        ColumnValue::Boolean(v) => i128::from(v),
        other => {
            return Err(unsupported(format!(
                "cannot coerce {other:?} to MySQL integer"
            )));
        }
    };
    mysql_integer_from_i128(int, kind, unsigned)
}

pub(super) fn mysql_integer_from_i128(
    value: i128,
    kind: MySqlIntKind,
    unsigned: bool,
) -> PgWireResult<ColumnValue> {
    let (min, max, type_name) = mysql_integer_range(kind, unsigned);
    if value < min || value > max {
        return Err(user_error(
            "22003",
            format!("value '{value}' out of range for {type_name}"),
        ));
    }
    Ok(match (kind, unsigned) {
        (MySqlIntKind::Tiny, false) | (MySqlIntKind::Small, false) => {
            ColumnValue::Int16(value as i16)
        }
        (MySqlIntKind::Tiny, true)
        | (MySqlIntKind::Small, true)
        | (MySqlIntKind::Medium, _)
        | (MySqlIntKind::Int, false) => ColumnValue::Int32(value as i32),
        (MySqlIntKind::Int, true) | (MySqlIntKind::Big, false) => ColumnValue::Int64(value as i64),
        (MySqlIntKind::Big, true) => ColumnValue::Numeric(value.to_string()),
    })
}

pub(super) fn mysql_integer_range(
    kind: MySqlIntKind,
    unsigned: bool,
) -> (i128, i128, &'static str) {
    match (kind, unsigned) {
        (MySqlIntKind::Tiny, false) => (-128, 127, "TINYINT"),
        (MySqlIntKind::Tiny, true) => (0, 255, "TINYINT UNSIGNED"),
        (MySqlIntKind::Small, false) => (-32_768, 32_767, "SMALLINT"),
        (MySqlIntKind::Small, true) => (0, 65_535, "SMALLINT UNSIGNED"),
        (MySqlIntKind::Medium, false) => (-8_388_608, 8_388_607, "MEDIUMINT"),
        (MySqlIntKind::Medium, true) => (0, 16_777_215, "MEDIUMINT UNSIGNED"),
        (MySqlIntKind::Int, false) => (-2_147_483_648, 2_147_483_647, "INT"),
        (MySqlIntKind::Int, true) => (0, 4_294_967_295, "INT UNSIGNED"),
        (MySqlIntKind::Big, false) => (
            -9_223_372_036_854_775_808,
            9_223_372_036_854_775_807,
            "BIGINT",
        ),
        (MySqlIntKind::Big, true) => (0, 18_446_744_073_709_551_615, "BIGINT UNSIGNED"),
    }
}

pub(super) fn parse_bit_value(raw: &str, limit: Option<u32>) -> PgWireResult<ColumnValue> {
    let max_bits = limit.unwrap_or(1).min(64);
    let value = if let Some(bits) = raw.strip_prefix("b'").and_then(|s| s.strip_suffix('\'')) {
        if bits.len() > max_bits as usize || !bits.bytes().all(|byte| byte == b'0' || byte == b'1')
        {
            return Err(user_error("22003", format!("invalid BIT value '{raw}'")));
        }
        u64::from_str_radix(bits, 2)
            .map_err(|_| user_error("22003", format!("invalid BIT value '{raw}'")))?
    } else {
        let value = raw
            .parse::<u64>()
            .map_err(|_| user_error("22003", format!("invalid BIT value '{raw}'")))?;
        let upper_bound = if max_bits == 64 {
            u64::MAX
        } else {
            (1_u64 << max_bits) - 1
        };
        if value > upper_bound {
            return Err(user_error(
                "22003",
                format!("value '{raw}' out of range for BIT({max_bits})"),
            ));
        }
        value
    };
    if value > i64::MAX as u64 {
        return Err(user_error(
            "22003",
            "BIT(64) values above signed BIGINT are not supported yet",
        ));
    }
    Ok(ColumnValue::Int64(value as i64))
}

pub(super) fn parse_year_value(raw: &str) -> PgWireResult<ColumnValue> {
    let year = raw
        .parse::<i32>()
        .map_err(|_| user_error("22007", format!("invalid YEAR value '{raw}'")))?;
    let normalized = if (0..=69).contains(&year) {
        2000 + year
    } else if (70..=99).contains(&year) {
        1900 + year
    } else {
        year
    };
    if normalized == 0 || (1901..=2155).contains(&normalized) {
        Ok(ColumnValue::Int32(normalized))
    } else {
        Err(user_error(
            "22007",
            format!("YEAR value '{raw}' out of range"),
        ))
    }
}

pub(super) fn coerce_enum(value: String, allowed: &[String]) -> PgWireResult<ColumnValue> {
    if allowed.iter().any(|item| item == &value) {
        Ok(ColumnValue::Text(value))
    } else {
        Err(user_error("22007", format!("invalid ENUM value '{value}'")))
    }
}

pub(super) fn coerce_set(value: String, allowed: &[String]) -> PgWireResult<ColumnValue> {
    if value.is_empty() {
        return Ok(ColumnValue::Text(value));
    }
    let mut seen = std::collections::HashSet::new();
    for item in value.split(',') {
        if !allowed.iter().any(|allowed| allowed == item) || !seen.insert(item) {
            return Err(user_error("22007", format!("invalid SET value '{value}'")));
        }
    }
    Ok(ColumnValue::Text(value))
}

pub(super) fn coerce_mysql_temporal(
    value: String,
    type_name: &str,
    fsp: Option<u32>,
) -> PgWireResult<String> {
    validate_timestamp(&value)?;
    validate_mysql_temporal_date(&value, type_name)?;
    coerce_mysql_temporal_fraction(value, type_name, fsp)
}

pub(super) fn coerce_mysql_temporal_fraction(
    value: String,
    type_name: &str,
    fsp: Option<u32>,
) -> PgWireResult<String> {
    let fsp = fsp.unwrap_or(0);
    if fsp > 6 {
        return Err(user_error(
            "22007",
            format!("{type_name} fractional seconds precision must be between 0 and 6"),
        ));
    }
    if let Some((prefix, frac)) = value.split_once('.') {
        let digits = frac
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect::<String>();
        if digits.len() > fsp as usize {
            return Err(user_error(
                "22007",
                format!("{type_name} value exceeds fractional seconds precision {fsp}"),
            ));
        }
        if fsp == 0 {
            Ok(prefix.to_string())
        } else {
            Ok(value)
        }
    } else {
        Ok(value)
    }
}

pub(super) fn validate_mysql_temporal_date(value: &str, type_name: &str) -> PgWireResult<()> {
    let Some(date) = value.get(0..10) else {
        return Err(user_error(
            "22007",
            format!("invalid {type_name} value '{value}'"),
        ));
    };
    if date == "0000-00-00" {
        return Err(user_error(
            "22007",
            format!("zero date is not accepted for {type_name}"),
        ));
    }
    validate_date(date)
}

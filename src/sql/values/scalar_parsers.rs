use pgwire::error::PgWireResult;

use super::parse_text_for_type;
use crate::error::user_error;
use crate::types::{ColumnValue, DataType};

pub(super) fn parse_int16_text(raw: &str) -> PgWireResult<ColumnValue> {
    raw.parse::<i16>()
        .map(ColumnValue::Int16)
        .map_err(|_| user_error("22P02", format!("invalid input for INT2: '{raw}'")))
}

pub(super) fn parse_int32_text(raw: &str) -> PgWireResult<ColumnValue> {
    raw.parse::<i32>()
        .map(ColumnValue::Int32)
        .map_err(|_| user_error("22P02", format!("invalid input for INT4: '{raw}'")))
}

pub(super) fn parse_int64_text(raw: &str) -> PgWireResult<ColumnValue> {
    raw.parse::<i64>()
        .map(ColumnValue::Int64)
        .map_err(|_| user_error("22P02", format!("invalid input for INT8: '{raw}'")))
}

pub(super) fn parse_float32_text(raw: &str) -> PgWireResult<ColumnValue> {
    raw.parse::<f32>()
        .map(ColumnValue::Float32)
        .map_err(|_| user_error("22P02", format!("invalid input for FLOAT4: '{raw}'")))
}

pub(super) fn parse_float64_text(raw: &str) -> PgWireResult<ColumnValue> {
    raw.parse::<f64>()
        .map(ColumnValue::Float64)
        .map_err(|_| user_error("22P02", format!("invalid input for FLOAT8: '{raw}'")))
}

pub(super) fn parse_bool_text(raw: &str) -> PgWireResult<ColumnValue> {
    match raw {
        "t" | "T" | "true" | "TRUE" | "True" | "1" | "yes" | "YES" | "Yes" | "on" | "ON" | "On" => {
            Ok(ColumnValue::Boolean(true))
        }
        "f" | "F" | "false" | "FALSE" | "False" | "0" | "no" | "NO" | "No" | "off" | "OFF"
        | "Off" => Ok(ColumnValue::Boolean(false)),
        _ => match raw.to_ascii_lowercase().as_str() {
            "true" | "yes" | "on" => Ok(ColumnValue::Boolean(true)),
            "false" | "no" | "off" => Ok(ColumnValue::Boolean(false)),
            _ => Err(user_error(
                "22P02",
                format!("invalid input for BOOL: '{raw}'"),
            )),
        },
    }
}

pub(super) fn coerce_decimal_text(
    value: String,
    data_type: &DataType,
) -> PgWireResult<ColumnValue> {
    value
        .parse::<f64>()
        .map_err(|_| user_error("22P02", format!("invalid input for NUMERIC: '{value}'")))?;
    let DataType::Numeric { precision, scale } = data_type else {
        return Ok(ColumnValue::Numeric(value));
    };
    validate_decimal_precision(&value, *precision, *scale)?;
    Ok(ColumnValue::Numeric(value))
}

pub(super) fn validate_decimal_precision(
    value: &str,
    precision: Option<u32>,
    scale: Option<u32>,
) -> PgWireResult<()> {
    let trimmed = value.trim_start_matches(['+', '-']);
    let mut parts = trimmed.split('.');
    let int = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("");
    let digit_count = int.chars().filter(char::is_ascii_digit).count()
        + frac.chars().filter(char::is_ascii_digit).count();
    if let Some(scale) = scale
        && frac.len() > scale as usize
    {
        return Err(user_error(
            "22003",
            format!("numeric scale exceeds DECIMAL scale {scale}: '{value}'"),
        ));
    }
    if let Some(precision) = precision
        && digit_count > precision as usize
    {
        return Err(user_error(
            "22003",
            format!("numeric precision exceeds DECIMAL precision {precision}: '{value}'"),
        ));
    }
    Ok(())
}

pub(super) fn coerce_varchar(value: String, limit: Option<u32>) -> PgWireResult<ColumnValue> {
    if let Some(limit) = limit
        && value.chars().count() > limit as usize
    {
        return Err(user_error(
            "22001",
            format!("value too long for type character varying({limit})"),
        ));
    }
    Ok(ColumnValue::Text(value))
}

pub(super) fn coerce_char(value: String, limit: Option<u32>) -> PgWireResult<ColumnValue> {
    let Some(limit) = limit else {
        return Ok(ColumnValue::Text(value));
    };
    let len = value.chars().count();
    if len > limit as usize {
        return Err(user_error(
            "22001",
            format!("value too long for type character({limit})"),
        ));
    }
    let mut padded = value;
    padded.extend(std::iter::repeat_n(' ', limit as usize - len));
    Ok(ColumnValue::Text(padded))
}

pub(super) fn coerce_binary(mut value: Vec<u8>, limit: Option<u32>) -> PgWireResult<ColumnValue> {
    let limit = limit.unwrap_or(1) as usize;
    if value.len() > limit {
        return Err(user_error(
            "22001",
            format!("value too long for type binary({limit})"),
        ));
    }
    value.resize(limit, 0);
    Ok(ColumnValue::Bytea(value))
}

pub(super) fn coerce_varbinary(value: Vec<u8>, limit: Option<u32>) -> PgWireResult<ColumnValue> {
    if let Some(limit) = limit
        && value.len() > limit as usize
    {
        return Err(user_error(
            "22001",
            format!("value too long for type varbinary({limit})"),
        ));
    }
    Ok(ColumnValue::Bytea(value))
}

pub(super) fn coerce_blob(
    value: Vec<u8>,
    max_len: Option<u64>,
    type_name: &str,
) -> PgWireResult<ColumnValue> {
    if let Some(max_len) = max_len
        && value.len() as u64 > max_len
    {
        return Err(user_error(
            "22001",
            format!("value too long for type {type_name}"),
        ));
    }
    Ok(ColumnValue::Bytea(value))
}

pub(super) fn coerce_mysql_text(
    value: String,
    max_len: Option<u64>,
    type_name: &str,
) -> PgWireResult<ColumnValue> {
    if let Some(max_len) = max_len
        && value.len() as u64 > max_len
    {
        return Err(user_error(
            "22001",
            format!("value too long for type {type_name}"),
        ));
    }
    Ok(ColumnValue::Text(value))
}

pub(super) fn validate_date(raw: &str) -> PgWireResult<()> {
    let bytes = raw.as_bytes();
    if !(bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, byte)| idx == 4 || idx == 7 || byte.is_ascii_digit()))
    {
        return Err(user_error("22007", format!("invalid DATE value '{raw}'")));
    }
    let year = raw[0..4]
        .parse::<i32>()
        .map_err(|_| user_error("22007", format!("invalid DATE value '{raw}'")))?;
    let month = raw[5..7]
        .parse::<u32>()
        .map_err(|_| user_error("22007", format!("invalid DATE value '{raw}'")))?;
    let day = raw[8..10]
        .parse::<u32>()
        .map_err(|_| user_error("22007", format!("invalid DATE value '{raw}'")))?;
    if year == 0 || !(1..=12).contains(&month) {
        return Err(user_error("22007", format!("invalid DATE value '{raw}'")));
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    };
    if day == 0 || day > max_day {
        return Err(user_error("22007", format!("invalid DATE value '{raw}'")));
    }
    Ok(())
}

pub(super) fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(super) fn validate_timestamp(raw: &str) -> PgWireResult<()> {
    if raw.len() >= 10 {
        validate_date(&raw[..10])
    } else {
        Err(user_error(
            "22007",
            format!("invalid TIMESTAMP value '{raw}'"),
        ))
    }
}

pub(super) fn validate_time(raw: &str, allow_tz: bool) -> PgWireResult<()> {
    let time = raw.split_whitespace().next().unwrap_or(raw);
    let parts = time.split(':').collect::<Vec<_>>();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(user_error("22007", format!("invalid TIME value '{raw}'")));
    }
    let hour = parts[0].parse::<u32>().ok();
    let minute = parts[1].parse::<u32>().ok();
    let second_part = parts.get(2).copied().unwrap_or("0");
    let second_digits = second_part
        .split(['.', '+', '-', 'Z'])
        .next()
        .unwrap_or(second_part);
    let second = second_digits.parse::<u32>().ok();
    if hour.is_some_and(|v| v < 24)
        && minute.is_some_and(|v| v < 60)
        && second.is_some_and(|v| v < 60)
        && (allow_tz || !raw.contains('+') && !raw.ends_with('Z'))
    {
        Ok(())
    } else {
        Err(user_error("22007", format!("invalid TIME value '{raw}'")))
    }
}

pub(super) fn validate_interval(raw: &str) -> PgWireResult<()> {
    if raw.trim().is_empty() {
        Err(user_error("22007", "invalid INTERVAL value"))
    } else {
        Ok(())
    }
}

pub(super) fn validate_uuid(raw: &str) -> PgWireResult<()> {
    let bytes = raw.as_bytes();
    let valid = bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|idx| bytes[*idx] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, byte)| [8, 13, 18, 23].contains(&idx) || byte.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(user_error("22P02", format!("invalid UUID value '{raw}'")))
    }
}

pub(super) fn validate_json(raw: &str) -> PgWireResult<()> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(|_| ())
        .map_err(|e| user_error("22P02", format!("invalid JSON value: {e}")))
}

pub(super) fn parse_bytea(raw: &str) -> PgWireResult<Vec<u8>> {
    let Some(hex) = raw.strip_prefix("\\x").or_else(|| raw.strip_prefix("\\X")) else {
        return Ok(raw.as_bytes().to_vec());
    };
    if hex.len() % 2 != 0 {
        return Err(user_error(
            "22P02",
            format!("invalid BYTEA hex value '{raw}'"),
        ));
    }
    (0..hex.len())
        .step_by(2)
        .map(|idx| {
            u8::from_str_radix(&hex[idx..idx + 2], 16)
                .map_err(|_| user_error("22P02", format!("invalid BYTEA hex value '{raw}'")))
        })
        .collect()
}

pub(super) fn parse_array_text(raw: &str, inner: &DataType) -> PgWireResult<ColumnValue> {
    let Some(body) = raw.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Err(user_error("22P02", format!("invalid ARRAY value '{raw}'")));
    };
    if body.is_empty() {
        return Ok(ColumnValue::Array(Vec::new()));
    }
    parse_array_items(body, raw)?
        .into_iter()
        .map(|item| match item {
            ArrayItem::Null => Ok(ColumnValue::Null),
            ArrayItem::Value(value) => parse_text_for_type(&value, inner),
        })
        .collect::<PgWireResult<Vec<_>>>()
        .map(ColumnValue::Array)
}

enum ArrayItem {
    Null,
    Value(String),
}

fn parse_array_items(body: &str, raw: &str) -> PgWireResult<Vec<ArrayItem>> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quoted = false;
    let mut escaped = false;
    let mut nested_depth = 0_u32;

    for ch in body.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_quotes = !in_quotes;
            quoted = true;
            continue;
        }
        if !in_quotes {
            match ch {
                '{' => {
                    nested_depth += 1;
                    current.push(ch);
                    continue;
                }
                '}' => {
                    if nested_depth == 0 {
                        return Err(user_error("22P02", format!("invalid ARRAY value '{raw}'")));
                    }
                    nested_depth -= 1;
                    current.push(ch);
                    continue;
                }
                ',' if nested_depth == 0 => {
                    items.push(array_item_from_text(&current, quoted));
                    current.clear();
                    quoted = false;
                    continue;
                }
                _ => {}
            }
        }
        current.push(ch);
    }

    if escaped || in_quotes || nested_depth != 0 {
        return Err(user_error("22P02", format!("invalid ARRAY value '{raw}'")));
    }
    items.push(array_item_from_text(&current, quoted));
    Ok(items)
}

fn array_item_from_text(value: &str, quoted: bool) -> ArrayItem {
    if quoted {
        ArrayItem::Value(value.to_string())
    } else {
        let value = value.trim();
        if value.eq_ignore_ascii_case("NULL") {
            ArrayItem::Null
        } else {
            ArrayItem::Value(value.to_string())
        }
    }
}

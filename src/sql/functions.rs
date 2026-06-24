use pgwire::error::PgWireResult;
use sqlparser::ast::{DateTimeField, TrimWhereField};

use super::json::{
    add_interval_to_date, days_from_civil, days_in_month, parse_ymd, simple_regexp_match,
    value_to_i64,
};
use super::operators::mysql_value_to_number;
use super::values::{civil_from_days, current_timestamp_text};
use crate::error::{unsupported, user_error};
use crate::types::ColumnValue;

pub(super) fn exactly_one(mut args: Vec<ColumnValue>, name: &str) -> PgWireResult<ColumnValue> {
    if args.len() != 1 {
        return Err(unsupported(format!("{name} requires 1 argument")));
    }
    Ok(args.remove(0))
}

pub(super) fn unary_text(
    args: Vec<ColumnValue>,
    f: impl FnOnce(String) -> String,
) -> PgWireResult<ColumnValue> {
    Ok(ColumnValue::Text(f(exactly_one(args, "text function")?
        .to_text()
        .unwrap_or_default())))
}

pub(super) fn eval_concat_ws(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.is_empty() {
        return Err(unsupported("concat_ws requires at least 1 argument"));
    }
    let sep = args[0].to_text().unwrap_or_default();
    Ok(ColumnValue::Text(
        args.into_iter()
            .skip(1)
            .filter_map(|value| value.to_text())
            .collect::<Vec<_>>()
            .join(&sep),
    ))
}

pub(super) fn eval_substring_index(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 3 {
        return Err(unsupported("substring_index requires 3 arguments"));
    }
    let text = args[0].to_text().unwrap_or_default();
    let delim = args[1].to_text().unwrap_or_default();
    let count = value_to_i64(&args[2])?;
    if delim.is_empty() || count == 0 {
        return Ok(ColumnValue::Text(String::new()));
    }
    let parts = text.split(&delim).collect::<Vec<_>>();
    let result = if count > 0 {
        parts
            .into_iter()
            .take(count as usize)
            .collect::<Vec<_>>()
            .join(&delim)
    } else {
        let take = (-count) as usize;
        parts
            .iter()
            .skip(parts.len().saturating_sub(take))
            .copied()
            .collect::<Vec<_>>()
            .join(&delim)
    };
    Ok(ColumnValue::Text(result))
}

pub(super) fn eval_locate(name: &str, args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    let (needle, haystack, start) = match (name, args.as_slice()) {
        ("instr", [haystack, needle]) => (
            needle.to_text().unwrap_or_default(),
            haystack.to_text().unwrap_or_default(),
            1usize,
        ),
        (_, [needle, haystack]) => (
            needle.to_text().unwrap_or_default(),
            haystack.to_text().unwrap_or_default(),
            1usize,
        ),
        (_, [needle, haystack, start]) => (
            needle.to_text().unwrap_or_default(),
            haystack.to_text().unwrap_or_default(),
            value_to_i64(start)?.max(1) as usize,
        ),
        _ => return Err(unsupported("locate/instr argument count is invalid")),
    };
    let suffix = haystack
        .chars()
        .skip(start.saturating_sub(1))
        .collect::<String>();
    let pos = suffix
        .find(&needle)
        .map(|idx| start + suffix[..idx].chars().count())
        .unwrap_or(0);
    Ok(ColumnValue::Int32(pos as i32))
}

pub(super) fn eval_field(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.is_empty() {
        return Err(unsupported("field requires arguments"));
    }
    let needle = args[0].to_text().unwrap_or_default();
    let pos = args
        .iter()
        .skip(1)
        .position(|value| value.to_text().as_deref() == Some(&needle))
        .map(|idx| idx + 1)
        .unwrap_or(0);
    Ok(ColumnValue::Int32(pos as i32))
}

pub(super) fn eval_elt(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.is_empty() {
        return Err(unsupported("elt requires arguments"));
    }
    let idx = value_to_i64(&args[0])?;
    if idx <= 0 {
        return Ok(ColumnValue::Null);
    }
    Ok(args.get(idx as usize).cloned().unwrap_or(ColumnValue::Null))
}

pub(super) fn eval_find_in_set(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 2 {
        return Err(unsupported("find_in_set requires 2 arguments"));
    }
    let needle = args[0].to_text().unwrap_or_default();
    let set = args[1].to_text().unwrap_or_default();
    let pos = set
        .split(',')
        .position(|item| item == needle)
        .map(|idx| idx + 1)
        .unwrap_or(0);
    Ok(ColumnValue::Int32(pos as i32))
}

pub(super) fn eval_format(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() < 2 {
        return Err(unsupported("format requires at least 2 arguments"));
    }
    let value = mysql_value_to_number(&args[0]).unwrap_or(0.0);
    let decimals = value_to_i64(&args[1])?.clamp(0, 30) as usize;
    Ok(ColumnValue::Text(format!("{value:.decimals$}")))
}

pub(super) fn eval_regexp_like(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() < 2 {
        return Err(unsupported("regexp_like requires at least 2 arguments"));
    }
    Ok(ColumnValue::Boolean(simple_regexp_match(
        &args[0].to_text().unwrap_or_default(),
        &args[1].to_text().unwrap_or_default(),
    )))
}

pub(super) fn eval_regexp_replace(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() < 3 {
        return Err(unsupported("regexp_replace requires at least 3 arguments"));
    }
    let text = args[0].to_text().unwrap_or_default();
    let pattern = args[1].to_text().unwrap_or_default();
    let repl = args[2].to_text().unwrap_or_default();
    Ok(ColumnValue::Text(text.replace(&pattern, &repl)))
}

pub(super) fn eval_abs(value: ColumnValue) -> PgWireResult<ColumnValue> {
    match value {
        ColumnValue::Int16(v) => Ok(ColumnValue::Int16(v.abs())),
        ColumnValue::Int32(v) => Ok(ColumnValue::Int32(v.abs())),
        ColumnValue::Int64(v) => Ok(ColumnValue::Int64(v.abs())),
        ColumnValue::Float32(v) => Ok(ColumnValue::Float32(v.abs())),
        ColumnValue::Float64(v) => Ok(ColumnValue::Float64(v.abs())),
        ColumnValue::Numeric(v) => v
            .parse::<f64>()
            .map(|value| ColumnValue::Numeric(value.abs().to_string()))
            .map_err(|_| user_error("22003", "numeric out of range")),
        other => Err(unsupported(format!("abs does not support {other:?}"))),
    }
}

pub(super) fn eval_numeric_unary(
    value: ColumnValue,
    f: impl FnOnce(f64) -> f64,
) -> PgWireResult<ColumnValue> {
    match value {
        ColumnValue::Int16(v) => Ok(ColumnValue::Float64(f(v as f64))),
        ColumnValue::Int32(v) => Ok(ColumnValue::Float64(f(v as f64))),
        ColumnValue::Int64(v) => Ok(ColumnValue::Float64(f(v as f64))),
        ColumnValue::Float32(v) => Ok(ColumnValue::Float32(f(v as f64) as f32)),
        ColumnValue::Float64(v) => Ok(ColumnValue::Float64(f(v))),
        ColumnValue::Numeric(v) => v
            .parse::<f64>()
            .map(|value| ColumnValue::Numeric(f(value).to_string()))
            .map_err(|_| user_error("22003", "numeric out of range")),
        other => Err(unsupported(format!(
            "numeric function does not support {other:?}"
        ))),
    }
}

pub(super) fn eval_substring(
    value: ColumnValue,
    from: Option<ColumnValue>,
    len: Option<ColumnValue>,
) -> PgWireResult<ColumnValue> {
    let text = value.to_text().unwrap_or_default();
    let start = from
        .and_then(|v| v.to_text())
        .and_then(|v| v.parse::<isize>().ok())
        .unwrap_or(1);
    let start = start.max(1) as usize - 1;
    let iter = text.chars().skip(start);
    let result = match len
        .and_then(|v| v.to_text())
        .and_then(|v| v.parse::<usize>().ok())
    {
        Some(len) => iter.take(len).collect(),
        None => iter.collect(),
    };
    Ok(ColumnValue::Text(result))
}

pub(super) fn eval_trim(
    value: ColumnValue,
    side: Option<TrimWhereField>,
    chars: Option<ColumnValue>,
) -> PgWireResult<ColumnValue> {
    let text = value.to_text().unwrap_or_default();
    let chars = chars
        .and_then(|v| v.to_text())
        .unwrap_or_else(|| " ".to_string());
    let should_trim = |ch: char| chars.contains(ch);
    let trimmed = match side.unwrap_or(TrimWhereField::Both) {
        TrimWhereField::Both => text.trim_matches(should_trim).to_string(),
        TrimWhereField::Leading => text.trim_start_matches(should_trim).to_string(),
        TrimWhereField::Trailing => text.trim_end_matches(should_trim).to_string(),
    };
    Ok(ColumnValue::Text(trimmed))
}

pub(super) fn eval_date_trunc(field: String, value: ColumnValue) -> PgWireResult<ColumnValue> {
    let text = value.to_text().unwrap_or_default();
    let normalized = field.to_ascii_lowercase();
    let result = match normalized.as_str() {
        "year" => format!("{}-01-01 00:00:00", text.get(0..4).unwrap_or("1970")),
        "month" => format!("{}-01 00:00:00", text.get(0..7).unwrap_or("1970-01")),
        "day" => format!("{} 00:00:00", text.get(0..10).unwrap_or("1970-01-01")),
        "hour" => format!("{}:00:00", text.get(0..13).unwrap_or("1970-01-01 00")),
        "minute" => format!("{}:00", text.get(0..16).unwrap_or("1970-01-01 00:00")),
        "second" => text.get(0..19).unwrap_or(&text).to_string(),
        _ => {
            return Err(unsupported(format!(
                "unsupported date_trunc field '{field}'"
            )));
        }
    };
    Ok(match value {
        ColumnValue::TimestampTz(_) => ColumnValue::TimestampTz(result),
        ColumnValue::Timestamp(_) => ColumnValue::Timestamp(result),
        _ => ColumnValue::Text(result),
    })
}

pub(super) fn eval_extract(field: &DateTimeField, value: ColumnValue) -> PgWireResult<ColumnValue> {
    let text = value.to_text().unwrap_or_default();
    let part = field.to_string().to_ascii_lowercase();
    let parsed = match part.as_str() {
        "year" | "years" => text.get(0..4).and_then(|v| v.parse::<i32>().ok()),
        "month" | "months" => text.get(5..7).and_then(|v| v.parse::<i32>().ok()),
        "day" | "days" => text.get(8..10).and_then(|v| v.parse::<i32>().ok()),
        "hour" | "hours" => text.get(11..13).and_then(|v| v.parse::<i32>().ok()),
        "minute" | "minutes" => text.get(14..16).and_then(|v| v.parse::<i32>().ok()),
        "second" | "seconds" => text.get(17..19).and_then(|v| v.parse::<i32>().ok()),
        _ => return Err(unsupported(format!("unsupported extract field '{field}'"))),
    };
    parsed
        .map(ColumnValue::Int32)
        .ok_or_else(|| user_error("22007", format!("cannot extract {field} from '{text}'")))
}

pub(super) fn eval_date_add_sub(
    args: Vec<ColumnValue>,
    subtract: bool,
) -> PgWireResult<ColumnValue> {
    if args.len() != 2 {
        return Err(unsupported("date_add/date_sub requires 2 arguments"));
    }
    let date = args[0].to_text().unwrap_or_default();
    let interval = args[1].to_text().unwrap_or_default();
    add_interval_to_date(&date, &interval, subtract).map(ColumnValue::Text)
}

pub(super) fn eval_timestampadd(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 3 {
        return Err(unsupported("timestampadd requires 3 arguments"));
    }
    let interval = format!(
        "{} {}",
        args[1].to_text().unwrap_or_default(),
        args[0].to_text().unwrap_or_default()
    );
    add_interval_to_date(&args[2].to_text().unwrap_or_default(), &interval, false)
        .map(ColumnValue::Text)
}

pub(super) fn eval_timestampdiff(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 3 {
        return Err(unsupported("timestampdiff requires 3 arguments"));
    }
    let unit = args[0].to_text().unwrap_or_default().to_ascii_lowercase();
    let start = parse_ymd(&args[1].to_text().unwrap_or_default())?;
    let end = parse_ymd(&args[2].to_text().unwrap_or_default())?;
    let days = days_from_civil(end.0, end.1, end.2) - days_from_civil(start.0, start.1, start.2);
    let value = match unit.as_str() {
        "day" | "days" => days,
        "week" | "weeks" => days / 7,
        "month" | "months" => (end.0 - start.0) * 12 + end.1 as i64 - start.1 as i64,
        "year" | "years" => end.0 - start.0,
        _ => days,
    };
    Ok(ColumnValue::Int64(value))
}

pub(super) fn eval_date_format(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 2 {
        return Err(unsupported("date_format requires 2 arguments"));
    }
    let text = args[0].to_text().unwrap_or_default();
    let (y, m, d) = parse_ymd(&text)?;
    let hms = text.get(11..19).unwrap_or("00:00:00");
    let fmt = args[1].to_text().unwrap_or_default();
    let out = fmt
        .replace("%Y", &format!("{y:04}"))
        .replace("%m", &format!("{m:02}"))
        .replace("%d", &format!("{d:02}"))
        .replace("%H", hms.get(0..2).unwrap_or("00"))
        .replace("%i", hms.get(3..5).unwrap_or("00"))
        .replace("%s", hms.get(6..8).unwrap_or("00"));
    Ok(ColumnValue::Text(out))
}

pub(super) fn eval_str_to_date(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() != 2 {
        return Err(unsupported("str_to_date requires 2 arguments"));
    }
    Ok(ColumnValue::Text(args[0].to_text().unwrap_or_default()))
}

pub(super) fn eval_unix_timestamp(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    let text = if args.is_empty() {
        current_timestamp_text()
    } else {
        args[0].to_text().unwrap_or_default()
    };
    let (y, m, d) = parse_ymd(&text)?;
    Ok(ColumnValue::Int64(days_from_civil(y, m, d) * 86_400))
}

pub(super) fn eval_from_unixtime(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.is_empty() {
        return Err(unsupported("from_unixtime requires an argument"));
    }
    let seconds = value_to_i64(&args[0])?.max(0);
    let days = seconds / 86_400;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    Ok(ColumnValue::Text(format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        seconds_of_day / 3600,
        (seconds_of_day % 3600) / 60,
        seconds_of_day % 60
    )))
}

pub(super) fn eval_last_day(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    let (y, m, _) = parse_ymd(&exactly_one(args, "last_day")?.to_text().unwrap_or_default())?;
    Ok(ColumnValue::Date(format!(
        "{y:04}-{m:02}-{:02}",
        days_in_month(y, m)
    )))
}

pub(super) fn eval_week(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    let (y, m, d) = parse_ymd(&exactly_one(args, "week")?.to_text().unwrap_or_default())?;
    let day_of_year = days_from_civil(y, m, d) - days_from_civil(y, 1, 1);
    Ok(ColumnValue::Int32((day_of_year / 7) as i32))
}

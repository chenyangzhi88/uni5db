use pgwire::error::PgWireResult;
use serde_json::Value as JsonValue;
use sqlparser::ast::{FunctionArg, FunctionArguments, Value as SqlValue};

use super::functions::exactly_one;
use super::operators::mysql_value_to_number;
use super::values::civil_from_days;
use crate::error::{unsupported, user_error};
use crate::types::ColumnValue;

pub(super) fn json_text(value: ColumnValue) -> PgWireResult<String> {
    match value {
        ColumnValue::Json(raw) | ColumnValue::Jsonb(raw) | ColumnValue::Text(raw) => Ok(raw),
        other => other
            .to_text()
            .ok_or_else(|| unsupported("JSON operator received NULL")),
    }
}

pub(super) fn json_key(value: ColumnValue) -> PgWireResult<String> {
    match value {
        ColumnValue::Text(raw)
        | ColumnValue::Json(raw)
        | ColumnValue::Jsonb(raw)
        | ColumnValue::Numeric(raw)
        | ColumnValue::Uuid(raw) => Ok(raw),
        ColumnValue::Int16(v) => Ok(v.to_string()),
        ColumnValue::Int32(v) => Ok(v.to_string()),
        ColumnValue::Int64(v) => Ok(v.to_string()),
        other => other
            .to_text()
            .ok_or_else(|| unsupported("JSON key operator received NULL")),
    }
}

pub(super) fn json_extract_value(
    source: ColumnValue,
    key: ColumnValue,
    as_text: bool,
) -> PgWireResult<ColumnValue> {
    let raw = json_text(source)?;
    let parsed: JsonValue = serde_json::from_str(&raw)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    let key = json_key(key)?;
    let selected = match parsed {
        JsonValue::Array(values) => key
            .parse::<usize>()
            .ok()
            .and_then(|idx| values.get(idx).cloned()),
        JsonValue::Object(map) => map.get(&key).cloned(),
        _ => None,
    };
    let Some(selected) = selected else {
        return Ok(ColumnValue::Null);
    };
    if as_text {
        Ok(ColumnValue::Text(match selected {
            JsonValue::String(s) => s,
            JsonValue::Null => return Ok(ColumnValue::Null),
            other => other.to_string(),
        }))
    } else {
        Ok(ColumnValue::Jsonb(selected.to_string()))
    }
}

pub(super) fn json_contains_value(
    source: ColumnValue,
    contained: ColumnValue,
) -> PgWireResult<bool> {
    let left: JsonValue = serde_json::from_str(&json_text(source)?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    let right: JsonValue = serde_json::from_str(&json_text(contained)?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    Ok(json_contains(&left, &right))
}

pub(super) fn json_contains(left: &JsonValue, right: &JsonValue) -> bool {
    match (left, right) {
        (_, JsonValue::Null) => left.is_null(),
        (JsonValue::Object(left), JsonValue::Object(right)) => right.iter().all(|(key, value)| {
            left.get(key)
                .is_some_and(|candidate| json_contains(candidate, value))
        }),
        (JsonValue::Array(left), JsonValue::Array(right)) => right.iter().all(|wanted| {
            left.iter()
                .any(|candidate| json_contains(candidate, wanted))
        }),
        (JsonValue::Array(left), right) => {
            left.iter().any(|candidate| json_contains(candidate, right))
        }
        _ => left == right,
    }
}

pub(super) fn json_has_key_value(source: ColumnValue, key: ColumnValue) -> PgWireResult<bool> {
    let parsed: JsonValue = serde_json::from_str(&json_text(source)?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    let key = json_key(key)?;
    Ok(match parsed {
        JsonValue::Object(map) => map.contains_key(&key),
        JsonValue::Array(values) => values.iter().any(|value| value.as_str() == Some(&key)),
        _ => false,
    })
}

pub(super) fn eval_json_extract(
    args: Vec<ColumnValue>,
    unquote: bool,
) -> PgWireResult<ColumnValue> {
    if args.len() < 2 {
        return Err(unsupported("json_extract requires at least 2 arguments"));
    }
    let parsed: JsonValue = serde_json::from_str(&json_text(args[0].clone())?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    let mut selected = Vec::new();
    for path in args.iter().skip(1) {
        if let Some(value) = json_path_get(&parsed, &path.to_text().unwrap_or_default()) {
            selected.push(value.clone());
        }
    }
    if selected.is_empty() {
        return Ok(ColumnValue::Null);
    }
    let value = if selected.len() == 1 {
        selected.remove(0)
    } else {
        JsonValue::Array(selected)
    };
    if unquote {
        Ok(ColumnValue::Text(json_unquote_value(value)))
    } else {
        Ok(ColumnValue::Json(value.to_string()))
    }
}

pub(super) fn eval_json_unquote(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    let value = exactly_one(args, "json_unquote")?;
    let parsed: JsonValue = serde_json::from_str(&json_text(value)?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    Ok(ColumnValue::Text(json_unquote_value(parsed)))
}

pub(super) fn eval_json_contains(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() < 2 {
        return Err(unsupported("json_contains requires at least 2 arguments"));
    }
    let source = if args.len() >= 3 {
        eval_json_extract(vec![args[0].clone(), args[2].clone()], false)?
    } else {
        args[0].clone()
    };
    Ok(ColumnValue::Boolean(json_contains_value(
        source,
        args[1].clone(),
    )?))
}

pub(super) enum JsonMutation {
    Set,
    Replace,
    Remove,
}

pub(super) fn eval_json_mutate(
    args: Vec<ColumnValue>,
    mode: JsonMutation,
) -> PgWireResult<ColumnValue> {
    if args.len() < 2 {
        return Err(unsupported("json mutation requires arguments"));
    }
    let mut root: JsonValue = serde_json::from_str(&json_text(args[0].clone())?)
        .map_err(|e| user_error("22P02", format!("invalid JSON: {e}")))?;
    match mode {
        JsonMutation::Remove => {
            for path in args.iter().skip(1) {
                json_path_remove(&mut root, &path.to_text().unwrap_or_default());
            }
        }
        JsonMutation::Set | JsonMutation::Replace => {
            for pair in args[1..].chunks(2) {
                if pair.len() != 2 {
                    return Err(unsupported(
                        "json_set/json_replace require path/value pairs",
                    ));
                }
                let value = column_value_to_json(&pair[1]);
                let replace_only = matches!(mode, JsonMutation::Replace);
                json_path_set(
                    &mut root,
                    &pair[0].to_text().unwrap_or_default(),
                    value,
                    replace_only,
                );
            }
        }
    }
    Ok(ColumnValue::Json(root.to_string()))
}

pub(super) fn eval_json_object(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    if args.len() % 2 != 0 {
        return Err(unsupported("json_object requires key/value pairs"));
    }
    let mut map = serde_json::Map::new();
    for pair in args.chunks(2) {
        map.insert(
            pair[0].to_text().unwrap_or_default(),
            column_value_to_json(&pair[1]),
        );
    }
    Ok(ColumnValue::Json(JsonValue::Object(map).to_string()))
}

pub(super) fn eval_json_array(args: Vec<ColumnValue>) -> PgWireResult<ColumnValue> {
    Ok(ColumnValue::Json(
        JsonValue::Array(args.iter().map(column_value_to_json).collect()).to_string(),
    ))
}

pub(super) fn matches_like(
    value: &Option<String>,
    pattern: &Option<String>,
    escape_char: Option<char>,
    case_insensitive: bool,
) -> bool {
    let (Some(value), Some(pattern)) = (value, pattern) else {
        return false;
    };
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
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    like_match_chars(&value, &pattern, escape_char, 0, 0)
}

pub(super) fn like_match_chars(
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

pub(super) fn simple_regexp_match(value: &str, pattern: &str) -> bool {
    if let Some(rest) = pattern.strip_prefix('^') {
        let rest = rest.strip_suffix('$').unwrap_or(rest);
        return simple_regexp_match_here(value, rest);
    }
    if let Some(inner) = pattern.strip_suffix('$') {
        return (0..=value.len()).any(|idx| {
            value.is_char_boundary(idx)
                && simple_regexp_match_here(&value[idx..], inner)
                && value[idx..].chars().count() == regexp_min_len(inner)
        });
    }
    value
        .char_indices()
        .any(|(idx, _)| simple_regexp_match_here(&value[idx..], pattern))
        || pattern.is_empty()
}

pub(super) fn simple_regexp_match_here(value: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    if let Some(rest) = pattern.strip_prefix(".*") {
        return (0..=value.len()).any(|idx| {
            value.is_char_boundary(idx) && simple_regexp_match_here(&value[idx..], rest)
        });
    }
    let mut pchars = pattern.chars();
    let Some(pch) = pchars.next() else {
        return true;
    };
    let prest = &pattern[pch.len_utf8()..];
    let mut vchars = value.chars();
    let Some(vch) = vchars.next() else {
        return false;
    };
    (pch == '.' || pch == vch) && simple_regexp_match_here(&value[vch.len_utf8()..], prest)
}

pub(super) fn regexp_min_len(pattern: &str) -> usize {
    pattern.replace(".*", "").chars().count()
}

pub(super) fn value_to_i64(value: &ColumnValue) -> PgWireResult<i64> {
    mysql_value_to_number(value)
        .map(|value| value as i64)
        .ok_or_else(|| unsupported("value is not numeric"))
}

pub(super) fn parse_ymd(text: &str) -> PgWireResult<(i64, u32, u32)> {
    let date = text.get(0..10).unwrap_or(text);
    let parts = date.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(user_error("22007", format!("invalid date '{text}'")));
    }
    let y = parts[0]
        .parse::<i64>()
        .map_err(|_| user_error("22007", format!("invalid date '{text}'")))?;
    let m = parts[1]
        .parse::<u32>()
        .map_err(|_| user_error("22007", format!("invalid date '{text}'")))?;
    let d = parts[2]
        .parse::<u32>()
        .map_err(|_| user_error("22007", format!("invalid date '{text}'")))?;
    Ok((y, m, d))
}

pub(super) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i64;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(super) fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 30,
    }
}

pub(super) fn add_interval_to_date(
    text: &str,
    interval: &str,
    subtract: bool,
) -> PgWireResult<String> {
    let (mut y, mut m, mut d) = parse_ymd(text)?;
    let mut parts = interval.split_whitespace();
    let amount = parts
        .next()
        .unwrap_or("0")
        .parse::<i64>()
        .map_err(|_| user_error("22007", format!("invalid interval '{interval}'")))?;
    let amount = if subtract { -amount } else { amount };
    let unit = parts.next().unwrap_or("DAY").to_ascii_lowercase();
    match unit.as_str() {
        "year" | "years" => y += amount,
        "month" | "months" => {
            let total = y * 12 + m as i64 - 1 + amount;
            y = total.div_euclid(12);
            m = total.rem_euclid(12) as u32 + 1;
            d = d.min(days_in_month(y, m));
        }
        "week" | "weeks" => {
            let (ny, nm, nd) = civil_from_days(days_from_civil(y, m, d) + amount * 7);
            y = ny;
            m = nm;
            d = nd;
        }
        _ => {
            let (ny, nm, nd) = civil_from_days(days_from_civil(y, m, d) + amount);
            y = ny;
            m = nm;
            d = nd;
        }
    }
    let suffix = text.get(10..).unwrap_or("");
    Ok(format!("{y:04}-{m:02}-{d:02}{suffix}"))
}

pub(super) fn json_path_get<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let mut current = value;
    for token in json_path_tokens(path) {
        match token {
            JsonPathToken::Key(key) => current = current.as_object()?.get(&key)?,
            JsonPathToken::Index(idx) => current = current.as_array()?.get(idx)?,
        }
    }
    Some(current)
}

pub(super) fn json_path_set(
    root: &mut JsonValue,
    path: &str,
    value: JsonValue,
    replace_only: bool,
) {
    let tokens = json_path_tokens(path);
    if tokens.len() != 1 {
        return;
    }
    match &tokens[0] {
        JsonPathToken::Key(key) => {
            if let Some(map) = root.as_object_mut()
                && (!replace_only || map.contains_key(key))
            {
                map.insert(key.clone(), value);
            }
        }
        JsonPathToken::Index(idx) => {
            if let Some(array) = root.as_array_mut()
                && *idx < array.len()
            {
                array[*idx] = value;
            }
        }
    }
}

pub(super) fn json_path_remove(root: &mut JsonValue, path: &str) {
    for token in json_path_tokens(path) {
        match token {
            JsonPathToken::Key(key) => {
                if let Some(map) = root.as_object_mut() {
                    map.remove(&key);
                }
            }
            JsonPathToken::Index(idx) => {
                if let Some(array) = root.as_array_mut()
                    && idx < array.len()
                {
                    array.remove(idx);
                }
            }
        }
    }
}

pub(super) enum JsonPathToken {
    Key(String),
    Index(usize),
}

pub(super) fn json_path_tokens(path: &str) -> Vec<JsonPathToken> {
    let mut rest = path.trim().strip_prefix('$').unwrap_or(path.trim());
    let mut tokens = Vec::new();
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
            tokens.push(JsonPathToken::Key(after_dot[..end].to_string()));
            rest = &after_dot[end..];
        } else if let Some(after_bracket) = rest.strip_prefix('[') {
            if let Some(end) = after_bracket.find(']') {
                if let Ok(idx) = after_bracket[..end].parse::<usize>() {
                    tokens.push(JsonPathToken::Index(idx));
                }
                rest = &after_bracket[end + 1..];
            } else {
                break;
            }
        } else {
            break;
        }
    }
    tokens
}

pub(super) fn json_unquote_value(value: JsonValue) -> String {
    match value {
        JsonValue::String(value) => value,
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

pub(super) fn column_value_to_json(value: &ColumnValue) -> JsonValue {
    match value {
        ColumnValue::Null => JsonValue::Null,
        ColumnValue::Boolean(v) => JsonValue::Bool(*v),
        ColumnValue::Int16(v) => JsonValue::from(*v),
        ColumnValue::Int32(v) => JsonValue::from(*v),
        ColumnValue::Int64(v) => JsonValue::from(*v),
        ColumnValue::Float32(v) => JsonValue::from(*v as f64),
        ColumnValue::Float64(v) => JsonValue::from(*v),
        ColumnValue::Json(v) | ColumnValue::Jsonb(v) => {
            serde_json::from_str(v).unwrap_or_else(|_| JsonValue::String(v.clone()))
        }
        other => JsonValue::String(other.to_text().unwrap_or_default()),
    }
}

pub(super) fn escape_char_value(
    value: &Option<sqlparser::ast::ValueWithSpan>,
) -> PgWireResult<Option<char>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match &value.value {
        SqlValue::SingleQuotedString(raw) | SqlValue::DoubleQuotedString(raw) => {
            let mut chars = raw.chars();
            let ch = chars
                .next()
                .ok_or_else(|| user_error("22025", "LIKE ESCAPE must not be empty"))?;
            if chars.next().is_some() {
                return Err(user_error(
                    "22025",
                    "LIKE ESCAPE must be a single character",
                ));
            }
            Ok(Some(ch))
        }
        _ => Err(user_error("22025", "LIKE ESCAPE must be a string literal")),
    }
}

pub(super) fn function_args(args: FunctionArguments) -> Vec<FunctionArg> {
    match args {
        FunctionArguments::List(list) => list.args,
        FunctionArguments::None | FunctionArguments::Subquery(_) => Vec::new(),
    }
}

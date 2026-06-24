use opensrv_mysql::{ColumnType, Value, ValueInner};

use super::{
    MySqlBackend, escape_sql_string, render_mysql_date, render_mysql_datetime, render_mysql_time,
};

impl MySqlBackend {
    pub(super) fn count_placeholders(sql: &str) -> usize {
        let mut count = 0usize;
        let mut chars = sql.chars().peekable();
        let mut quote = None;
        while let Some(ch) = chars.next() {
            if let Some(active) = quote {
                if ch == '\\' {
                    let _ = chars.next();
                } else if ch == active {
                    quote = None;
                }
                continue;
            }
            match ch {
                '\'' | '"' | '`' => quote = Some(ch),
                '?' => count += 1,
                '-' if chars.peek() == Some(&'-') => {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            break;
                        }
                    }
                }
                '#' => {
                    for next in chars.by_ref() {
                        if next == '\n' {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
        count
    }

    pub(super) fn bind_prepared_sql(sql: &str, params: &[String]) -> Result<String, String> {
        let mut output = String::with_capacity(sql.len() + params.len() * 8);
        let mut param_idx = 0usize;
        let mut chars = sql.chars().peekable();
        let mut quote = None;
        while let Some(ch) = chars.next() {
            if let Some(active) = quote {
                output.push(ch);
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        output.push(next);
                    }
                } else if ch == active {
                    quote = None;
                }
                continue;
            }
            match ch {
                '\'' | '"' | '`' => {
                    quote = Some(ch);
                    output.push(ch);
                }
                '?' => {
                    let Some(value) = params.get(param_idx) else {
                        return Err("not enough prepared statement parameters".to_string());
                    };
                    output.push_str(value);
                    param_idx += 1;
                }
                '-' if chars.peek() == Some(&'-') => {
                    output.push(ch);
                    if let Some(next) = chars.next() {
                        output.push(next);
                    }
                    for next in chars.by_ref() {
                        output.push(next);
                        if next == '\n' {
                            break;
                        }
                    }
                }
                '#' => {
                    output.push(ch);
                    for next in chars.by_ref() {
                        output.push(next);
                        if next == '\n' {
                            break;
                        }
                    }
                }
                _ => output.push(ch),
            }
        }
        if param_idx != params.len() {
            return Err("too many prepared statement parameters".to_string());
        }
        Ok(output)
    }

    pub(super) fn render_mysql_param_value(value: Value<'_>, coltype: ColumnType) -> String {
        match value.into_inner() {
            ValueInner::NULL => "NULL".to_string(),
            ValueInner::Bytes(bytes) if Self::mysql_binary_param_type(coltype) => {
                Self::render_mysql_hex_param(bytes)
            }
            ValueInner::Bytes(bytes) => Self::render_mysql_bytes_param(bytes),
            ValueInner::Int(value) => value.to_string(),
            ValueInner::UInt(value) => value.to_string(),
            ValueInner::Double(value) => {
                if value.is_finite() {
                    value.to_string()
                } else {
                    "NULL".to_string()
                }
            }
            ValueInner::Date(bytes) => render_mysql_date(bytes),
            ValueInner::Datetime(bytes) => render_mysql_datetime(bytes),
            ValueInner::Time(bytes) => render_mysql_time(bytes),
        }
    }

    pub(super) fn mysql_binary_param_type(coltype: ColumnType) -> bool {
        matches!(
            coltype,
            ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BIT
                | ColumnType::MYSQL_TYPE_GEOMETRY
        )
    }

    pub(super) fn render_mysql_bytes_param(bytes: &[u8]) -> String {
        if let Ok(value) = std::str::from_utf8(bytes)
            && !value.contains('\0')
        {
            return format!("'{}'", escape_sql_string(value));
        }
        Self::render_mysql_hex_param(bytes)
    }

    pub(super) fn render_mysql_hex_param(bytes: &[u8]) -> String {
        let mut hex = String::with_capacity(bytes.len() * 2 + 3);
        hex.push_str("X'");
        for byte in bytes {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        hex.push('\'');
        hex
    }
}

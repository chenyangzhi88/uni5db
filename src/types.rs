use std::cmp::Ordering;
use std::collections::HashMap;

use pgwire::api::Type;
use pgwire::error::PgWireResult;
use serde_json::Value;
use sqlparser::ast::Expr;

use crate::error::user_error;

pub const INTERNAL_ROWID_COLUMN: &str = "__pg_rowid";
const INTERNAL_ROWID_TYPE: DataType = DataType::Int64;

// ── DataType ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Int16,
    Int32,
    Int64,
    MySqlInt {
        kind: MySqlIntKind,
        unsigned: bool,
    },
    Float32,
    Float64,
    MySqlFloat {
        precision: Option<u32>,
    },
    MySqlDouble {
        precision: Option<u32>,
    },
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Text,
    MySqlText {
        max_len: Option<u64>,
        type_name: &'static str,
    },
    VarChar(Option<u32>),
    Char(Option<u32>),
    Binary(Option<u32>),
    VarBinary(Option<u32>),
    Blob {
        max_len: Option<u64>,
        type_name: &'static str,
    },
    Boolean,
    Bit(Option<u32>),
    Year,
    Date,
    Time,
    MySqlTime {
        fsp: Option<u32>,
    },
    TimeTz,
    Interval,
    Timestamp,
    MySqlDateTime {
        fsp: Option<u32>,
    },
    MySqlTimestamp {
        fsp: Option<u32>,
    },
    TimestampTz,
    Uuid,
    Bytea,
    Json,
    Jsonb,
    Array(Box<DataType>),
    Domain(String),
    Enum(Vec<String>),
    Set(Vec<String>),
    Geometry(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MySqlIntKind {
    Tiny,
    Small,
    Medium,
    Int,
    Big,
}

impl DataType {
    pub fn from_sql(sql_type: &str) -> Self {
        Self::from_sql_with_mode(sql_type, false)
    }

    pub fn from_mysql_sql(sql_type: &str) -> Self {
        Self::from_sql_with_mode(sql_type, true)
    }

    fn from_sql_with_mode(sql_type: &str, mysql_mode: bool) -> Self {
        let raw = sql_type.trim();
        if let Some(inner) = raw
            .strip_prefix("DOMAIN(")
            .and_then(|s| s.strip_suffix(')'))
        {
            return DataType::Domain(inner.to_string());
        }
        if let Some(inner) = raw.strip_suffix("[]") {
            return DataType::Array(Box::new(DataType::from_sql_with_mode(inner, mysql_mode)));
        }
        let upper = raw.to_ascii_uppercase();
        if upper.starts_with("ENUM(") {
            return DataType::Enum(parse_mysql_string_list(raw));
        }
        if upper.starts_with("SET(") {
            return DataType::Set(parse_mysql_string_list(raw));
        }
        if upper.starts_with("ARRAY<") && upper.ends_with('>') {
            return DataType::Array(Box::new(DataType::from_sql_with_mode(
                &raw[6..raw.len() - 1],
                mysql_mode,
            )));
        }
        if upper.starts_with('_') {
            return DataType::Array(Box::new(DataType::from_sql_with_mode(
                &raw[1..],
                mysql_mode,
            )));
        }
        let varchar_len =
            if upper.starts_with("VARCHAR(") || upper.starts_with("CHARACTER VARYING(") {
                mysql_type_numbers(raw).first().copied()
            } else {
                None
            };
        let char_len = if upper.starts_with("CHAR(")
            || upper.starts_with("CHARACTER(")
            || upper.starts_with("BPCHAR(")
        {
            mysql_type_numbers(raw).first().copied()
        } else {
            None
        };
        let binary_len = if upper.starts_with("BINARY(") {
            mysql_type_numbers(raw).first().copied()
        } else {
            None
        };
        let varbinary_len = if upper.starts_with("VARBINARY(") {
            mysql_type_numbers(raw).first().copied()
        } else {
            None
        };
        let numbers = mysql_type_numbers(raw);
        let unsigned = upper.split_whitespace().any(|part| part == "UNSIGNED");
        let normalized = upper
            .split_whitespace()
            .next()
            .unwrap_or("")
            .split('(')
            .next()
            .unwrap_or("");
        match normalized {
            "TINYINT" | "UTINYINT" => DataType::MySqlInt {
                kind: MySqlIntKind::Tiny,
                unsigned: unsigned || normalized == "UTINYINT",
            },
            "SMALLINT" if mysql_mode || unsigned => DataType::MySqlInt {
                kind: MySqlIntKind::Small,
                unsigned,
            },
            "MEDIUMINT" => DataType::MySqlInt {
                kind: MySqlIntKind::Medium,
                unsigned,
            },
            "INT" | "INTEGER" if mysql_mode || unsigned => DataType::MySqlInt {
                kind: MySqlIntKind::Int,
                unsigned,
            },
            "BIGINT" if mysql_mode || unsigned => DataType::MySqlInt {
                kind: MySqlIntKind::Big,
                unsigned,
            },
            "SMALLINT" | "INT2" | "SMALLSERIAL" | "SERIAL2" => DataType::Int16,
            "INT" | "INTEGER" | "INT4" | "SERIAL" => DataType::Int32,
            "BIGINT" | "INT8" | "BIGSERIAL" => DataType::Int64,
            "FLOAT" if !numbers.is_empty() => DataType::MySqlFloat {
                precision: numbers.first().copied(),
            },
            "REAL" | "FLOAT" | "FLOAT4" => DataType::Float32,
            "DOUBLE" if !numbers.is_empty() => DataType::MySqlDouble {
                precision: numbers.first().copied(),
            },
            "DOUBLE" | "DOUBLEPRECISION" | "FLOAT8" => DataType::Float64,
            "DECIMAL" | "NUMERIC" => DataType::Numeric {
                precision: numbers.first().copied(),
                scale: numbers.get(1).copied(),
            },
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            "TEXT" | "STRING" => DataType::Text,
            "MYSQL_TEXT" => DataType::MySqlText {
                max_len: Some(65_535),
                type_name: "text",
            },
            "TINYTEXT" => DataType::MySqlText {
                max_len: Some(255),
                type_name: "tinytext",
            },
            "MEDIUMTEXT" => DataType::MySqlText {
                max_len: Some(16_777_215),
                type_name: "mediumtext",
            },
            "LONGTEXT" => DataType::MySqlText {
                max_len: Some(4_294_967_295),
                type_name: "longtext",
            },
            "VARCHAR" => DataType::VarChar(varchar_len),
            "CHAR" | "CHARACTER" | "BPCHAR" => DataType::Char(char_len.or(Some(1))),
            "BINARY" => DataType::Binary(binary_len.or(Some(1))),
            "VARBINARY" => DataType::VarBinary(varbinary_len),
            "BIT" => DataType::Bit(numbers.first().copied().or(Some(1))),
            "YEAR" => DataType::Year,
            "TIME" => {
                if upper.contains("WITH TIME ZONE") {
                    DataType::TimeTz
                } else if !numbers.is_empty() {
                    DataType::MySqlTime {
                        fsp: numbers.first().copied(),
                    }
                } else {
                    DataType::Time
                }
            }
            "TIMETZ" => DataType::TimeTz,
            "INTERVAL" => DataType::Interval,
            "DATE" => DataType::Date,
            "DATETIME" => DataType::MySqlDateTime {
                fsp: numbers.first().copied(),
            },
            "TIMESTAMP" => {
                if upper.contains("WITH TIME ZONE") {
                    DataType::TimestampTz
                } else if !numbers.is_empty() {
                    DataType::MySqlTimestamp {
                        fsp: numbers.first().copied(),
                    }
                } else {
                    DataType::Timestamp
                }
            }
            "TIMESTAMPTZ" => DataType::TimestampTz,
            "UUID" => DataType::Uuid,
            "BYTEA" => DataType::Bytea,
            "TINYBLOB" => DataType::Blob {
                max_len: Some(255),
                type_name: "tinyblob",
            },
            "BLOB" => DataType::Blob {
                max_len: Some(65_535),
                type_name: "blob",
            },
            "MEDIUMBLOB" => DataType::Blob {
                max_len: Some(16_777_215),
                type_name: "mediumblob",
            },
            "LONGBLOB" => DataType::Blob {
                max_len: Some(4_294_967_295),
                type_name: "longblob",
            },
            "JSON" => DataType::Json,
            "JSONB" => DataType::Jsonb,
            "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
            | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => {
                DataType::Geometry(normalized.to_ascii_lowercase())
            }
            _ => DataType::Domain(raw.to_string()),
        }
    }

    pub fn to_pg_type(&self) -> Type {
        match self {
            DataType::Int16 => Type::INT2,
            DataType::Int32 => Type::INT4,
            DataType::Int64 => Type::INT8,
            DataType::MySqlInt {
                kind: MySqlIntKind::Tiny | MySqlIntKind::Small,
                unsigned: false,
            } => Type::INT2,
            DataType::MySqlInt {
                kind: MySqlIntKind::Big,
                ..
            } => Type::INT8,
            DataType::MySqlInt { .. } => Type::INT4,
            DataType::Float32 => Type::FLOAT4,
            DataType::Float64 => Type::FLOAT8,
            DataType::MySqlFloat { .. } => Type::FLOAT4,
            DataType::MySqlDouble { .. } => Type::FLOAT8,
            DataType::Numeric { .. } => Type::NUMERIC,
            DataType::Text | DataType::MySqlText { .. } => Type::TEXT,
            DataType::VarChar(_) => Type::VARCHAR,
            DataType::Char(_) => Type::BPCHAR,
            DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob { .. } => Type::BYTEA,
            DataType::Boolean => Type::BOOL,
            DataType::Bit(_) => Type::BIT,
            DataType::Year => Type::INT4,
            DataType::Date => Type::DATE,
            DataType::Time => Type::TIME,
            DataType::MySqlTime { .. } => Type::TIME,
            DataType::TimeTz => Type::TIMETZ,
            DataType::Interval => Type::INTERVAL,
            DataType::Timestamp => Type::TIMESTAMP,
            DataType::MySqlDateTime { .. } => Type::TIMESTAMP,
            DataType::MySqlTimestamp { .. } => Type::TIMESTAMP,
            DataType::TimestampTz => Type::TIMESTAMPTZ,
            DataType::Uuid => Type::UUID,
            DataType::Bytea => Type::BYTEA,
            DataType::Json => Type::JSON,
            DataType::Jsonb => Type::JSONB,
            DataType::Array(inner) => inner.to_pg_array_type(),
            DataType::Domain(_) | DataType::Enum(_) | DataType::Set(_) | DataType::Geometry(_) => {
                Type::TEXT
            }
        }
    }

    fn to_pg_array_type(&self) -> Type {
        match self {
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Bytea => Type::BYTEA_ARRAY,
            DataType::Int16 => Type::INT2_ARRAY,
            DataType::MySqlInt {
                kind: MySqlIntKind::Tiny | MySqlIntKind::Small,
                unsigned: false,
            } => Type::INT2_ARRAY,
            DataType::Int32 => Type::INT4_ARRAY,
            DataType::Text
            | DataType::MySqlText { .. }
            | DataType::VarChar(_)
            | DataType::Char(_)
            | DataType::Binary(_)
            | DataType::VarBinary(_)
            | DataType::Blob { .. }
            | DataType::Domain(_)
            | DataType::Enum(_)
            | DataType::Set(_)
            | DataType::Geometry(_) => Type::TEXT_ARRAY,
            DataType::Int64 => Type::INT8_ARRAY,
            DataType::MySqlInt {
                kind: MySqlIntKind::Big,
                ..
            } => Type::INT8_ARRAY,
            DataType::MySqlInt { .. } | DataType::Year | DataType::Bit(_) => Type::INT4_ARRAY,
            DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::MySqlFloat { .. } => Type::FLOAT4_ARRAY,
            DataType::MySqlDouble { .. } => Type::FLOAT8_ARRAY,
            DataType::Time => Type::TIME_ARRAY,
            DataType::MySqlTime { .. } => Type::TIME_ARRAY,
            DataType::TimeTz => Type::TIMETZ_ARRAY,
            DataType::Date => Type::DATE_ARRAY,
            DataType::Timestamp => Type::TIMESTAMP_ARRAY,
            DataType::MySqlDateTime { .. } | DataType::MySqlTimestamp { .. } => {
                Type::TIMESTAMP_ARRAY
            }
            DataType::TimestampTz => Type::TIMESTAMPTZ_ARRAY,
            DataType::Interval => Type::INTERVAL_ARRAY,
            DataType::Numeric { .. } => Type::NUMERIC_ARRAY,
            DataType::Uuid => Type::UUID_ARRAY,
            DataType::Json => Type::JSON_ARRAY,
            DataType::Jsonb => Type::JSONB_ARRAY,
            DataType::Array(_) => Type::TEXT_ARRAY,
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            DataType::Int16 => "INT2".to_string(),
            DataType::Int32 => "INT4".to_string(),
            DataType::Int64 => "INT8".to_string(),
            DataType::MySqlInt { kind, unsigned } => {
                format!(
                    "{}{}",
                    kind.as_mysql_name(),
                    if *unsigned { " UNSIGNED" } else { "" }
                )
            }
            DataType::Float32 => "FLOAT4".to_string(),
            DataType::Float64 => "FLOAT8".to_string(),
            DataType::MySqlFloat { precision } => match precision {
                Some(precision) => format!("FLOAT({precision})"),
                None => "FLOAT".to_string(),
            },
            DataType::MySqlDouble { precision } => match precision {
                Some(precision) => format!("DOUBLE({precision})"),
                None => "DOUBLE".to_string(),
            },
            DataType::Numeric { precision, scale } => match (precision, scale) {
                (Some(precision), Some(scale)) => format!("NUMERIC({precision},{scale})"),
                (Some(precision), None) => format!("NUMERIC({precision})"),
                _ => "NUMERIC".to_string(),
            },
            DataType::Text => "TEXT".to_string(),
            DataType::MySqlText { type_name, .. } if *type_name == "text" => {
                "MYSQL_TEXT".to_string()
            }
            DataType::MySqlText { type_name, .. } => type_name.to_ascii_uppercase(),
            DataType::VarChar(Some(len)) => format!("VARCHAR({len})"),
            DataType::VarChar(None) => "VARCHAR".to_string(),
            DataType::Char(Some(len)) => format!("CHAR({len})"),
            DataType::Char(None) => "CHAR".to_string(),
            DataType::Binary(Some(len)) => format!("BINARY({len})"),
            DataType::Binary(None) => "BINARY".to_string(),
            DataType::VarBinary(Some(len)) => format!("VARBINARY({len})"),
            DataType::VarBinary(None) => "VARBINARY".to_string(),
            DataType::Blob { type_name, .. } => type_name.to_ascii_uppercase(),
            DataType::Boolean => "BOOL".to_string(),
            DataType::Bit(Some(len)) => format!("BIT({len})"),
            DataType::Bit(None) => "BIT".to_string(),
            DataType::Year => "YEAR".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::MySqlTime { fsp } => match fsp {
                Some(fsp) => format!("TIME({fsp})"),
                None => "TIME".to_string(),
            },
            DataType::TimeTz => "TIMETZ".to_string(),
            DataType::Interval => "INTERVAL".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::MySqlDateTime { fsp } => match fsp {
                Some(fsp) => format!("DATETIME({fsp})"),
                None => "DATETIME".to_string(),
            },
            DataType::MySqlTimestamp { fsp } => match fsp {
                Some(fsp) => format!("TIMESTAMP({fsp})"),
                None => "TIMESTAMP".to_string(),
            },
            DataType::TimestampTz => "TIMESTAMPTZ".to_string(),
            DataType::Uuid => "UUID".to_string(),
            DataType::Bytea => "BYTEA".to_string(),
            DataType::Json => "JSON".to_string(),
            DataType::Jsonb => "JSONB".to_string(),
            DataType::Array(inner) => format!("{}[]", inner.to_str()),
            DataType::Domain(name) => format!("DOMAIN({name})"),
            DataType::Enum(values) => format!("ENUM({})", quote_mysql_string_list(values)),
            DataType::Set(values) => format!("SET({})", quote_mysql_string_list(values)),
            DataType::Geometry(name) => name.to_ascii_uppercase(),
        }
    }
}

impl MySqlIntKind {
    pub fn as_mysql_name(self) -> &'static str {
        match self {
            Self::Tiny => "TINYINT",
            Self::Small => "SMALLINT",
            Self::Medium => "MEDIUMINT",
            Self::Int => "INT",
            Self::Big => "BIGINT",
        }
    }
}

fn mysql_type_numbers(raw: &str) -> Vec<u32> {
    let Some((_, rest)) = raw.split_once('(') else {
        return Vec::new();
    };
    let Some((inner, _)) = rest.split_once(')') else {
        return Vec::new();
    };
    inner
        .split(',')
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .collect()
}

fn parse_mysql_string_list(raw: &str) -> Vec<String> {
    let Some((_, rest)) = raw.split_once('(') else {
        return Vec::new();
    };
    let Some((inner, _)) = rest.rsplit_once(')') else {
        return Vec::new();
    };
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '\'' {
            in_quote = !in_quote;
            continue;
        }
        if ch == ',' && !in_quote {
            values.push(current.trim().trim_matches('"').to_string());
            current.clear();
            continue;
        }
        current.push(ch);
    }
    if !current.is_empty() || inner.ends_with(',') {
        values.push(current.trim().trim_matches('"').to_string());
    }
    values
}

fn quote_mysql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(",")
}

// ── ColumnValue ───────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Numeric(String),
    Text(String),
    Boolean(bool),
    Date(String),
    Timestamp(String),
    TimestampTz(String),
    Uuid(String),
    Bytea(Vec<u8>),
    Json(String),
    Jsonb(String),
    Array(Vec<ColumnValue>),
}

impl ColumnValue {
    pub fn to_text(&self) -> Option<String> {
        match self {
            ColumnValue::Null => None,
            ColumnValue::Int16(v) => Some(v.to_string()),
            ColumnValue::Int32(v) => Some(v.to_string()),
            ColumnValue::Int64(v) => Some(v.to_string()),
            ColumnValue::Float32(v) => Some(v.to_string()),
            ColumnValue::Float64(v) => Some(v.to_string()),
            ColumnValue::Numeric(v) => Some(v.clone()),
            ColumnValue::Text(v) => Some(v.clone()),
            ColumnValue::Boolean(v) => Some(v.to_string()),
            ColumnValue::Date(v) => Some(v.clone()),
            ColumnValue::Timestamp(v) => Some(v.clone()),
            ColumnValue::TimestampTz(v) => Some(v.clone()),
            ColumnValue::Uuid(v) => Some(v.clone()),
            ColumnValue::Bytea(v) => Some(format!("\\x{}", hex_encode(v))),
            ColumnValue::Json(v) => Some(v.clone()),
            ColumnValue::Jsonb(v) => Some(v.clone()),
            ColumnValue::Array(values) => Some(format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|value| value
                        .to_text()
                        .map(escape_array_text)
                        .unwrap_or_else(|| "NULL".to_string()))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }
}

impl PartialOrd for ColumnValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ColumnValue::Int16(a), ColumnValue::Int16(b)) => a.partial_cmp(b),
            (ColumnValue::Int16(a), ColumnValue::Int32(b)) => (*a as i32).partial_cmp(b),
            (ColumnValue::Int32(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as i32)),
            (ColumnValue::Int16(a), ColumnValue::Int64(b)) => (*a as i64).partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as i64)),
            (ColumnValue::Int32(a), ColumnValue::Int32(b)) => a.partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int64(b)) => a.partial_cmp(b),
            (ColumnValue::Int32(a), ColumnValue::Int64(b)) => (*a as i64).partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int32(b)) => a.partial_cmp(&(*b as i64)),
            (ColumnValue::Float32(a), ColumnValue::Float32(b)) => a.partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Float64(b)) => a.partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Float32(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Int16(a), ColumnValue::Float32(b)) => (*a as f32).partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as f32)),
            (ColumnValue::Int16(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Int32(a), ColumnValue::Float32(b)) => (*a as f32).partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Int32(b)) => a.partial_cmp(&(*b as f32)),
            (ColumnValue::Int64(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Numeric(a), ColumnValue::Numeric(b)) => numeric_cmp(a, b),
            (ColumnValue::Text(a), ColumnValue::Text(b)) => a.partial_cmp(b),
            (ColumnValue::Boolean(a), ColumnValue::Boolean(b)) => a.partial_cmp(b),
            (ColumnValue::Date(a), ColumnValue::Date(b))
            | (ColumnValue::Timestamp(a), ColumnValue::Timestamp(b))
            | (ColumnValue::TimestampTz(a), ColumnValue::TimestampTz(b))
            | (ColumnValue::Uuid(a), ColumnValue::Uuid(b)) => a.partial_cmp(b),
            (ColumnValue::Bytea(a), ColumnValue::Bytea(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

fn numeric_cmp(a: &str, b: &str) -> Option<Ordering> {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(a), Ok(b)) => a.partial_cmp(&b),
        _ => a.partial_cmp(b),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn escape_array_text(value: String) -> String {
    if value.is_empty()
        || value.contains(',')
        || value.contains('{')
        || value.contains('}')
        || value.contains('"')
        || value.contains('\\')
        || value.chars().any(char::is_whitespace)
    {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value
    }
}

// ── Schema ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub column_id: u32,
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub nullable: bool,
    pub default: Option<String>,
    pub on_update: Option<String>,
    pub character_set: Option<String>,
    pub collation: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub table_name: String,
    pub table_id: u32,
    pub schema_version: u32,
    pub table_epoch: u64,
    pub primary_key: String,
    pub check_constraints: Vec<CheckConstraintSchema>,
    pub unique_constraints: Vec<UniqueConstraintSchema>,
    pub foreign_keys: Vec<ForeignKeyConstraintSchema>,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckConstraintSchema {
    pub name: String,
    pub expr: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraintSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub primary_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyConstraintSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
}

impl TableSchema {
    pub fn normalize_descriptor(&mut self) {
        if self.schema_version == 0 {
            self.schema_version = 1;
        }
        if self.table_epoch == 0 {
            self.table_epoch = 1;
        }
        for (idx, column) in self.columns.iter_mut().enumerate() {
            if column.column_id == 0 {
                column.column_id = idx as u32 + 1;
            }
        }
    }

    pub fn pk_data_type(&self) -> &DataType {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| &c.data_type)
            .unwrap_or(&INTERNAL_ROWID_TYPE)
    }

    pub fn find_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn find_column_by_id(&self, column_id: u32) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.column_id == column_id)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn has_user_primary_key(&self) -> bool {
        self.columns.iter().any(|c| c.primary_key)
    }
}

pub fn parse_column_schema(value: &Value) -> PgWireResult<ColumnSchema> {
    Ok(ColumnSchema {
        column_id: value.get("column_id").and_then(Value::as_u64).unwrap_or(0) as u32,
        name: value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| user_error("XX000", "schema column name is malformed"))?
            .to_string(),
        data_type: DataType::from_sql(
            value
                .get("data_type")
                .and_then(Value::as_str)
                .unwrap_or("TEXT"),
        ),
        primary_key: value
            .get("primary_key")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        nullable: value
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        default: value
            .get("default")
            .and_then(Value::as_str)
            .map(str::to_string),
        on_update: value
            .get("on_update")
            .and_then(Value::as_str)
            .map(str::to_string),
        character_set: value
            .get("character_set")
            .and_then(Value::as_str)
            .map(str::to_string),
        collation: value
            .get("collation")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

// ── Row helpers ───────────────────────────────────────────────────────

pub type RowMap = HashMap<String, ColumnValue>;

pub fn apply_assignments(row: &mut RowMap, assignments: &[(String, ColumnValue)]) {
    for (column, value) in assignments {
        row.insert(column.clone(), value.clone());
    }
}

#[derive(Clone, Debug)]
pub enum UpdateAssignment {
    Expr { column: String, expr: Expr },
}

#[derive(Clone, Debug)]
pub struct InsertConflictAssignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Clone, Debug)]
pub enum InsertConflictAction {
    DoNothing,
    DoNothingAnyUnique {
        target_column_sets: Vec<Vec<String>>,
    },
    ReplaceAnyUnique {
        target_column_sets: Vec<Vec<String>>,
    },
    DoUpdate {
        target_columns: Vec<String>,
        assignments: Vec<InsertConflictAssignment>,
        selection: Option<Expr>,
    },
    DoUpdateAnyUnique {
        target_column_sets: Vec<Vec<String>>,
        assignments: Vec<InsertConflictAssignment>,
    },
}

#[derive(Clone, Debug)]
pub enum ReturningProjection {
    Wildcard,
    Column(String),
    Expr { expr: Expr, output_name: String },
}

// ── Query plan ────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum QueryPlan {
    Noop {
        tag: String,
    },
    CreateDatabase {
        database_name: String,
        if_not_exists: bool,
    },
    CreateSchema {
        database_name: String,
        schema_name: String,
        if_not_exists: bool,
    },
    CreateTable {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        auto_increment_start: Option<i64>,
        indexes: Vec<(String, Vec<String>, bool)>,
    },
    CreateSequence {
        database_name: String,
        schema_name: String,
        sequence_name: String,
        if_not_exists: bool,
        start: i64,
        increment: i64,
    },
    CreateView {
        database_name: String,
        schema_name: String,
        view_name: String,
        definition: String,
        or_replace: bool,
        if_not_exists: bool,
    },
    CreateTableAs {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        rows: Vec<RowMap>,
    },
    AlterTableAddPrimaryKey {
        database_name: String,
        schema_name: String,
        table_name: String,
        column_name: String,
    },
    AlterTable {
        database_name: String,
        schema_name: String,
        table_name: String,
        operations: Vec<TableAlterOperation>,
    },
    CreateIndex {
        database_name: String,
        schema_name: String,
        table_name: String,
        index_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropTables {
        database_name: String,
        tables: Vec<(String, String)>,
        if_exists: bool,
    },
    DropIndexes {
        database_name: String,
        indexes: Vec<(String, String)>,
        if_exists: bool,
    },
    DropSequences {
        database_name: String,
        sequences: Vec<(String, String)>,
        if_exists: bool,
    },
    DropViews {
        database_name: String,
        views: Vec<(String, String)>,
        if_exists: bool,
    },
    DropSchemas {
        database_name: String,
        schemas: Vec<String>,
        if_exists: bool,
    },
    DropDatabases {
        databases: Vec<String>,
        if_exists: bool,
    },
    TruncateTables {
        database_name: String,
        tables: Vec<(String, TableSchema)>,
    },
    ExplainRows {
        rows: Vec<Vec<Option<String>>>,
    },
    PostgresExplainRows {
        rows: Vec<Vec<Option<String>>>,
    },
    AnalyzeTables {
        database_name: String,
        tables: Vec<(String, TableSchema)>,
    },
    TableMaintenanceRows {
        rows: Vec<Vec<Option<String>>>,
    },
    InsertRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        rows: Vec<RowMap>,
        on_conflict: Option<InsertConflictAction>,
        returning: Option<Vec<ReturningProjection>>,
    },
    SelectRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        projection: Vec<String>,
        access: ReadAccess,
        limit: Option<usize>,
        offset: usize,
    },
    UpdateRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        assignments: Vec<UpdateAssignment>,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
        returning: Option<Vec<ReturningProjection>>,
    },
    DeleteRows {
        database_name: String,
        schema_name: String,
        schema: TableSchema,
        access: WriteAccess,
        limit: Option<usize>,
        order_by_primary_key: bool,
        returning: Option<Vec<ReturningProjection>>,
    },
}

#[derive(Clone, Debug)]
pub enum TableAlterOperation {
    AddColumn {
        column: ColumnSchema,
        if_not_exists: bool,
    },
    ModifyColumn {
        column_name: String,
        column: ColumnSchema,
    },
    DropColumn {
        column_name: String,
        if_exists: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
    SetDefault {
        column_name: String,
        default: Option<String>,
    },
    SetNotNull {
        column_name: String,
        nullable: bool,
    },
    AddCheck {
        constraint: CheckConstraintSchema,
    },
    AddUnique {
        constraint: UniqueConstraintSchema,
    },
    AddForeignKey {
        constraint: ForeignKeyConstraintSchema,
    },
    DropForeignKey {
        name: String,
        if_exists: bool,
    },
    AddIndex {
        index_name: String,
        column_names: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    RenameIndex {
        old_name: String,
        new_name: String,
    },
    SetAutoIncrement {
        value: i64,
    },
}

#[derive(Clone, Debug)]
pub enum ReadAccess {
    PointLookup {
        key: ColumnValue,
    },
    PrimaryKeyInLookup {
        keys: Vec<ColumnValue>,
    },
    PrimaryKeyRangeScan {
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    SecondaryIndexLookup {
        index_name: String,
        column_name: String,
        key: ColumnValue,
        filter: Option<Expr>,
    },
    SecondaryIndexRangeScan {
        index_name: String,
        column_name: String,
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    PrefixScan {
        filter: Option<Expr>,
    },
}

#[derive(Clone, Debug)]
pub enum WriteAccess {
    PointLookup {
        key: ColumnValue,
    },
    PrimaryKeyRangeScan {
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    SecondaryIndexLookup {
        index_name: String,
        key: ColumnValue,
        filter: Option<Expr>,
    },
    SecondaryIndexRangeScan {
        index_name: String,
        lower: Option<(ColumnValue, bool)>,
        upper: Option<(ColumnValue, bool)>,
        filter: Option<Expr>,
    },
    PrefixScan {
        filter: Option<Expr>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── DataType tests ────────────────────────────────────────────────

    #[test]
    fn data_type_from_sql_int_variants() {
        assert_eq!(DataType::from_sql("INT"), DataType::Int32);
        assert_eq!(DataType::from_sql("int4"), DataType::Int32);
        assert_eq!(DataType::from_sql("INTEGER"), DataType::Int32);
        assert_eq!(DataType::from_sql("SERIAL"), DataType::Int32);
    }

    #[test]
    fn data_type_from_sql_bigint_variants() {
        assert_eq!(DataType::from_sql("BIGINT"), DataType::Int64);
        assert_eq!(DataType::from_sql("int8"), DataType::Int64);
        assert_eq!(DataType::from_sql("BIGSERIAL"), DataType::Int64);
    }

    #[test]
    fn data_type_from_sql_bool_variants() {
        assert_eq!(DataType::from_sql("BOOL"), DataType::Boolean);
        assert_eq!(DataType::from_sql("boolean"), DataType::Boolean);
    }

    #[test]
    fn data_type_from_sql_mysql_type_variants() {
        assert_eq!(
            DataType::from_sql("TINYINT UNSIGNED"),
            DataType::MySqlInt {
                kind: MySqlIntKind::Tiny,
                unsigned: true,
            }
        );
        assert_eq!(
            DataType::from_sql("MEDIUMINT"),
            DataType::MySqlInt {
                kind: MySqlIntKind::Medium,
                unsigned: false,
            }
        );
        assert_eq!(DataType::from_sql("BIT(8)"), DataType::Bit(Some(8)));
        assert_eq!(DataType::from_sql("YEAR"), DataType::Year);
        assert_eq!(
            DataType::from_sql("ENUM('new','paid')"),
            DataType::Enum(vec!["new".into(), "paid".into()])
        );
        assert_eq!(
            DataType::from_sql("SET('red','blue')"),
            DataType::Set(vec!["red".into(), "blue".into()])
        );
        assert_eq!(DataType::from_sql("BINARY(4)"), DataType::Binary(Some(4)));
        assert_eq!(
            DataType::from_sql("VARBINARY(12)"),
            DataType::VarBinary(Some(12))
        );
        assert_eq!(
            DataType::from_sql("DATETIME(6)"),
            DataType::MySqlDateTime { fsp: Some(6) }
        );
    }

    #[test]
    fn data_type_from_sql_text_fallback() {
        assert_eq!(DataType::from_sql("TEXT"), DataType::Text);
        assert_eq!(DataType::from_sql("VARCHAR"), DataType::VarChar(None));
        assert_eq!(
            DataType::from_sql("VARCHAR(12)"),
            DataType::VarChar(Some(12))
        );
        assert_eq!(DataType::from_sql("CHAR"), DataType::Char(Some(1)));
        assert_eq!(
            DataType::from_sql("unknown_type"),
            DataType::Domain("unknown_type".into())
        );
    }

    #[test]
    fn data_type_from_sql_phase2_and_arrays() {
        assert_eq!(DataType::from_sql("REAL"), DataType::Float32);
        assert_eq!(DataType::from_sql("DOUBLE PRECISION"), DataType::Float64);
        assert_eq!(
            DataType::from_sql("NUMERIC(10,2)"),
            DataType::Numeric {
                precision: Some(10),
                scale: Some(2)
            }
        );
        assert_eq!(DataType::from_sql("DATE"), DataType::Date);
        assert_eq!(DataType::from_sql("TIMESTAMP"), DataType::Timestamp);
        assert_eq!(
            DataType::from_sql("TIMESTAMP WITH TIME ZONE"),
            DataType::TimestampTz
        );
        assert_eq!(DataType::from_sql("UUID"), DataType::Uuid);
        assert_eq!(DataType::from_sql("BYTEA"), DataType::Bytea);
        assert_eq!(DataType::from_sql("JSONB"), DataType::Jsonb);
        assert_eq!(
            DataType::from_sql("INT[]"),
            DataType::Array(Box::new(DataType::Int32))
        );
    }

    #[test]
    fn data_type_to_pg_type() {
        assert_eq!(DataType::Int32.to_pg_type(), Type::INT4);
        assert_eq!(DataType::Int64.to_pg_type(), Type::INT8);
        assert_eq!(DataType::Float32.to_pg_type(), Type::FLOAT4);
        assert_eq!(DataType::Float64.to_pg_type(), Type::FLOAT8);
        assert_eq!(
            DataType::Numeric {
                precision: None,
                scale: None
            }
            .to_pg_type(),
            Type::NUMERIC
        );
        assert_eq!(DataType::Text.to_pg_type(), Type::TEXT);
        assert_eq!(DataType::Boolean.to_pg_type(), Type::BOOL);
        assert_eq!(DataType::Jsonb.to_pg_type(), Type::JSONB);
        assert_eq!(
            DataType::Array(Box::new(DataType::Int32)).to_pg_type(),
            Type::INT4_ARRAY
        );
    }

    #[test]
    fn data_type_roundtrip_str() {
        for dt in [
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Numeric {
                precision: None,
                scale: None,
            },
            DataType::Text,
            DataType::Boolean,
            DataType::Date,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Uuid,
            DataType::Bytea,
            DataType::Json,
            DataType::Jsonb,
            DataType::Array(Box::new(DataType::Int32)),
        ] {
            assert_eq!(DataType::from_sql(&dt.to_str()), dt);
        }
    }

    // ── ColumnValue tests ─────────────────────────────────────────────

    #[test]
    fn column_value_to_text() {
        assert_eq!(ColumnValue::Null.to_text(), None);
        assert_eq!(ColumnValue::Int32(42).to_text(), Some("42".into()));
        assert_eq!(ColumnValue::Int64(-100).to_text(), Some("-100".into()));
        assert_eq!(ColumnValue::Float64(1.5).to_text(), Some("1.5".into()));
        assert_eq!(
            ColumnValue::Bytea(vec![0xde, 0xad]).to_text(),
            Some("\\xdead".into())
        );
        assert_eq!(ColumnValue::Text("hi".into()).to_text(), Some("hi".into()));
        assert_eq!(ColumnValue::Boolean(true).to_text(), Some("true".into()));
    }

    #[test]
    fn column_value_is_null() {
        assert!(ColumnValue::Null.is_null());
        assert!(!ColumnValue::Int32(0).is_null());
        assert!(!ColumnValue::Text("".into()).is_null());
    }

    #[test]
    fn column_value_ordering_int32() {
        assert!(ColumnValue::Int32(-1) < ColumnValue::Int32(0));
        assert!(ColumnValue::Int32(0) < ColumnValue::Int32(1));
        assert!(ColumnValue::Int32(2) < ColumnValue::Int32(10));
        assert_eq!(
            ColumnValue::Int32(5).partial_cmp(&ColumnValue::Int32(5)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn column_value_ordering_cross_int() {
        assert!(ColumnValue::Int32(1) < ColumnValue::Int64(2));
        assert!(ColumnValue::Int64(1) < ColumnValue::Int32(2));
        assert_eq!(
            ColumnValue::Int32(42).partial_cmp(&ColumnValue::Int64(42)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn column_value_ordering_text() {
        assert!(ColumnValue::Text("a".into()) < ColumnValue::Text("b".into()));
        assert!(ColumnValue::Text("abc".into()) < ColumnValue::Text("abd".into()));
    }

    #[test]
    fn column_value_ordering_incompatible_returns_none() {
        assert_eq!(
            ColumnValue::Int32(1).partial_cmp(&ColumnValue::Text("1".into())),
            None
        );
        assert_eq!(
            ColumnValue::Boolean(true).partial_cmp(&ColumnValue::Int32(1)),
            None
        );
    }

    // ── TableSchema tests ─────────────────────────────────────────────

    fn test_schema() -> TableSchema {
        TableSchema {
            table_name: "users".into(),
            table_id: 1,
            schema_version: 1,
            table_epoch: 1,
            primary_key: "id".into(),
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            columns: vec![
                ColumnSchema {
                    column_id: 0,
                    name: "id".into(),
                    data_type: DataType::Int32,
                    primary_key: true,
                    nullable: false,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
                ColumnSchema {
                    column_id: 0,
                    name: "name".into(),
                    data_type: DataType::Text,
                    primary_key: false,
                    nullable: true,
                    default: None,

                    on_update: None,

                    character_set: None,

                    collation: None,
                },
            ],
        }
    }

    #[test]
    fn schema_pk_data_type() {
        let schema = test_schema();
        assert_eq!(schema.pk_data_type(), &DataType::Int32);
    }

    #[test]
    fn schema_find_column() {
        let schema = test_schema();
        assert!(schema.find_column("id").unwrap().primary_key);
        assert!(schema.find_column("name").is_some());
        assert!(schema.find_column("missing").is_none());
    }

    #[test]
    fn schema_column_names() {
        let schema = test_schema();
        assert_eq!(schema.column_names(), vec!["id", "name"]);
    }

    // ── apply_assignments tests ───────────────────────────────────────

    #[test]
    fn apply_assignments_overwrites_existing() {
        let mut row = RowMap::new();
        row.insert("name".into(), ColumnValue::Text("old".into()));

        apply_assignments(
            &mut row,
            &[("name".into(), ColumnValue::Text("new".into()))],
        );

        assert_eq!(row.get("name"), Some(&ColumnValue::Text("new".into())));
    }

    #[test]
    fn apply_assignments_adds_new_column() {
        let mut row = RowMap::new();
        apply_assignments(&mut row, &[("age".into(), ColumnValue::Int32(25))]);
        assert_eq!(row.get("age"), Some(&ColumnValue::Int32(25)));
    }

    // ── parse_column_schema tests ─────────────────────────────────────

    #[test]
    fn parse_column_schema_full() {
        let json = serde_json::json!({
            "name": "age",
            "data_type": "INT4",
            "primary_key": false,
            "nullable": true,
        });
        let col = parse_column_schema(&json).unwrap();
        assert_eq!(col.name, "age");
        assert_eq!(col.data_type, DataType::Int32);
        assert!(!col.primary_key);
        assert!(col.nullable);
    }

    #[test]
    fn parse_column_schema_defaults() {
        let json = serde_json::json!({ "name": "x" });
        let col = parse_column_schema(&json).unwrap();
        assert_eq!(col.data_type, DataType::Text);
        assert!(!col.primary_key);
        assert!(col.nullable);
    }

    #[test]
    fn parse_column_schema_missing_name_errors() {
        let json = serde_json::json!({ "data_type": "INT4" });
        assert!(parse_column_schema(&json).is_err());
    }
}

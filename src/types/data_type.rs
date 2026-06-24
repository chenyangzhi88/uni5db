use pgwire::api::Type;

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
        let numbers = mysql_type_numbers(raw);
        let first_number = numbers.first().copied();
        let varchar_len =
            if upper.starts_with("VARCHAR(") || upper.starts_with("CHARACTER VARYING(") {
                first_number
            } else {
                None
            };
        let char_len = if upper.starts_with("CHAR(")
            || upper.starts_with("CHARACTER(")
            || upper.starts_with("BPCHAR(")
        {
            first_number
        } else {
            None
        };
        let binary_len = if upper.starts_with("BINARY(") {
            first_number
        } else {
            None
        };
        let varbinary_len = if upper.starts_with("VARBINARY(") {
            first_number
        } else {
            None
        };
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

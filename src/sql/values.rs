use super::*;

#[derive(Clone, Copy)]
pub struct EvalContext<'a> {
    pub row: &'a RowMap,
    pub excluded_row: Option<&'a RowMap>,
}

// ── SQL value → ColumnValue conversion ────────────────────────────────

pub fn sql_expr_to_column_value(expr: &Expr, data_type: &DataType) -> PgWireResult<ColumnValue> {
    match expr {
        Expr::Cast {
            expr,
            data_type: cast_type,
            ..
        } => {
            let casted_type = DataType::from_sql(&cast_type.to_string());
            let value = sql_expr_to_column_value(expr, &casted_type)?;
            coerce_column_value(value, data_type)
        }
        Expr::TypedString(TypedString {
            value,
            data_type: typed_data_type,
            ..
        }) => {
            let casted_type = DataType::from_sql(&typed_data_type.to_string());
            let value = sql_expr_to_column_value(&Expr::Value(value.clone()), &casted_type)?;
            coerce_column_value(value, data_type)
        }
        Expr::Function(function)
            if function
                .name
                .to_string()
                .eq_ignore_ascii_case("current_timestamp")
                || function.name.to_string().eq_ignore_ascii_case("now") =>
        {
            coerce_column_value(ColumnValue::Text(current_timestamp_text()), data_type)
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_timestamp") => {
            coerce_column_value(ColumnValue::Text(current_timestamp_text()), data_type)
        }
        Expr::Value(value) => {
            match &value.value {
                SqlValue::Number(v, _) => match data_type {
                    DataType::Int16 => v.parse::<i16>().map(ColumnValue::Int16).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT2"))
                    }),
                    DataType::Int32 => v.parse::<i32>().map(ColumnValue::Int32).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT4"))
                    }),
                    DataType::Int64 => v.parse::<i64>().map(ColumnValue::Int64).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for INT8"))
                    }),
                    DataType::Float32 => v.parse::<f32>().map(ColumnValue::Float32).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for FLOAT4"))
                    }),
                    DataType::Float64 => v.parse::<f64>().map(ColumnValue::Float64).map_err(|_| {
                        user_error("22003", format!("value '{v}' out of range for FLOAT8"))
                    }),
                    DataType::MySqlInt { .. }
                    | DataType::Bit(_)
                    | DataType::Year
                    | DataType::Numeric { .. } => parse_text_for_type(v, data_type),
                    _ => parse_text_for_type(v, data_type),
                },
                SqlValue::SingleQuotedString(v) | SqlValue::DoubleQuotedString(v) => {
                    parse_text_for_type(v, data_type)
                }
                SqlValue::Boolean(v) => coerce_column_value(ColumnValue::Boolean(*v), data_type),
                SqlValue::Null => Ok(ColumnValue::Null),
                _ => Err(unsupported("only literal values are supported")),
            }
        }
        Expr::Array(array) => match data_type {
            DataType::Array(inner) => array
                .elem
                .iter()
                .map(|expr| sql_expr_to_column_value(expr, inner))
                .collect::<PgWireResult<Vec<_>>>()
                .map(ColumnValue::Array),
            _ => Err(unsupported("ARRAY literal requires an array column type")),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(value) if matches!(&value.value, SqlValue::Number(_, _)) => {
                let SqlValue::Number(v, _) = &value.value else {
                    unreachable!();
                };
                let neg = format!("-{v}");
                match data_type {
                    DataType::Int16 => neg.parse::<i16>().map(ColumnValue::Int16).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT2"))
                    }),
                    DataType::Int32 => neg.parse::<i32>().map(ColumnValue::Int32).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT4"))
                    }),
                    DataType::Int64 => neg.parse::<i64>().map(ColumnValue::Int64).map_err(|_| {
                        user_error("22003", format!("value '{neg}' out of range for INT8"))
                    }),
                    DataType::Float32 => {
                        neg.parse::<f32>().map(ColumnValue::Float32).map_err(|_| {
                            user_error("22003", format!("value '{neg}' out of range for FLOAT4"))
                        })
                    }
                    DataType::Float64 => {
                        neg.parse::<f64>().map(ColumnValue::Float64).map_err(|_| {
                            user_error("22003", format!("value '{neg}' out of range for FLOAT8"))
                        })
                    }
                    DataType::MySqlInt { .. } => parse_text_for_type(&neg, data_type),
                    _ => parse_text_for_type(&neg, data_type),
                }
            }
            _ => Err(unsupported("only literal values are supported")),
        },
        _ => Err(unsupported("only literal values are supported")),
    }
}

pub fn is_default_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("default"))
}

pub fn column_default_value(default_sql: &str, data_type: &DataType) -> PgWireResult<ColumnValue> {
    let expr = parse_default_expr(default_sql)?;
    sql_expr_to_column_value(&expr, data_type)
}

pub fn nextval_sequence_name(default_sql: &str) -> Option<String> {
    let lower = default_sql.trim().to_ascii_lowercase();
    if !lower.starts_with("nextval(") || !lower.ends_with(')') {
        return None;
    }
    let inner = default_sql
        .trim()
        .strip_prefix("nextval(")?
        .strip_suffix(')')?
        .trim();
    Some(
        inner
            .trim_matches('\'')
            .trim_matches('"')
            .trim_end_matches("::regclass")
            .trim_matches('\'')
            .to_string(),
    )
}

pub(super) fn parse_default_expr(default_sql: &str) -> PgWireResult<Expr> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &format!("SELECT {default_sql}"))
        .map_err(|e| user_error("42601", format!("invalid default expression: {e}")))?;
    let Some(Statement::Query(query)) = statements.into_iter().next() else {
        return Err(user_error("42601", "invalid default expression"));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(user_error("42601", "invalid default expression"));
    };
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expr)] => Ok(expr.clone()),
        _ => Err(user_error("42601", "invalid default expression")),
    }
}

pub(super) fn current_timestamp_text() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let days = seconds / 86_400;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

pub(super) fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m as u32, d as u32)
}

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
        (ColumnValue::Text(v), DataType::Int16) => v
            .parse::<i16>()
            .map(ColumnValue::Int16)
            .map_err(|_| user_error("22P02", format!("invalid input for INT2: '{v}'"))),
        (ColumnValue::Text(v), DataType::Int32) => v
            .parse::<i32>()
            .map(ColumnValue::Int32)
            .map_err(|_| user_error("22P02", format!("invalid input for INT4: '{v}'"))),
        (ColumnValue::Text(v), DataType::Int64) => v
            .parse::<i64>()
            .map(ColumnValue::Int64)
            .map_err(|_| user_error("22P02", format!("invalid input for INT8: '{v}'"))),
        (ColumnValue::Text(v), DataType::Boolean) => match v.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(ColumnValue::Boolean(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(ColumnValue::Boolean(false)),
            _ => Err(user_error(
                "22P02",
                format!("invalid input for BOOL: '{v}'"),
            )),
        },
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

pub(crate) fn parse_text_for_type(raw: &str, data_type: &DataType) -> PgWireResult<ColumnValue> {
    match data_type {
        DataType::MySqlInt { kind, unsigned } => parse_mysql_integer(raw, *kind, *unsigned),
        DataType::Int16 => raw
            .parse::<i16>()
            .map(ColumnValue::Int16)
            .map_err(|_| user_error("22P02", format!("invalid input for INT2: '{raw}'"))),
        DataType::Int32 => raw
            .parse::<i32>()
            .map(ColumnValue::Int32)
            .map_err(|_| user_error("22P02", format!("invalid input for INT4: '{raw}'"))),
        DataType::Int64 => raw
            .parse::<i64>()
            .map(ColumnValue::Int64)
            .map_err(|_| user_error("22P02", format!("invalid input for INT8: '{raw}'"))),
        DataType::Float32 | DataType::MySqlFloat { .. } => raw
            .parse::<f32>()
            .map(ColumnValue::Float32)
            .map_err(|_| user_error("22P02", format!("invalid input for FLOAT4: '{raw}'"))),
        DataType::Float64 | DataType::MySqlDouble { .. } => raw
            .parse::<f64>()
            .map(ColumnValue::Float64)
            .map_err(|_| user_error("22P02", format!("invalid input for FLOAT8: '{raw}'"))),
        DataType::Numeric { .. } => coerce_decimal_text(raw.to_string(), data_type),
        DataType::Boolean => match raw.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(ColumnValue::Boolean(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(ColumnValue::Boolean(false)),
            _ => Err(user_error(
                "22P02",
                format!("invalid input for BOOL: '{raw}'"),
            )),
        },
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
    body.split(',')
        .map(|part| {
            let part = part.trim().trim_matches('"');
            if part.eq_ignore_ascii_case("NULL") {
                Ok(ColumnValue::Null)
            } else {
                parse_text_for_type(part, inner)
            }
        })
        .collect::<PgWireResult<Vec<_>>>()
        .map(ColumnValue::Array)
}

// ── SQL AST extraction helpers ────────────────────────────────────────

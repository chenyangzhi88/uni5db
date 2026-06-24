use std::time::{SystemTime, UNIX_EPOCH};

use pgwire::error::PgWireResult;
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::sql_expr_to_column_value;
use crate::error::user_error;
use crate::types::{ColumnValue, DataType};

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

fn parse_default_expr(default_sql: &str) -> PgWireResult<Expr> {
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

pub(in crate::sql) fn current_timestamp_text() -> String {
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

pub(in crate::sql) fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
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

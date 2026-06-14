use pgwire::error::PgWireResult;
use sqlparser::ast::{Expr, Statement};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDialect {
    Postgres,
    MySql,
}

pub fn parse_sql(dialect: SqlDialect, sql: &str) -> PgWireResult<Vec<Statement>> {
    match dialect {
        SqlDialect::Postgres => super::postgres::parse_sql(sql),
        SqlDialect::MySql => super::mysql::parse_sql(sql),
    }
}

pub fn parse_check_expr(expr_sql: &str) -> PgWireResult<Expr> {
    super::postgres::parse_check_expr(expr_sql)
}

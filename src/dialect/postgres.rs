use pgwire::error::PgWireResult;
use sqlparser::ast::{Expr, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::error::user_error;

pub fn parse_sql(sql: &str) -> PgWireResult<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    let rewritten_copy = rewrite_copy_freeze_on_off(sql);
    let rewritten_catalog_casts =
        rewrite_pg_catalog_oid_alias_casts(rewritten_copy.as_deref().unwrap_or(sql));
    let parse_input = rewritten_catalog_casts
        .as_deref()
        .or(rewritten_copy.as_deref())
        .unwrap_or(sql);
    match Parser::parse_sql(&dialect, parse_input) {
        Ok(statements) => Ok(expand_multi_table_truncate_statements(statements)),
        Err(error) => parse_offset_comma_limit(parse_input)
            .or_else(|| parse_copy_from_stdin(parse_input))
            .or_else(|| parse_multi_table_truncate(parse_input))
            .ok_or_else(|| user_error("42601", format!("sql parse error: {error}"))),
    }
}

pub fn parse_check_expr(expr_sql: &str) -> PgWireResult<Expr> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &format!("SELECT {expr_sql}"))
        .map_err(|e| user_error("42601", format!("invalid CHECK expression: {e}")))?;
    let Some(Statement::Query(query)) = statements.into_iter().next() else {
        return Err(user_error("42601", "invalid CHECK expression"));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(user_error("42601", "invalid CHECK expression"));
    };
    match select.projection.as_slice() {
        [sqlparser::ast::SelectItem::UnnamedExpr(expr)] => Ok(expr.clone()),
        _ => Err(user_error("42601", "invalid CHECK expression")),
    }
}

fn parse_offset_comma_limit(sql: &str) -> Option<Vec<Statement>> {
    let rewritten = rewrite_offset_comma_limit(sql)?;
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, &rewritten).ok()
}

fn parse_copy_from_stdin(sql: &str) -> Option<Vec<Statement>> {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') || !trimmed.to_ascii_lowercase().contains("from stdin") {
        return None;
    }

    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, &format!("{trimmed};")).ok()
}

fn parse_multi_table_truncate(sql: &str) -> Option<Vec<Statement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = "truncate table ";
    if !lower.starts_with(prefix) {
        return None;
    }

    let rest = trimmed[prefix.len()..].trim();
    if rest.is_empty() || !rest.contains(',') {
        return None;
    }

    let dialect = PostgreSqlDialect {};
    let mut statements = Vec::new();
    for table_name in rest.split(',') {
        let table_name = table_name.trim();
        if table_name.is_empty() {
            return None;
        }
        let sql = format!("TRUNCATE TABLE {table_name}");
        let mut parsed = Parser::parse_sql(&dialect, &sql).ok()?;
        if parsed.len() != 1 {
            return None;
        }
        statements.push(parsed.remove(0));
    }
    Some(statements)
}

fn expand_multi_table_truncate_statements(statements: Vec<Statement>) -> Vec<Statement> {
    let mut expanded = Vec::with_capacity(statements.len());
    for statement in statements {
        match statement {
            Statement::Truncate(truncate) if truncate.table_names.len() > 1 => {
                for target in truncate.table_names.clone() {
                    let mut single = truncate.clone();
                    single.table_names = vec![target];
                    expanded.push(Statement::Truncate(single));
                }
            }
            statement => expanded.push(statement),
        }
    }
    expanded
}

fn rewrite_offset_comma_limit(sql: &str) -> Option<String> {
    let trimmed_end = sql.trim_end();
    let (parse_end, semicolon) = if let Some(without_semicolon) = trimmed_end.strip_suffix(';') {
        (without_semicolon.trim_end().len(), true)
    } else {
        (trimmed_end.len(), false)
    };
    let body = &sql[..parse_end];
    let limit_start = find_top_level_limit_keyword(body)?;
    let limit_expr_start = limit_start + "limit".len();
    let comma = find_top_level_comma(body, limit_expr_start)?;

    let offset = body[limit_expr_start..comma].trim();
    let count = body[comma + 1..].trim();
    if offset.is_empty() || count.is_empty() {
        return None;
    }

    let mut rewritten = String::with_capacity(sql.len() + " OFFSET ".len());
    rewritten.push_str(&body[..limit_start]);
    rewritten.push_str("LIMIT ");
    rewritten.push_str(count);
    rewritten.push_str(" OFFSET ");
    rewritten.push_str(offset);
    if semicolon {
        rewritten.push(';');
    }
    Some(rewritten)
}

fn rewrite_copy_freeze_on_off(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("copy") || !lower.contains("freeze") {
        return None;
    }

    let mut out = String::with_capacity(sql.len() + 4);
    let mut idx = 0usize;
    let bytes = sql.as_bytes();
    let mut changed = false;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                let end = skip_single_quoted_sql(sql, idx)?;
                out.push_str(&sql[idx..end]);
                idx = end;
            }
            b'"' => {
                let end = skip_double_quoted_sql(sql, idx)?;
                out.push_str(&sql[idx..end]);
                idx = end;
            }
            _ if keyword_at(sql, idx, "freeze") => {
                let keyword_end = idx + "freeze".len();
                out.push_str(&sql[idx..keyword_end]);
                idx = keyword_end;

                let whitespace_start = idx;
                while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                    idx += 1;
                }
                if idx > whitespace_start && keyword_at(sql, idx, "on") {
                    out.push(' ');
                    out.push_str("true");
                    idx += "on".len();
                    changed = true;
                } else if idx > whitespace_start && keyword_at(sql, idx, "off") {
                    out.push(' ');
                    out.push_str("false");
                    idx += "off".len();
                    changed = true;
                } else {
                    out.push_str(&sql[whitespace_start..idx]);
                }
            }
            _ => {
                let ch = sql[idx..].chars().next()?;
                out.push(ch);
                idx += ch.len_utf8();
            }
        }
    }

    changed.then_some(out)
}

fn rewrite_pg_catalog_oid_alias_casts(sql: &str) -> Option<String> {
    const OID_ALIAS_TYPES: &[&str] = &[
        "regclass",
        "regcollation",
        "regconfig",
        "regdictionary",
        "regnamespace",
        "regoper",
        "regoperator",
        "regproc",
        "regprocedure",
        "regrole",
        "regtype",
    ];

    let lower = sql.to_ascii_lowercase();
    if !OID_ALIAS_TYPES
        .iter()
        .any(|type_name| lower.contains(type_name))
    {
        return None;
    }

    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let bytes = sql.as_bytes();
    let mut changed = false;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => {
                let end = skip_single_quoted_sql(sql, idx)?;
                out.push_str(&sql[idx..end]);
                idx = end;
            }
            b'"' => {
                let end = skip_double_quoted_sql(sql, idx)?;
                out.push_str(&sql[idx..end]);
                idx = end;
            }
            b':' if bytes.get(idx + 1) == Some(&b':') => {
                let type_start = idx + 2;
                if let Some(type_name) = OID_ALIAS_TYPES
                    .iter()
                    .find(|type_name| keyword_at(sql, type_start, type_name))
                {
                    out.push_str("::int4");
                    idx = type_start + type_name.len();
                    changed = true;
                } else {
                    out.push(':');
                    idx += 1;
                }
            }
            _ => {
                let ch = sql[idx..].chars().next()?;
                out.push(ch);
                idx += ch.len_utf8();
            }
        }
    }

    changed.then_some(out)
}

fn find_top_level_limit_keyword(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    let mut depth = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quoted_sql(sql, idx)?,
            b'"' => idx = skip_double_quoted_sql(sql, idx)?,
            b'(' => {
                depth = depth.saturating_add(1);
                idx += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
            }
            _ if depth == 0 && keyword_at(sql, idx, "limit") => return Some(idx),
            _ => idx += 1,
        }
    }
    None
}

fn find_top_level_comma(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start;
    let mut depth = 0usize;
    while idx < bytes.len() {
        match bytes[idx] {
            b'\'' => idx = skip_single_quoted_sql(sql, idx)?,
            b'"' => idx = skip_double_quoted_sql(sql, idx)?,
            b'(' => {
                depth = depth.saturating_add(1);
                idx += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
            }
            b',' if depth == 0 => return Some(idx),
            _ => idx += 1,
        }
    }
    None
}

fn keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let end = idx + keyword.len();
    sql.get(idx..end)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
        && !sql[..idx]
            .chars()
            .next_back()
            .is_some_and(is_sql_identifier_char)
        && !sql[end..]
            .chars()
            .next()
            .is_some_and(is_sql_identifier_char)
}

fn is_sql_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

fn skip_single_quoted_sql(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        if bytes[idx] == b'\'' {
            if bytes.get(idx + 1) == Some(&b'\'') {
                idx += 2;
            } else {
                return Some(idx + 1);
            }
        } else {
            idx += 1;
        }
    }
    None
}

fn skip_double_quoted_sql(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = start + 1;
    while idx < bytes.len() {
        if bytes[idx] == b'"' {
            if bytes.get(idx + 1) == Some(&b'"') {
                idx += 2;
            } else {
                return Some(idx + 1);
            }
        } else {
            idx += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{CopyOption, Statement};

    use super::parse_sql;

    #[test]
    fn parses_pgbench_copy_freeze_on() {
        let statements =
            parse_sql("COPY pgbench_accounts FROM stdin WITH (freeze on, delimiter E'\\t');")
                .unwrap();
        let Statement::Copy { options, .. } = &statements[0] else {
            panic!("expected COPY statement");
        };
        assert!(
            options
                .iter()
                .any(|option| matches!(option, CopyOption::Freeze(true)))
        );
    }

    #[test]
    fn parses_copy_freeze_off() {
        let statements =
            parse_sql("COPY pgbench_accounts FROM stdin WITH (freeze off, delimiter E'\\t');")
                .unwrap();
        let Statement::Copy { options, .. } = &statements[0] else {
            panic!("expected COPY statement");
        };
        assert!(
            options
                .iter()
                .any(|option| matches!(option, CopyOption::Freeze(false)))
        );
    }

    #[test]
    fn parses_pg_catalog_regtype_casts_as_int4() {
        let statements = parse_sql("SELECT oid::regtype::text FROM pg_type;").unwrap();
        assert_eq!(statements.len(), 1);
    }
}

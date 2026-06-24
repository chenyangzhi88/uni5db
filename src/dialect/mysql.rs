use pgwire::error::PgWireResult;
use sqlparser::ast::{Ident, ObjectName, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::error::user_error;

pub fn parse_sql(sql: &str) -> PgWireResult<Vec<Statement>> {
    let dialect = MySqlDialect {};
    let sql = rewrite_mysql_metadata_query(sql).unwrap_or_else(|| sql.to_string());
    Parser::parse_sql(&dialect, &sql)
        .or_else(|error| parse_mysql_optimize_table(&sql).or(Err(error)))
        .map_err(|error| user_error("42601", format!("sql parse error: {error}")))
}

fn parse_mysql_optimize_table(sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let Some(table_name) = first_identifier_after(trimmed, &["optimize table "]) else {
        return Err(sqlparser::parser::ParserError::ParserError(
            "not an OPTIMIZE TABLE statement".to_string(),
        ));
    };
    Ok(vec![Statement::OptimizeTable {
        name: mysql_object_name(&table_name),
        has_table_keyword: true,
        on_cluster: None,
        partition: None,
        include_final: false,
        deduplicate: None,
        predicate: None,
        zorder: None,
    }])
}

fn mysql_object_name(name: &str) -> ObjectName {
    ObjectName::from(
        name.split('.')
            .map(|part| Ident::new(strip_mysql_identifier(part)))
            .collect::<Vec<_>>(),
    )
}

fn rewrite_mysql_metadata_query(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "show tables" | "show full tables" => {
            return Some(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
                    .to_string(),
            );
        }
        _ => {}
    }

    if let Some(table) =
        first_identifier_after(trimmed, &["show columns from ", "show fields from "])
            .or_else(|| first_identifier_after(trimmed, &["describe ", "desc "]))
    {
        let table = table.replace('\'', "''");
        return Some(format!(
            "SELECT column_name AS Field, data_type AS Type, is_nullable AS nullable, '' AS key_name, column_default AS default_value, '' AS Extra FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position"
        ));
    }

    None
}

fn first_identifier_after(query: &str, prefixes: &[&str]) -> Option<String> {
    let lower = query.to_ascii_lowercase();
    for prefix in prefixes {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let raw = &query[query.len() - rest.len()..];
            let ident = raw.split_whitespace().next()?;
            return Some(strip_mysql_identifier(ident));
        }
    }
    None
}

fn strip_mysql_identifier(value: &str) -> String {
    value
        .trim()
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_sql;
    use sqlparser::ast::{OnInsert, SetExpr, Statement};

    #[test]
    fn rewrites_show_tables_to_information_schema_query() {
        let statements = parse_sql("SHOW TABLES").unwrap();
        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::Query(_)));
        assert!(
            statements[0]
                .to_string()
                .contains("information_schema.tables")
        );
    }

    #[test]
    fn rewrites_describe_to_information_schema_columns_query() {
        let statements = parse_sql("DESCRIBE `users`").unwrap();
        assert_eq!(statements.len(), 1);
        assert!(
            statements[0]
                .to_string()
                .contains("information_schema.columns")
        );
    }

    #[test]
    fn parses_limit_offset_count_syntax() {
        let statements = parse_sql("SELECT id FROM users ORDER BY id LIMIT 5, 10").unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        assert!(matches!(query.body.as_ref(), SetExpr::Select(_)));
    }

    #[test]
    fn parses_on_duplicate_key_update() {
        let statements = parse_sql(
            "INSERT INTO users (id, name) VALUES (1, 'alice') ON DUPLICATE KEY UPDATE name = VALUES(name)",
        )
        .unwrap();
        let Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert");
        };
        assert!(matches!(insert.on, Some(OnInsert::DuplicateKeyUpdate(_))));
    }

    #[test]
    fn parses_optimize_table() {
        let statements = parse_sql("OPTIMIZE TABLE `users`").unwrap();
        assert_eq!(statements.len(), 1);
        assert!(matches!(statements[0], Statement::OptimizeTable { .. }));
    }
}

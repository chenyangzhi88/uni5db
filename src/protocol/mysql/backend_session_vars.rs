use opensrv_mysql::Column;

use super::{
    MYSQL_AUTOCOMMIT, MYSQL_CHARACTER_SET_CLIENT, MYSQL_CHARACTER_SET_CONNECTION,
    MYSQL_CHARACTER_SET_RESULTS, MYSQL_COLLATION_CONNECTION, MYSQL_DEFAULT_SQL_MODE,
    MYSQL_LOCK_WAIT_TIMEOUT, MYSQL_SERVER_VERSION, MYSQL_SQL_MODE, MYSQL_TIME_ZONE,
    MYSQL_TRANSACTION_ISOLATION, MySqlBackend, MySqlSystemVariableValue,
};

impl MySqlBackend {
    pub(super) fn mysql_system_variable_select(
        &self,
        normalized: &str,
    ) -> Option<(Vec<Column>, Vec<MySqlSystemVariableValue>)> {
        let mut select_list = normalized.strip_prefix("select ")?;
        if !select_list.starts_with("@@") {
            return None;
        }
        if let Some((before_from, from_target)) = select_list.rsplit_once(" from ") {
            if from_target.trim() != "dual" {
                return None;
            }
            select_list = before_from.trim();
        }
        if let Some((before_limit, limit)) = select_list.rsplit_once(" limit ")
            && limit.trim().parse::<u64>().ok() == Some(1)
        {
            select_list = before_limit.trim();
        }

        let mut columns = Vec::new();
        let mut values = Vec::new();
        for expr in select_list.split(',') {
            let expr = expr.trim();
            let (variable_expr, alias) = Self::mysql_select_expr_alias(expr);
            let value = self.mysql_system_variable_value(variable_expr)?;
            let column_name = alias.unwrap_or(variable_expr);
            columns.push(match value {
                MySqlSystemVariableValue::Int(_) => Self::int_column(column_name),
                MySqlSystemVariableValue::Text(_) => Self::text_column(column_name),
            });
            values.push(value);
        }
        (!columns.is_empty()).then_some((columns, values))
    }

    pub(super) fn mysql_select_expr_alias(expr: &str) -> (&str, Option<&str>) {
        if let Some((left, right)) = expr.rsplit_once(" as ") {
            let alias = right.trim().trim_matches('`').trim_matches('"');
            if !alias.is_empty() {
                return (left.trim(), Some(alias));
            }
        }
        (expr, None)
    }

    pub(super) fn mysql_system_variable_value(
        &self,
        variable_expr: &str,
    ) -> Option<MySqlSystemVariableValue> {
        let variable = variable_expr
            .trim()
            .trim_start_matches("@@")
            .strip_prefix("session.")
            .or_else(|| {
                variable_expr
                    .trim()
                    .trim_start_matches("@@")
                    .strip_prefix("global.")
            })
            .unwrap_or_else(|| variable_expr.trim().trim_start_matches("@@"));

        match variable {
            "auto_increment_increment" => Some(MySqlSystemVariableValue::Int(1)),
            "autocommit" => Some(MySqlSystemVariableValue::Int(
                self.session_value(MYSQL_AUTOCOMMIT, "1")
                    .parse::<i32>()
                    .unwrap_or(1),
            )),
            "character_set_client" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_CLIENT, "utf8mb4"),
            )),
            "character_set_connection" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_CONNECTION, "utf8mb4"),
            )),
            "character_set_results" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_CHARACTER_SET_RESULTS, "utf8mb4"),
            )),
            "collation_connection" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_COLLATION_CONNECTION, "utf8mb4_0900_ai_ci"),
            )),
            "innodb_lock_wait_timeout" => Some(MySqlSystemVariableValue::Int(
                self.session_value(MYSQL_LOCK_WAIT_TIMEOUT, "50")
                    .parse::<i32>()
                    .unwrap_or(50),
            )),
            "license" => Some(MySqlSystemVariableValue::Text("GPL".to_string())),
            "lower_case_table_names" => Some(MySqlSystemVariableValue::Int(0)),
            "max_allowed_packet" => Some(MySqlSystemVariableValue::Int(67_108_864)),
            "net_buffer_length" => Some(MySqlSystemVariableValue::Int(16_384)),
            "net_write_timeout" => Some(MySqlSystemVariableValue::Int(60)),
            "query_cache_size" => Some(MySqlSystemVariableValue::Int(0)),
            "read_only" | "transaction_read_only" => Some(MySqlSystemVariableValue::Int(0)),
            "sql_mode" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_SQL_MODE, MYSQL_DEFAULT_SQL_MODE),
            )),
            "system_time_zone" => Some(MySqlSystemVariableValue::Text("UTC".to_string())),
            "time_zone" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_TIME_ZONE, "SYSTEM"),
            )),
            "transaction_isolation" | "tx_isolation" => Some(MySqlSystemVariableValue::Text(
                self.session_value(MYSQL_TRANSACTION_ISOLATION, "REPEATABLE-READ"),
            )),
            "version" => Some(MySqlSystemVariableValue::Text(
                MYSQL_SERVER_VERSION.to_string(),
            )),
            "version_comment" => Some(MySqlSystemVariableValue::Text(
                "UniDB MySQL compatibility layer".to_string(),
            )),
            "wait_timeout" => Some(MySqlSystemVariableValue::Int(28_800)),
            "warning_count" => Some(MySqlSystemVariableValue::Int(
                self.client.warnings.len().min(i32::MAX as usize) as i32,
            )),
            _ => None,
        }
    }

    pub(super) fn strip_mysql_identifier(value: &str) -> String {
        value
            .trim()
            .trim_end_matches(';')
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .to_string()
    }

    pub(super) fn first_identifier_after<'a>(query: &'a str, prefixes: &[&str]) -> Option<String> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        for prefix in prefixes {
            if let Some(rest) = lower.strip_prefix(prefix) {
                let raw = &trimmed[trimmed.len() - rest.len()..];
                let ident = raw.split_whitespace().next()?;
                return Some(Self::strip_mysql_identifier(ident));
            }
        }
        None
    }

    pub(super) fn sql_string(value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
    }

    pub(super) fn show_like_pattern(query: &str) -> Option<String> {
        let trimmed = query.trim().trim_end_matches(';').trim();
        let lower = trimmed.to_ascii_lowercase();
        let idx = lower.find(" like ")?;
        Some(Self::strip_mysql_identifier(&trimmed[idx + 6..]))
    }

    pub(super) fn set_mysql_session_variable(&mut self, normalized: &str, original: &str) -> bool {
        if normalized.starts_with("set names ") {
            let charset = original
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .nth(2)
                .map(Self::strip_mysql_identifier)
                .unwrap_or_else(|| "utf8mb4".to_string());
            for key in [
                MYSQL_CHARACTER_SET_CLIENT,
                MYSQL_CHARACTER_SET_CONNECTION,
                MYSQL_CHARACTER_SET_RESULTS,
            ] {
                self.client
                    .metadata
                    .insert(key.to_string(), charset.clone());
            }
            return true;
        }
        if normalized.starts_with("set character set ") {
            let charset = original
                .trim()
                .trim_end_matches(';')
                .split_whitespace()
                .nth(3)
                .map(Self::strip_mysql_identifier)
                .unwrap_or_else(|| "utf8mb4".to_string());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_CLIENT.to_string(), charset.clone());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_CONNECTION.to_string(), charset.clone());
            self.client
                .metadata
                .insert(MYSQL_CHARACTER_SET_RESULTS.to_string(), charset);
            return true;
        }
        for (prefix, key) in [
            ("set autocommit", MYSQL_AUTOCOMMIT),
            ("set session autocommit", MYSQL_AUTOCOMMIT),
            ("set @@autocommit", MYSQL_AUTOCOMMIT),
            ("set sql_mode", MYSQL_SQL_MODE),
            ("set session sql_mode", MYSQL_SQL_MODE),
            ("set time_zone", MYSQL_TIME_ZONE),
            ("set session time_zone", MYSQL_TIME_ZONE),
            ("set innodb_lock_wait_timeout", MYSQL_LOCK_WAIT_TIMEOUT),
            (
                "set session innodb_lock_wait_timeout",
                MYSQL_LOCK_WAIT_TIMEOUT,
            ),
            ("set @@innodb_lock_wait_timeout", MYSQL_LOCK_WAIT_TIMEOUT),
        ] {
            if normalized.starts_with(prefix) {
                let mut value = original
                    .split_once('=')
                    .map(|(_, value)| Self::strip_mysql_identifier(value))
                    .unwrap_or_default();
                if key == MYSQL_AUTOCOMMIT {
                    value = if value == "0" || value.eq_ignore_ascii_case("off") {
                        "0".to_string()
                    } else {
                        "1".to_string()
                    };
                } else if key == MYSQL_SQL_MODE && value.eq_ignore_ascii_case("default") {
                    value = MYSQL_DEFAULT_SQL_MODE.to_string();
                }
                self.client.metadata.insert(key.to_string(), value);
                return true;
            }
        }
        false
    }

    pub(super) fn mysql_transaction_isolation_sql(normalized: &str) -> Option<&'static str> {
        if !normalized.contains("transaction isolation level") {
            return None;
        }
        if normalized.contains("read committed") {
            Some("read committed")
        } else if normalized.contains("repeatable read") {
            Some("repeatable read")
        } else if normalized.contains("serializable") {
            Some("serializable")
        } else {
            None
        }
    }

    pub(super) fn is_transaction_control(normalized: &str) -> bool {
        matches!(
            normalized,
            "begin" | "start transaction" | "commit" | "end" | "rollback"
        )
    }

    pub(super) fn autocommit_disabled(&self) -> bool {
        self.client
            .metadata
            .get(MYSQL_AUTOCOMMIT)
            .is_some_and(|value| value == "0" || value.eq_ignore_ascii_case("off"))
    }
}

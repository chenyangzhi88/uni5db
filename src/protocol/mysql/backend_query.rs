use super::*;

impl MySqlBackend {
    pub(super) async fn handle_query<'a, W>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> std::io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let normalized = Self::normalize_query(query);
        if normalized.is_empty() {
            return results
                .error(ErrorKind::ER_EMPTY_QUERY, b"empty query")
                .await;
        }
        if normalized != "show warnings" && normalized != "show count(*) warnings" {
            self.client.clear_warnings();
        }

        if let Some((columns, values)) = self.mysql_system_variable_select(&normalized) {
            return Self::mysql_system_variable_row(columns, values, results).await;
        }
        if normalized.contains("@@session.auto_increment_increment")
            || normalized.contains("@@auto_increment_increment")
        {
            return Self::single_int_row("@@session.auto_increment_increment", 1, results).await;
        }

        match normalized.as_str() {
            "select 1" => return Self::single_int_row("1", 1, results).await,
            "select version()" | "select @@version" => {
                return Self::single_text_row("VERSION()", MYSQL_SERVER_VERSION, results).await;
            }
            "select @@version_comment" | "select @@version_comment limit 1" => {
                return Self::single_text_row(
                    "@@version_comment",
                    "UniDB MySQL compatibility layer",
                    results,
                )
                .await;
            }
            "select database()" | "select schema()" => {
                let database = self.current_database();
                return Self::single_text_row("DATABASE()", database, results).await;
            }
            "select user()" | "select current_user()" | "select current_user" => {
                return Self::single_text_row("USER()", "root@localhost", results).await;
            }
            "select @@autocommit" | "select @@session.autocommit" => {
                let value = self
                    .session_value(MYSQL_AUTOCOMMIT, "1")
                    .parse::<i32>()
                    .unwrap_or(1);
                return Self::single_int_row("@@autocommit", value, results).await;
            }
            "select @@innodb_lock_wait_timeout" | "select @@session.innodb_lock_wait_timeout" => {
                let value = self
                    .session_value(MYSQL_LOCK_WAIT_TIMEOUT, "50")
                    .parse::<i32>()
                    .unwrap_or(50);
                return Self::single_int_row("@@innodb_lock_wait_timeout", value, results).await;
            }
            "select @@transaction_isolation"
            | "select @@session.transaction_isolation"
            | "select @@tx_isolation"
            | "select @@session.tx_isolation" => {
                let value = self.session_value(MYSQL_TRANSACTION_ISOLATION, "REPEATABLE-READ");
                return Self::single_text_row("@@transaction_isolation", &value, results).await;
            }
            "select @@sql_mode" | "select @@session.sql_mode" => {
                let value = self.session_value(MYSQL_SQL_MODE, MYSQL_DEFAULT_SQL_MODE);
                return Self::single_text_row("@@sql_mode", &value, results).await;
            }
            "select last_insert_id()" => {
                return Self::single_int_row(
                    "LAST_INSERT_ID()",
                    self.client.last_insert_id.min(i32::MAX as u64) as i32,
                    results,
                )
                .await;
            }
            "select @@warning_count" | "select @@session.warning_count" => {
                return Self::single_int_row(
                    "@@warning_count",
                    self.client.warnings.len().min(i32::MAX as usize) as i32,
                    results,
                )
                .await;
            }
            "select @@time_zone" | "select @@session.time_zone" => {
                let value = self.session_value(MYSQL_TIME_ZONE, "SYSTEM");
                return Self::single_text_row("@@time_zone", &value, results).await;
            }
            "select @@character_set_client" => {
                let value = self.session_value(MYSQL_CHARACTER_SET_CLIENT, "utf8mb4");
                return Self::single_text_row("@@character_set_client", &value, results).await;
            }
            "select @@character_set_connection" => {
                let value = self.session_value(MYSQL_CHARACTER_SET_CONNECTION, "utf8mb4");
                return Self::single_text_row("@@character_set_connection", &value, results).await;
            }
            "select @@character_set_results" => {
                let value = self.session_value(MYSQL_CHARACTER_SET_RESULTS, "utf8mb4");
                return Self::single_text_row("@@character_set_results", &value, results).await;
            }
            "show databases" => {
                return match self.server.mysql_show_databases().await {
                    Ok(databases) => Self::write_show_databases(databases, results).await,
                    Err(error) => {
                        let kind = Self::mysql_error_kind_for_pgwire(&error);
                        results.error(kind, error.to_string().as_bytes()).await
                    }
                };
            }
            "show tables" => {
                let column = format!("Tables_in_{}", self.current_database());
                return match self.server.mysql_show_tables(self.current_database()).await {
                    Ok(tables) => Self::write_show_tables(&column, tables, false, results).await,
                    Err(error) => {
                        let kind = Self::mysql_error_kind_for_pgwire(&error);
                        results.error(kind, error.to_string().as_bytes()).await
                    }
                };
            }
            "show full tables" => {
                let column = format!("Tables_in_{}", self.current_database());
                return match self.server.mysql_show_tables(self.current_database()).await {
                    Ok(tables) => Self::write_show_tables(&column, tables, true, results).await,
                    Err(error) => {
                        let kind = Self::mysql_error_kind_for_pgwire(&error);
                        results.error(kind, error.to_string().as_bytes()).await
                    }
                };
            }
            "show engines" => {
                return self
                    .run_gateway_query(
                        "SELECT engine AS `Engine`, support AS `Support`, comment AS `Comment`, transactions AS `Transactions`, xa AS `XA`, savepoints AS `Savepoints` FROM information_schema.engines ORDER BY engine",
                        results,
                    )
                    .await;
            }
            "show character set" | "show character sets" => {
                return self
                    .run_gateway_query(
                        "SELECT character_set_name AS `Charset`, description AS `Description`, default_collate_name AS `Default collation`, maxlen AS `Maxlen` FROM information_schema.character_sets ORDER BY character_set_name",
                        results,
                    )
                    .await;
            }
            "show collation" | "show collations" => {
                return self
                    .run_gateway_query(
                        "SELECT collation_name AS `Collation`, character_set_name AS `Charset`, id AS `Id`, is_default AS `Default`, is_compiled AS `Compiled`, sortlen AS `Sortlen` FROM information_schema.collations ORDER BY collation_name",
                        results,
                    )
                    .await;
            }
            _ if normalized.starts_with("show variables")
                || normalized.starts_with("show session variables")
                || normalized.starts_with("show global variables") =>
            {
                let table = if normalized.starts_with("show global variables") {
                    "information_schema.global_variables"
                } else {
                    "information_schema.session_variables"
                };
                let mut sql = format!(
                    "SELECT variable_name AS `Variable_name`, variable_value AS `Value` FROM {table}"
                );
                if let Some(pattern) = Self::show_like_pattern(query) {
                    sql.push_str(" WHERE variable_name LIKE ");
                    sql.push_str(&Self::sql_string(&pattern));
                }
                sql.push_str(" ORDER BY variable_name");
                return self.run_gateway_query(&sql, results).await;
            }
            _ if normalized.starts_with("show table status") => {
                let database = self.current_database().replace('\'', "''");
                let mut sql = format!(
                    "SELECT table_name AS `Name`, engine AS `Engine`, version AS `Version`, row_format AS `Row_format`, table_rows AS `Rows`, avg_row_length AS `Avg_row_length`, data_length AS `Data_length`, max_data_length AS `Max_data_length`, index_length AS `Index_length`, data_free AS `Data_free`, auto_increment AS `Auto_increment`, create_time AS `Create_time`, update_time AS `Update_time`, check_time AS `Check_time`, table_collation AS `Collation`, checksum AS `Checksum`, create_options AS `Create_options`, table_comment AS `Comment` FROM information_schema.tables WHERE table_schema = '{database}'"
                );
                if let Some(pattern) = Self::show_like_pattern(query) {
                    sql.push_str(" AND table_name LIKE ");
                    sql.push_str(&Self::sql_string(&pattern));
                }
                sql.push_str(" ORDER BY table_name");
                return self.run_gateway_query(&sql, results).await;
            }
            "show warnings" => {
                let columns = [
                    Self::text_column("Level"),
                    Self::int_column("Code"),
                    Self::text_column("Message"),
                ];
                let mut rows = results.start(&columns).await?;
                for warning in &self.client.warnings {
                    rows.write_col(warning.level)?;
                    rows.write_col(warning.code as i32)?;
                    rows.write_col(warning.message.as_str())?;
                    rows.end_row().await?;
                }
                return rows.finish().await;
            }
            "show count(*) warnings" => {
                return Self::single_int_row(
                    "@@session.warning_count",
                    self.client.warnings.len().min(i32::MAX as usize) as i32,
                    results,
                )
                .await;
            }
            _ if normalized.starts_with("set ") => {
                if let Some(isolation) = Self::mysql_transaction_isolation_sql(&normalized) {
                    self.client.metadata.insert(
                        MYSQL_TRANSACTION_ISOLATION.to_string(),
                        isolation.replace(' ', "-").to_ascii_uppercase(),
                    );
                    return self
                        .run_gateway_query(
                            &format!("SET TRANSACTION ISOLATION LEVEL {isolation}"),
                            results,
                        )
                        .await;
                }
                if self.set_mysql_session_variable(&normalized, query) {
                    if normalized.starts_with("set autocommit")
                        || normalized.starts_with("set session autocommit")
                        || normalized.starts_with("set @@autocommit")
                    {
                        if self.autocommit_disabled()
                            && self.client.transaction_status == TransactionStatus::Idle
                        {
                            if let Err(error) =
                                self.server.run_query(&mut self.client, "BEGIN").await
                            {
                                let kind = Self::mysql_error_kind_for_pgwire(&error);
                                return results.error(kind, error.to_string().as_bytes()).await;
                            }
                        } else if !self.autocommit_disabled()
                            && self.client.transaction_status == TransactionStatus::Transaction
                        {
                            if let Err(error) =
                                self.server.run_query(&mut self.client, "COMMIT").await
                            {
                                let kind = Self::mysql_error_kind_for_pgwire(&error);
                                return results.error(kind, error.to_string().as_bytes()).await;
                            }
                        }
                    }
                    return results.completed(OkResponse::default()).await;
                }
            }
            _ => {}
        }

        if let Some(database) = Self::first_identifier_after(query, &["use "]) {
            return self.switch_database(&database, results).await;
        }
        if let Some(database) =
            Self::first_identifier_after(query, &["show create database ", "show create schema "])
        {
            let create = format!(
                "CREATE DATABASE `{}` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */",
                database.replace('`', "``")
            );
            return Self::single_text_pair_row(
                ["Database", "Create Database"],
                [database, create],
                results,
            )
            .await;
        }
        if let Some(table) = Self::first_identifier_after(query, &["show create table "]) {
            let table = Self::strip_mysql_identifier(&table);
            return match self
                .server
                .mysql_show_create_table(self.current_database(), &table)
                .await
            {
                Ok(create) => {
                    Self::single_text_pair_row(["Table", "Create Table"], [table, create], results)
                        .await
                }
                Err(error) => {
                    let kind = Self::mysql_error_kind_for_pgwire(&error);
                    results.error(kind, error.to_string().as_bytes()).await
                }
            };
        }
        if let Some(table) = Self::first_identifier_after(
            query,
            &["show index from ", "show indexes from ", "show keys from "],
        ) {
            let table = Self::strip_mysql_identifier(&table);
            let database = self.current_database().replace('\'', "''");
            let table_sql = table.replace('\'', "''");
            let sql = format!(
                "SELECT table_name AS `Table`, non_unique AS `Non_unique`, index_name AS `Key_name`, seq_in_index AS `Seq_in_index`, column_name AS `Column_name`, collation AS `Collation`, cardinality AS `Cardinality`, sub_part AS `Sub_part`, packed AS `Packed`, nullable AS `Null`, index_type AS `Index_type`, comment AS `Comment`, index_comment AS `Index_comment`, is_visible AS `Visible`, expression AS `Expression` FROM information_schema.statistics WHERE table_schema = '{database}' AND table_name = '{table_sql}' ORDER BY index_name, seq_in_index"
            );
            return self.run_gateway_query(&sql, results).await;
        }
        if let Some(table) = Self::first_identifier_after(
            query,
            &[
                "show columns from ",
                "show full columns from ",
                "show fields from ",
                "show full fields from ",
            ],
        ) {
            let table = Self::strip_mysql_identifier(&table);
            return match self
                .server
                .mysql_describe_table_fast(self.current_database(), &table)
                .await
            {
                Ok(rows) => Self::write_describe_rows(rows, results).await,
                Err(error) => {
                    let kind = Self::mysql_error_kind_for_pgwire(&error);
                    results.error(kind, error.to_string().as_bytes()).await
                }
            };
        }
        if let Some(table) = Self::first_identifier_after(query, &["describe ", "desc "]) {
            let table = Self::strip_mysql_identifier(&table);
            return match self
                .server
                .mysql_describe_table_fast(self.current_database(), &table)
                .await
            {
                Ok(rows) => Self::write_describe_rows(rows, results).await,
                Err(error) => {
                    let kind = Self::mysql_error_kind_for_pgwire(&error);
                    results.error(kind, error.to_string().as_bytes()).await
                }
            };
        }

        if self.autocommit_disabled()
            && self.client.transaction_status == TransactionStatus::Idle
            && !Self::is_transaction_control(&normalized)
            && !normalized.starts_with("set ")
        {
            if let Err(error) = self.server.run_query(&mut self.client, "BEGIN").await {
                let kind = Self::mysql_error_kind_for_pgwire(&error);
                return results.error(kind, error.to_string().as_bytes()).await;
            }
        }

        match parser::parse_sql(SqlDialect::MySql, query) {
            Ok(_) => {}
            Err(error) => {
                self.client.record_warning(1064, error.to_string());
                return results
                    .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await;
            }
        }
        self.run_gateway_query(query, results).await
    }
}

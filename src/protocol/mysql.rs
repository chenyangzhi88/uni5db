const MYSQL_SERVER_VERSION: &str = "8.0.0-unidb";
const MYSQL_DEFAULT_SQL_MODE: &str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
const MYSQL_AUTOCOMMIT: &str = "mysql.autocommit";
const MYSQL_SQL_MODE: &str = "mysql.sql_mode";
const MYSQL_TIME_ZONE: &str = "mysql.time_zone";
const MYSQL_CHARACTER_SET_CLIENT: &str = "mysql.character_set_client";
const MYSQL_CHARACTER_SET_CONNECTION: &str = "mysql.character_set_connection";
const MYSQL_CHARACTER_SET_RESULTS: &str = "mysql.character_set_results";
const MYSQL_COLLATION_CONNECTION: &str = "mysql.collation_connection";
const MYSQL_TRANSACTION_ISOLATION: &str = "mysql.transaction_isolation";
const MYSQL_LOCK_WAIT_TIMEOUT: &str = "mysql.innodb_lock_wait_timeout";
const MYSQL_CHARSET_UTF8_GENERAL_CI: u16 = 0x21;
const MYSQL_CHARSET_BINARY: u16 = 0x3f;

mod serve;
pub use serve::serve;
use serve::{MySqlBackend, MySqlPreparedStatement, MySqlSystemVariableValue, MySqlWarning};
mod client_state;
use client_state::{
    MySqlClientState, affected_rows_from_tag, decode_pg_text_row, escape_sql_string,
    mysql_insert_result_from_tag, render_mysql_date, render_mysql_datetime, render_mysql_time,
};
mod backend_columns;
mod backend_metadata;
mod backend_prepared_bind;
mod backend_query;
mod backend_results;
mod backend_session_vars;
mod shim;
#[cfg(test)]
mod tests;

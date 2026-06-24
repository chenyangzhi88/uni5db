use std::sync::Arc;

use opensrv_mysql::{AsyncMysqlShim, CapabilityFlags, ColumnFlags, ColumnType, ErrorKind};
use pgwire::api::Type;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::messages::data::DataRow;

use super::client_state::{
    MySqlClientState, affected_rows_from_tag, decode_pg_text_row, mysql_insert_result_from_tag,
    render_mysql_date, render_mysql_datetime, render_mysql_time,
};
use super::serve::MySqlBackend;
use super::{
    MYSQL_AUTOCOMMIT, MYSQL_CHARACTER_SET_CLIENT, MYSQL_CHARSET_BINARY, MYSQL_DEFAULT_SQL_MODE,
    MYSQL_SQL_MODE,
};
use crate::core::server::GatewayServer;
use crate::mode::GatewayMode;

#[test]
fn parses_affected_rows_from_command_tag() {
    assert_eq!(affected_rows_from_tag("INSERT 0 3"), 3);
    assert_eq!(affected_rows_from_tag("UPDATE 12"), 12);
    assert_eq!(affected_rows_from_tag("CREATE TABLE"), 0);
}

#[test]
fn parses_mysql_insert_result_tag() {
    assert_eq!(
        mysql_insert_result_from_tag("MYSQL_INSERT 2 42"),
        Some((2, 42))
    );
    assert_eq!(mysql_insert_result_from_tag("INSERT 0 1"), None);
}

#[test]
fn mysql_client_defaults_include_sql_mode_and_warning_state() {
    let mut client = MySqlClientState::default();
    assert_eq!(
        client.metadata.get(MYSQL_SQL_MODE).map(String::as_str),
        Some(MYSQL_DEFAULT_SQL_MODE)
    );
    client.record_warning(1265, "Data truncated".to_string());
    assert_eq!(client.warnings.len(), 1);
    client.clear_warnings();
    assert!(client.warnings.is_empty());
}

#[test]
fn decodes_pgwire_text_data_row() {
    let mut data = Vec::new();
    data.extend_from_slice(&3i32.to_be_bytes());
    data.extend_from_slice(b"abc");
    data.extend_from_slice(&(-1i32).to_be_bytes());
    data.extend_from_slice(&2i32.to_be_bytes());
    data.extend_from_slice(b"42");

    let row = DataRow::new((&data[..]).into(), 3);
    let values = decode_pg_text_row(&row).unwrap();
    assert_eq!(values[0], Some(b"abc".to_vec()));
    assert_eq!(values[1], None);
    assert_eq!(values[2], Some(b"42".to_vec()));
}

#[test]
fn rejects_truncated_pgwire_text_data_row() {
    let row = DataRow::new((&[0, 0, 0][..]).into(), 1);
    assert!(decode_pg_text_row(&row).is_err());

    let mut data = Vec::new();
    data.extend_from_slice(&4i32.to_be_bytes());
    data.extend_from_slice(b"ab");
    let row = DataRow::new((&data[..]).into(), 1);
    assert!(decode_pg_text_row(&row).is_err());
}

#[test]
fn extracts_mysql_metadata_identifiers() {
    assert_eq!(
        MySqlBackend::first_identifier_after("SHOW COLUMNS FROM `users`", &["show columns from "]),
        Some("users".to_string())
    );
    assert_eq!(
        MySqlBackend::first_identifier_after("DESC app.users", &["describe ", "desc "]),
        Some("app.users".to_string())
    );
}

#[test]
fn records_mysql_session_variables() {
    let server = Arc::new(GatewayServer::with_mode(
        Arc::new(crate::mem_store::MemoryKvStore::new()),
        GatewayMode::MySql,
    ));
    let mut backend = MySqlBackend::new(server);
    assert!(backend.set_mysql_session_variable("set names utf8mb4", "SET NAMES utf8mb4"));
    assert_eq!(
        backend.client.metadata.get(MYSQL_CHARACTER_SET_CLIENT),
        Some(&"utf8mb4".to_string())
    );
    assert!(backend.set_mysql_session_variable("set autocommit = 0", "SET autocommit = 0"));
    assert_eq!(
        backend.client.metadata.get(MYSQL_AUTOCOMMIT),
        Some(&"0".to_string())
    );
}

#[test]
fn binds_mysql_prepared_statement_parameters() {
    let sql = "INSERT INTO users (name, note) VALUES (?, '?') WHERE id = ?";
    assert_eq!(MySqlBackend::count_placeholders(sql), 2);
    let bound = MySqlBackend::bind_prepared_sql(sql, &["'alice'".into(), "7".into()]).unwrap();
    assert_eq!(
        bound,
        "INSERT INTO users (name, note) VALUES ('alice', '?') WHERE id = 7"
    );
}

#[test]
fn binds_mysql_prepared_statement_skips_comments_and_identifiers() {
    let sql = "SELECT `?` FROM users -- ? ignored\nWHERE name = ? # ? ignored\nAND note = '?'";
    assert_eq!(MySqlBackend::count_placeholders(sql), 1);
    let bound = MySqlBackend::bind_prepared_sql(sql, &["'alice'".into()]).unwrap();
    assert_eq!(
        bound,
        "SELECT `?` FROM users -- ? ignored\nWHERE name = 'alice' # ? ignored\nAND note = '?'"
    );
}

#[test]
fn bind_mysql_prepared_statement_rejects_wrong_parameter_count() {
    assert_eq!(
        MySqlBackend::bind_prepared_sql("SELECT ? + ?", &["1".into()]).unwrap_err(),
        "not enough prepared statement parameters"
    );
    assert_eq!(
        MySqlBackend::bind_prepared_sql("SELECT ?", &["1".into(), "2".into()]).unwrap_err(),
        "too many prepared statement parameters"
    );
}

#[test]
fn maps_pgwire_columns_to_mysql_metadata() {
    let field = FieldInfo::new("id".into(), None, None, Type::INT4, FieldFormat::Text);
    let column = MySqlBackend::mysql_column(&field);
    assert_eq!(column.column, "id");
    assert_eq!(column.collen, 11);
    assert_eq!(column.charset, Some(MYSQL_CHARSET_BINARY));
    assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_LONG);
    assert!(column.colflags.contains(ColumnFlags::NUM_FLAG));
    assert_eq!(column.decimals, Some(0));

    let field = FieldInfo::new("payload".into(), None, None, Type::BYTEA, FieldFormat::Text);
    let column = MySqlBackend::mysql_column(&field);
    assert_eq!(column.coltype, ColumnType::MYSQL_TYPE_BLOB);
    assert_eq!(column.charset, Some(MYSQL_CHARSET_BINARY));
    assert!(column.colflags.contains(ColumnFlags::BLOB_FLAG));
    assert!(column.colflags.contains(ColumnFlags::BINARY_FLAG));
}

#[test]
fn describes_prepared_select_metadata() {
    let columns =
        MySqlBackend::prepared_result_columns("SELECT 1 AS id, name, CAST(? AS SIGNED) n");
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].column, "id");
    assert_eq!(columns[0].coltype, ColumnType::MYSQL_TYPE_NEWDECIMAL);
    assert!(columns[0].colflags.contains(ColumnFlags::NUM_FLAG));
    assert_eq!(columns[1].column, "name");
    assert_eq!(columns[2].column, "n");
    assert_eq!(columns[2].coltype, ColumnType::MYSQL_TYPE_LONG);
}

#[test]
fn renders_binary_prepared_parameters_as_hex() {
    assert_eq!(MySqlBackend::render_mysql_bytes_param(b"abc"), "'abc'");
    assert_eq!(MySqlBackend::render_mysql_bytes_param(b"a\0b"), "X'610062'");
    assert_eq!(
        MySqlBackend::render_mysql_hex_param(&[0, 255, 16]),
        "X'00ff10'"
    );
}

#[test]
fn renders_mysql_temporal_binary_parameters() {
    assert_eq!(render_mysql_date(&[0xe9, 0x07, 6, 14]), "'2025-06-14'");
    assert_eq!(
        render_mysql_datetime(&[0xe9, 0x07, 6, 14, 12, 34, 56]),
        "'2025-06-14 12:34:56'"
    );
    assert_eq!(
        render_mysql_datetime(&[0xe9, 0x07, 6, 14, 12, 34, 56, 0x40, 0xe2, 0x01, 0]),
        "'2025-06-14 12:34:56.123456'"
    );
    assert_eq!(
        render_mysql_time(&[0, 1, 0, 0, 0, 2, 3, 4, 0x40, 0xe2, 0x01, 0]),
        "'26:03:04.123456'"
    );
    assert_eq!(render_mysql_date(&[1, 2]), "NULL");
    assert_eq!(render_mysql_datetime(&[1, 2, 3]), "NULL");
    assert_eq!(render_mysql_time(&[1, 2, 3]), "NULL");
}

#[test]
fn maps_postgres_sqlstate_to_mysql_errors() {
    assert_eq!(
        MySqlBackend::mysql_error_kind_for_sqlstate("23505", "duplicate key"),
        ErrorKind::ER_DUP_ENTRY
    );
    assert_eq!(
        MySqlBackend::mysql_error_kind_for_sqlstate("42P01", "relation does not exist"),
        ErrorKind::ER_NO_SUCH_TABLE
    );
    assert_eq!(
        MySqlBackend::mysql_error_kind_for_sqlstate("22001", "value too long"),
        ErrorKind::ER_DATA_TOO_LONG
    );
    assert_eq!(
        MySqlBackend::mysql_error_kind_for_sqlstate("40001", "deadlock detected"),
        ErrorKind::ER_LOCK_DEADLOCK
    );
}

#[test]
fn advertises_and_records_mysql_client_capabilities() {
    let server = Arc::new(GatewayServer::with_mode(
        Arc::new(crate::mem_store::MemoryKvStore::new()),
        GatewayMode::MySql,
    ));
    let mut backend = MySqlBackend::new(server);
    let advertised = <MySqlBackend as AsyncMysqlShim<tokio::io::Sink>>::server_capabilities(
        &backend,
        CapabilityFlags::CLIENT_PROTOCOL_41,
    );
    assert!(advertised.contains(CapabilityFlags::CLIENT_FOUND_ROWS));
    assert!(advertised.contains(CapabilityFlags::CLIENT_MULTI_STATEMENTS));
    assert!(advertised.contains(CapabilityFlags::CLIENT_MULTI_RESULTS));
    assert!(advertised.contains(CapabilityFlags::CLIENT_CONNECT_ATTRS));
    assert!(!advertised.contains(CapabilityFlags::CLIENT_COMPRESS));

    <MySqlBackend as AsyncMysqlShim<tokio::io::Sink>>::on_client_capabilities(
        &mut backend,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_FOUND_ROWS,
        &[("program_name".to_string(), "mysql".to_string())],
    );
    assert!(
        backend
            .client
            .client_capabilities
            .contains(CapabilityFlags::CLIENT_FOUND_ROWS)
    );
    assert_eq!(
        backend.client.connection_attrs.get("program_name"),
        Some(&"mysql".to_string())
    );
}

#[test]
fn normalizes_mysql_autocommit_values() {
    let server = Arc::new(GatewayServer::with_mode(
        Arc::new(crate::mem_store::MemoryKvStore::new()),
        GatewayMode::MySql,
    ));
    let mut backend = MySqlBackend::new(server);
    assert!(backend.set_mysql_session_variable("set autocommit = off", "SET autocommit = OFF"));
    assert_eq!(
        backend.client.metadata.get(MYSQL_AUTOCOMMIT),
        Some(&"0".to_string())
    );
    assert!(backend.set_mysql_session_variable("set autocommit = on", "SET autocommit = ON"));
    assert_eq!(
        backend.client.metadata.get(MYSQL_AUTOCOMMIT),
        Some(&"1".to_string())
    );
}

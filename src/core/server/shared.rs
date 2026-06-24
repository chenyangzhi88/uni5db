use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use ::datafusion::catalog::CatalogProvider;
use async_trait::async_trait;
use pgwire::api::auth::md5pass::hash_md5_password;
use pgwire::api::auth::sasl::{SASLState, scram};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::results::Response;
use pgwire::error::{PgWireError, PgWireResult};
use sqlparser::ast::{ColumnOption, Expr, Statement};

use crate::catalog::{CatalogStore, IndexCatalog};
use crate::core::response::command_complete;
use crate::dialect::{TransactionIsolationLevel, parser};
use crate::error::{unsupported, user_error};
use crate::mem_store::{KvStore, KvTransaction};
use crate::mode::GatewayMode;
use crate::sql::nextval_sequence_name;
use crate::types::{ColumnSchema, ColumnValue, DataType, TableSchema};

pub(super) fn is_unsupported_error(e: &PgWireError) -> bool {
    match e {
        PgWireError::UserError(info) => info.code == "0A000",
        _ => false,
    }
}

pub(super) fn next_key_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] += 1;
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }
    None
}

pub(super) const FILTERED_LIMIT_SCAN_BATCH_SIZE: usize = 4096;

pub(super) fn filtered_limit_scan_batch_size(
    scan_limit: Option<usize>,
    row_offset: usize,
) -> usize {
    scan_limit
        .map(|limit| limit.saturating_add(row_offset).saturating_mul(4))
        .unwrap_or(FILTERED_LIMIT_SCAN_BATCH_SIZE)
        .clamp(256, FILTERED_LIMIT_SCAN_BATCH_SIZE)
}

pub(super) fn parse_mysql_quoted_literal(input: &str) -> Option<(String, &str)> {
    let mut chars = input.char_indices();
    let (_, quote) = chars.next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut out = String::new();
    let mut escaped = false;
    for (idx, ch) in chars {
        if escaped {
            out.push(match ch {
                '0' => '\0',
                'b' => '\u{0008}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'Z' => '\u{001a}',
                other => other,
            });
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Some((out, &input[idx + ch.len_utf8()..]));
        }
        out.push(ch);
    }
    None
}

pub(super) fn parse_mysql_identifier_token(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    if let Some(rest) = input.strip_prefix('`') {
        let end = rest.find('`')?;
        return Some((rest[..end].to_string(), &rest[end + 1..]));
    }
    let end = input
        .char_indices()
        .find_map(|(idx, ch)| {
            (!(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')).then_some(idx)
        })
        .unwrap_or(input.len());
    (end > 0).then(|| {
        (
            strip_mysql_identifier_token(&input[..end]).to_string(),
            &input[end..],
        )
    })
}

pub(super) fn strip_mysql_identifier_token(input: &str) -> &str {
    input.trim().trim_matches('`')
}

pub(super) fn single_char_literal(value: &str, clause: &str) -> PgWireResult<char> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(user_error("42000", format!("{clause} cannot be empty")));
    };
    if chars.next().is_some() {
        return Err(unsupported(format!("{clause} must be a single character")));
    }
    Ok(ch)
}

pub struct GatewayServer {
    pub(super) store: Arc<dyn KvStore>,
    pub(super) mode: GatewayMode,
    pub(super) catalog: CatalogStore,
    pub(super) datafusion_user_catalogs:
        tokio::sync::RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
    pub(super) active_transactions: tokio::sync::Mutex<HashMap<i32, Box<dyn KvTransaction>>>,
    pub(super) active_transaction_snapshots: tokio::sync::Mutex<HashMap<i32, u64>>,
    pub(super) active_transaction_isolations:
        tokio::sync::Mutex<HashMap<i32, TransactionIsolation>>,
    pub(super) active_transaction_read_only: tokio::sync::Mutex<HashSet<i32>>,
    pub(super) active_mysql_current_reads: tokio::sync::Mutex<HashSet<i32>>,
    pub(super) active_mysql_locks: tokio::sync::Mutex<Vec<MySqlLock>>,
    pub(super) active_mysql_lock_queue: tokio::sync::Mutex<VecDeque<MySqlLockWaiter>>,
    pub(super) active_mysql_lock_waits: tokio::sync::Mutex<HashMap<i32, i32>>,
    pub(super) mysql_lock_notify: tokio::sync::Notify,
    pub(super) active_sql_prepared:
        tokio::sync::Mutex<HashMap<i32, HashMap<String, PreparedSqlStatement>>>,
    pub(super) active_copy_in: tokio::sync::Mutex<HashMap<i32, CopyInState>>,
    pub(super) cancelled_sessions: tokio::sync::Mutex<HashSet<i32>>,
    pub(super) mvcc_clock: AtomicU64,
}

#[derive(Clone, Debug)]
pub(super) enum GatewayAuthMethod {
    Trust,
    Cleartext,
    Md5,
    Scram,
}

impl GatewayAuthMethod {
    pub(super) fn from_env() -> Self {
        match std::env::var("PG_GATEWAY_AUTH_METHOD")
            .unwrap_or_else(|_| "trust".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "cleartext" | "password" => Self::Cleartext,
            "md5" => Self::Md5,
            "scram" | "scram-sha-256" => Self::Scram,
            _ => Self::Trust,
        }
    }
}

#[derive(Debug)]
pub(super) struct GatewayAuthSource {
    pub(super) user: Option<String>,
    pub(super) password: String,
}

impl GatewayAuthSource {
    pub(super) fn from_env() -> Self {
        Self {
            user: std::env::var("PG_GATEWAY_AUTH_USER").ok(),
            password: std::env::var("PG_GATEWAY_AUTH_PASSWORD")
                .unwrap_or_else(|_| "postgres".to_string()),
        }
    }

    pub(super) fn salt(len: usize) -> Vec<u8> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        (0..len)
            .map(|idx| ((nanos >> ((idx % 8) * 8)) & 0xff) as u8)
            .collect()
    }
}

#[async_trait]
impl AuthSource for GatewayAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if let Some(expected_user) = &self.user
            && login.user() != Some(expected_user.as_str())
        {
            return Err(PgWireError::InvalidPassword(
                login.user().unwrap_or_default().to_string(),
            ));
        }
        let user = login.user().unwrap_or("postgres");
        let salt = Self::salt(10);
        let password = match GatewayAuthMethod::from_env() {
            GatewayAuthMethod::Md5 => {
                let md5_salt = salt.iter().copied().take(4).collect::<Vec<_>>();
                return Ok(Password::new(
                    Some(md5_salt.clone()),
                    hash_md5_password(user, &self.password, &md5_salt).into_bytes(),
                ));
            }
            GatewayAuthMethod::Scram => {
                scram::gen_salted_password(&self.password, &salt, scram::SCRAM_ITERATIONS)
            }
            GatewayAuthMethod::Cleartext | GatewayAuthMethod::Trust => {
                self.password.as_bytes().to_vec()
            }
        };
        Ok(Password::new(Some(salt), password))
    }
}

pub(super) struct GatewayStartupHandler {
    pub(super) server: Arc<GatewayServer>,
    pub(super) auth_method: GatewayAuthMethod,
    pub(super) auth_source: Arc<GatewayAuthSource>,
    pub(super) parameter_provider: DefaultServerParameterProvider,
    pub(super) md5_cached_password: tokio::sync::Mutex<Option<Vec<u8>>>,
    pub(super) scram_state: tokio::sync::Mutex<SASLState>,
    pub(super) scram_auth: scram::ScramAuth,
}

#[derive(Clone, Debug)]
pub(super) struct PreparedSqlStatement {
    pub(super) sql: String,
    pub(super) statement: Statement,
}

#[derive(Clone, Debug)]
pub(super) enum PreparedSqlExecution {
    Statement(Statement),
    Sql(String),
}

pub(super) struct SessionCatalog {
    pub(super) database_name: String,
    pub(super) schema_name: String,
    pub(super) search_path: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TransactionIsolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl TransactionIsolation {
    pub(super) fn from_level(level: TransactionIsolationLevel) -> Self {
        match level {
            TransactionIsolationLevel::ReadCommitted => Self::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => Self::RepeatableRead,
            TransactionIsolationLevel::Serializable => Self::Serializable,
        }
    }

    pub(super) fn as_pg_str(self) -> &'static str {
        match self {
            Self::ReadCommitted => "read committed",
            Self::RepeatableRead => "repeatable read",
            Self::Serializable => "serializable",
        }
    }
}

pub(super) struct CopyInState {
    pub(super) session_id: i32,
    pub(super) use_session_transaction: bool,
    pub(super) database_name: String,
    pub(super) schema_name: String,
    pub(super) schema: TableSchema,
    pub(super) indexes: Vec<IndexCatalog>,
    pub(super) columns: Vec<String>,
    pub(super) options: CopyInOptions,
    pub(super) buffer: Vec<u8>,
    pub(super) pending_writes: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) pending_rows: usize,
    pub(super) inserted_rows: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MySqlLock {
    pub(super) owner: i32,
    pub(super) scope: MySqlLockScope,
    pub(super) duration: MySqlLockDuration,
    pub(super) database_name: String,
    pub(super) schema_name: String,
    pub(super) table_name: String,
    pub(super) kind: MySqlLockKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MySqlLockWaiter {
    pub(super) session_id: i32,
    pub(super) priority: MySqlLockPriority,
    pub(super) locks: Vec<MySqlLock>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MySqlLockDuration {
    Statement,
    Transaction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MySqlLockPriority {
    Normal,
    High,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum MySqlLockScope {
    Database,
    Schema,
    Table,
    Index,
    View,
    Sequence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum MySqlLockKind {
    MetadataRead,
    MetadataWrite,
    Table,
    Record(String),
    Gap {
        lower: Option<String>,
        upper: Option<String>,
    },
    NextKey {
        lower: Option<String>,
        upper: Option<String>,
    },
    InsertIntention(String),
    IndexRecord {
        index_name: String,
        value: String,
    },
    IndexGap {
        index_name: String,
        lower: Option<String>,
        upper: Option<String>,
    },
    IndexNextKey {
        index_name: String,
        lower: Option<String>,
        upper: Option<String>,
    },
    IndexInsertIntention {
        index_name: String,
        value: String,
    },
}

#[derive(Clone, Debug)]
pub(super) struct MySqlLoadDataLocalSpec {
    pub(super) filename: String,
    pub(super) table_name: String,
    pub(super) columns: Vec<String>,
    pub(super) options: CopyInOptions,
}

#[derive(Clone, Debug)]
pub(super) struct CopyInOptions {
    pub(super) format: CopyInFormat,
    pub(super) delimiter: char,
    pub(super) null: String,
    pub(super) header: bool,
    pub(super) quote: char,
    pub(super) escape: char,
    pub(super) header_pending: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CopyInFormat {
    Text,
    Csv,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MySqlColumnMetadata {
    pub field: String,
    pub column_type: String,
    pub nullable: String,
    pub key: String,
    pub default_value: Option<String>,
    pub extra: String,
}

impl Default for CopyInOptions {
    fn default() -> Self {
        Self {
            format: CopyInFormat::Text,
            delimiter: '\t',
            null: "\\N".to_string(),
            header: false,
            quote: '"',
            escape: '"',
            header_pending: false,
        }
    }
}

pub(super) const METADATA_CURRENT_SCHEMA: &str = "pg_gateway.current_schema";
pub(super) const METADATA_SEARCH_PATH: &str = "search_path";
pub(super) const METADATA_TRANSACTION_ISOLATION: &str = "transaction_isolation";
pub(super) const MYSQL_AUTOCOMMIT_METADATA: &str = "mysql.autocommit";
pub(super) const MYSQL_LOCK_WAIT_TIMEOUT_METADATA: &str = "mysql.innodb_lock_wait_timeout";
pub(super) const COPY_WRITE_BATCH_MAX_ROWS: usize = 8192;
pub(super) const COPY_WRITE_BATCH_MAX_ENTRIES: usize = 32768;
pub(super) const PRIMARY_KEY_BACKFILL_WRITE_BATCH_SIZE: usize = 8192;
pub(super) const OLAP_CHUNK_ROW_TARGET: usize = 1024;

pub(super) fn copy_profile_enabled() -> bool {
    matches!(
        std::env::var("PG_GATEWAY_PROFILE_COPY").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

pub(super) fn log_copy_profile(message: impl AsRef<str>) {
    if copy_profile_enabled() {
        log::info!("[pg_gateway copy-profile] {}", message.as_ref());
    }
}

pub(super) fn parse_savepoint_identifier(raw: &str) -> PgWireResult<String> {
    let name = raw.trim().trim_end_matches(';').trim();
    if name.is_empty() || name.split_whitespace().count() != 1 {
        return Err(user_error("42601", "invalid savepoint name"));
    }
    Ok(name.trim_matches('"').to_string())
}

pub(super) fn parse_transaction_isolation(raw: &str) -> Option<TransactionIsolation> {
    let lower = raw
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase()
        .replace('_', " ");
    if lower.contains("read committed") {
        Some(TransactionIsolation::ReadCommitted)
    } else if lower.contains("repeatable read") {
        Some(TransactionIsolation::RepeatableRead)
    } else if lower.contains("serializable") {
        Some(TransactionIsolation::Serializable)
    } else {
        None
    }
}

pub(super) fn parse_mysql_autocommit_assignment(normalized: &str) -> Option<&str> {
    let lower = normalized.to_ascii_lowercase();
    for prefix in [
        "set autocommit",
        "set session autocommit",
        "set @@autocommit",
    ] {
        if lower.starts_with(prefix) {
            return normalized
                .split_once('=')
                .map(|(_, value)| value.trim().trim_matches('\'').trim_matches('"'));
        }
    }
    None
}

pub(super) fn is_serial_type(raw_data_type: &str) -> bool {
    matches!(
        raw_data_type.trim().to_ascii_uppercase().as_str(),
        "SERIAL" | "SERIAL4" | "BIGSERIAL" | "SERIAL8"
    )
}

pub(super) fn is_auto_increment_option(option: &ColumnOption) -> bool {
    match option {
        ColumnOption::DialectSpecific(tokens) => tokens
            .iter()
            .any(|token| token.to_string().eq_ignore_ascii_case("auto_increment")),
        ColumnOption::Identity(_) => true,
        _ => false,
    }
}

pub(super) fn serial_sequence_name(schema: &str, table: &str, column: &str) -> String {
    format!("{schema}.{table}_{column}_seq")
}

pub(super) fn sequence_state_key(database: &str, schema: &str, sequence: &str) -> Vec<u8> {
    format!("__catalog__/sequences/{database}/{schema}/{sequence}").into_bytes()
}

pub(super) fn encode_sequence_state(next: i64, increment: i64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&next.to_be_bytes());
    bytes.extend_from_slice(&increment.to_be_bytes());
    bytes
}

pub(super) fn decode_sequence_state(bytes: &[u8]) -> PgWireResult<(i64, i64)> {
    if bytes.len() != 16 {
        return Err(user_error("XX000", "sequence state is malformed"));
    }
    let next = i64::from_be_bytes(bytes[..8].try_into().unwrap());
    let increment = i64::from_be_bytes(bytes[8..].try_into().unwrap());
    Ok((next, increment))
}

pub(super) fn parse_nextval_query(normalized: &str) -> Option<String> {
    let trimmed = normalized.trim();
    if trimmed.len() < 7 || !trimmed[..6].eq_ignore_ascii_case("select") {
        return None;
    }
    let body = trimmed[6..].trim();
    nextval_sequence_name(body.trim())
}

pub(super) fn parse_check_expr(expr_sql: &str) -> PgWireResult<Expr> {
    parser::parse_check_expr(expr_sql)
}

pub(super) fn trim_search_path_entry(entry: &str) -> String {
    entry
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string()
}

pub(super) fn parse_search_path(search_path: &str) -> Vec<String> {
    search_path
        .split(',')
        .map(trim_search_path_entry)
        .filter(|entry| !entry.is_empty())
        .collect()
}

pub(super) fn replace_relation_after_keyword(
    sql: &str,
    keyword: &str,
    relation: &str,
    replacement: &str,
) -> String {
    let lower_sql = sql.to_ascii_lowercase();
    let lower_relation = relation.to_ascii_lowercase();
    let mut out = String::with_capacity(sql.len() + replacement.len());
    let mut cursor = 0usize;
    while let Some(relative_idx) = lower_sql[cursor..].find(keyword) {
        let keyword_idx = cursor + relative_idx;
        let after_keyword = keyword_idx + keyword.len();
        if keyword_idx > 0 && lower_sql.as_bytes()[keyword_idx - 1].is_ascii_alphanumeric() {
            out.push_str(&sql[cursor..after_keyword]);
            cursor = after_keyword;
            continue;
        }
        let mut rel_start = after_keyword;
        while rel_start < sql.len() && sql.as_bytes()[rel_start].is_ascii_whitespace() {
            rel_start += 1;
        }
        let rel_end = rel_start + relation.len();
        let matches_relation = rel_end <= sql.len()
            && lower_sql[rel_start..rel_end] == lower_relation
            && lower_sql.as_bytes().get(rel_end).is_none_or(|byte| {
                byte.is_ascii_whitespace() || matches!(byte, b',' | b';' | b')')
            });
        if matches_relation {
            out.push_str(&sql[cursor..rel_start]);
            out.push_str(replacement);
            cursor = rel_end;
        } else {
            out.push_str(&sql[cursor..after_keyword]);
            cursor = after_keyword;
        }
    }
    out.push_str(&sql[cursor..]);
    out
}

pub(super) fn strip_pg_catalog_function_qualifiers(sql: &str) -> String {
    [
        "format_type",
        "pg_get_expr",
        "pg_get_viewdef",
        "pg_get_userbyid",
        "pg_encoding_to_char",
        "array_to_string",
        "obj_description",
        "col_description",
        "version",
        "current_schema",
        "current_database",
        "current_setting",
        "has_table_privilege",
        "has_schema_privilege",
        "has_database_privilege",
        "has_column_privilege",
        "pg_table_is_visible",
        "pg_type_is_visible",
        "pg_function_is_visible",
    ]
    .into_iter()
    .fold(sql.to_string(), |acc, function| {
        acc.replace(&format!("pg_catalog.{function}"), function)
            .replace(&format!("PG_CATALOG.{function}"), function)
    })
}

pub(super) fn mysql_column_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::MySqlInt { kind, unsigned } => format!(
            "{}{}",
            kind.as_mysql_name().to_ascii_lowercase(),
            if *unsigned { " unsigned" } else { "" }
        ),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::MySqlFloat { precision } => match precision {
            Some(precision) => format!("float({precision})"),
            None => "float".to_string(),
        },
        DataType::MySqlDouble { precision } => match precision {
            Some(precision) => format!("double({precision})"),
            None => "double".to_string(),
        },
        DataType::Numeric { precision, scale } => match (precision, scale) {
            (Some(precision), Some(scale)) => format!("decimal({precision},{scale})"),
            (Some(precision), None) => format!("decimal({precision})"),
            _ => "decimal".to_string(),
        },
        DataType::Text => "text".to_string(),
        DataType::MySqlText { type_name, .. } => type_name.to_string(),
        DataType::VarChar(Some(len)) => format!("varchar({len})"),
        DataType::VarChar(None) => "varchar".to_string(),
        DataType::Char(Some(len)) => format!("char({len})"),
        DataType::Char(None) => "char".to_string(),
        DataType::Binary(Some(len)) => format!("binary({len})"),
        DataType::Binary(None) => "binary".to_string(),
        DataType::VarBinary(Some(len)) => format!("varbinary({len})"),
        DataType::VarBinary(None) => "varbinary".to_string(),
        DataType::Blob { type_name, .. } => type_name.to_string(),
        DataType::Boolean => "tinyint(1)".to_string(),
        DataType::Bit(Some(len)) => format!("bit({len})"),
        DataType::Bit(None) => "bit".to_string(),
        DataType::Year => "year".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time | DataType::TimeTz => "time".to_string(),
        DataType::MySqlTime { fsp } => match fsp {
            Some(fsp) => format!("time({fsp})"),
            None => "time".to_string(),
        },
        DataType::Interval => "text".to_string(),
        DataType::Timestamp | DataType::TimestampTz => "datetime".to_string(),
        DataType::MySqlDateTime { fsp } => match fsp {
            Some(fsp) => format!("datetime({fsp})"),
            None => "datetime".to_string(),
        },
        DataType::MySqlTimestamp { fsp } => match fsp {
            Some(fsp) => format!("timestamp({fsp})"),
            None => "timestamp".to_string(),
        },
        DataType::Uuid => "char(36)".to_string(),
        DataType::Bytea => "blob".to_string(),
        DataType::Json | DataType::Jsonb => "json".to_string(),
        DataType::Array(_) => "json".to_string(),
        DataType::Domain(name) => name.to_ascii_lowercase(),
        DataType::Enum(values) => format!("enum({})", mysql_quote_values(values)),
        DataType::Set(values) => format!("set({})", mysql_quote_values(values)),
        DataType::Geometry(name) => name.clone(),
    }
}

pub(super) fn mysql_escape_identifier(identifier: &str) -> String {
    identifier.replace('`', "``")
}

pub(super) fn mysql_quote_values(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn mysql_mode_data_type(raw_data_type: &str) -> DataType {
    let parsed = DataType::from_mysql_sql(raw_data_type);
    let upper = raw_data_type.trim().to_ascii_uppercase();
    let normalized = upper
        .split_whitespace()
        .next()
        .unwrap_or("")
        .split('(')
        .next()
        .unwrap_or("");
    match normalized {
        "TEXT" => DataType::MySqlText {
            max_len: Some(65_535),
            type_name: "text",
        },
        "TIME" => {
            if upper.contains("WITH TIME ZONE") {
                parsed
            } else {
                DataType::MySqlTime {
                    fsp: mysql_type_precision(raw_data_type),
                }
            }
        }
        "TIMESTAMP" => {
            if upper.contains("WITH TIME ZONE") {
                parsed
            } else {
                DataType::MySqlTimestamp {
                    fsp: mysql_type_precision(raw_data_type),
                }
            }
        }
        _ => parsed,
    }
}

pub(super) fn mysql_type_precision(raw_data_type: &str) -> Option<u32> {
    raw_data_type
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .and_then(|(inner, _)| inner.trim().parse::<u32>().ok())
}

pub(super) fn mysql_table_option_auto_increment(options_sql: &str) -> Option<i64> {
    let upper = options_sql.to_ascii_uppercase();
    let marker = "AUTO_INCREMENT";
    let start = upper.find(marker)? + marker.len();
    let rest = options_sql[start..].trim_start();
    let rest = rest.strip_prefix('=').unwrap_or(rest).trim_start();
    let digits = rest
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse::<i64>().ok()
    }
}

pub(super) fn mysql_auto_increment_column(schema: &TableSchema) -> Option<&ColumnSchema> {
    schema.columns.iter().find(|column| {
        (column.primary_key || column.name == schema.primary_key)
            && column
                .default
                .as_deref()
                .and_then(nextval_sequence_name)
                .is_some()
    })
}

pub(super) fn column_value_to_i64(value: &ColumnValue) -> Option<i64> {
    match value {
        ColumnValue::Int16(v) => Some(*v as i64),
        ColumnValue::Int32(v) => Some(*v as i64),
        ColumnValue::Int64(v) => Some(*v),
        ColumnValue::Numeric(v) | ColumnValue::Text(v) => v.parse::<i64>().ok(),
        _ => None,
    }
}

pub(super) fn mysql_insert_command_tag(
    affected_rows: usize,
    last_insert_id: Option<i64>,
) -> Response {
    let last_insert_id = last_insert_id.filter(|value| *value > 0).unwrap_or(0);
    command_complete(&format!("MYSQL_INSERT {affected_rows} {last_insert_id}"))
}

pub(super) fn sql_needs_system_catalogs(sql: &str) -> bool {
    let normalized = sql.to_ascii_lowercase();
    normalized.contains("pg_catalog")
        || normalized.contains("information_schema")
        || normalized.contains("pg_database")
        || normalized.contains("pg_class")
        || normalized.contains("pg_attribute")
        || normalized.contains("pg_type")
        || normalized.contains("pg_namespace")
        || normalized.contains("pg_index")
        || normalized.contains("pg_constraint")
        || normalized.contains("pg_roles")
        || normalized.contains("pg_user")
        || normalized.contains("pg_tables")
        || normalized.contains("pg_indexes")
        || normalized.contains("pg_stats")
}

pub(super) fn is_pg_catalog_relation_name(name: &str) -> bool {
    matches!(
        name,
        "pg_am"
            | "pg_attrdef"
            | "pg_attribute"
            | "pg_auth_members"
            | "pg_authid"
            | "pg_cast"
            | "pg_class"
            | "pg_collation"
            | "pg_constraint"
            | "pg_database"
            | "pg_depend"
            | "pg_description"
            | "pg_group"
            | "pg_index"
            | "pg_indexes"
            | "pg_namespace"
            | "pg_opclass"
            | "pg_opfamily"
            | "pg_operator"
            | "pg_policy"
            | "pg_proc"
            | "pg_rewrite"
            | "pg_roles"
            | "pg_sequence"
            | "pg_sequences"
            | "pg_settings"
            | "pg_shdepend"
            | "pg_shdescription"
            | "pg_statistic"
            | "pg_statistic_ext"
            | "pg_statistic_ext_data"
            | "pg_stats"
            | "pg_stats_ext"
            | "pg_stats_ext_exprs"
            | "pg_tables"
            | "pg_trigger"
            | "pg_type"
            | "pg_user"
            | "pg_views"
    )
}

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use common::types::options::Options;
use futures::StreamExt;
use pgwire::api::results::{FieldFormat, Response};
use pgwire::api::{ClientInfo, METADATA_DATABASE, PgWireConnectionState};
use pgwire::error::PgWireResult;
use pgwire::messages::ProtocolVersion;
use pgwire::messages::response::TransactionStatus;
use pgwire::messages::startup::SecretKey;
use sqlparser::ast::Statement;
use tempfile::tempdir;

use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::core::response::empty_query_response;
use crate::core::server::GatewayServer;
use crate::core::server::shared::{
    METADATA_CURRENT_SCHEMA, METADATA_SEARCH_PATH, PreparedSqlExecution, SessionCatalog,
    is_unsupported_error,
};
use crate::error::user_error;
use crate::kv_engine_store::KvEngineStore;
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;
use crate::types::QueryPlan;

pub(super) fn new_store() -> Arc<dyn KvStore> {
    Arc::new(crate::mem_store::MemoryKvStore::new())
}

pub(super) fn new_kv_engine_store() -> (Arc<dyn KvStore>, tempfile::TempDir) {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let mut opts = Options::default();
    opts.db_path = temp_dir.path().join("db");
    opts.wal_dir = temp_dir.path().join("wal");
    let store: Arc<dyn KvStore> =
        Arc::new(KvEngineStore::open(Arc::new(opts)).expect("failed to open kv_engine"));
    (store, temp_dir)
}

pub(super) fn default_session() -> SessionCatalog {
    SessionCatalog {
        database_name: DEFAULT_DATABASE_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        search_path: vec![DEFAULT_SCHEMA_NAME.to_string()],
    }
}

pub(super) fn analytics_session() -> SessionCatalog {
    SessionCatalog {
        database_name: DEFAULT_DATABASE_NAME.to_string(),
        schema_name: "analytics".to_string(),
        search_path: vec!["analytics".to_string(), DEFAULT_SCHEMA_NAME.to_string()],
    }
}

pub(super) struct TestClient {
    pub(super) metadata: HashMap<String, String>,
    pub(super) protocol_version: ProtocolVersion,
    pub(super) pid_secret_key: (i32, SecretKey),
    pub(super) state: PgWireConnectionState,
    pub(super) transaction_status: TransactionStatus,
}

impl Default for TestClient {
    fn default() -> Self {
        let mut metadata = HashMap::new();
        metadata.insert(
            METADATA_DATABASE.to_string(),
            DEFAULT_DATABASE_NAME.to_string(),
        );
        metadata.insert(
            METADATA_CURRENT_SCHEMA.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        metadata.insert(
            METADATA_SEARCH_PATH.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        Self {
            metadata,
            protocol_version: ProtocolVersion::PROTOCOL3_0,
            pid_secret_key: (1, SecretKey::I32(42)),
            state: PgWireConnectionState::ReadyForQuery,
            transaction_status: TransactionStatus::Idle,
        }
    }
}

impl ClientInfo for TestClient {
    fn socket_addr(&self) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 5432))
    }

    fn is_secure(&self) -> bool {
        false
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.protocol_version = version;
    }

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.pid_secret_key.clone()
    }

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.pid_secret_key = (pid, secret_key);
    }

    fn state(&self) -> PgWireConnectionState {
        self.state
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.state = new_state;
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.transaction_status = new_status;
    }

    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    fn sni_server_name(&self) -> Option<&str> {
        None
    }

    fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
        None
    }
}

pub(super) async fn plan_sql(
    server: &GatewayServer,
    session: &SessionCatalog,
    sql: &str,
) -> QueryPlan {
    let stmts = server.parse_sql(sql).unwrap();
    server
        .plan_statement(session, stmts.into_iter().next().unwrap())
        .await
        .unwrap()
}

pub(super) async fn exec_sql(server: &GatewayServer, session: &SessionCatalog, sql: &str) {
    let plan = plan_sql(server, session, sql).await;
    server
        .execute_plan(plan, None, FieldFormat::Text)
        .await
        .unwrap();
}

pub(super) async fn exec_sql_for_client(
    server: &GatewayServer,
    client: &mut TestClient,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    if let Some(response) = server.handle_session_command(client, sql).await? {
        return Ok(response);
    }
    if let Some(response) = server.spoofed_response(client, sql) {
        return response;
    }

    let mut statements = server.parse_sql(sql)?;
    let session = server.session_catalog(client);
    let session_id = Some(client.pid_and_secret_key().0);
    let in_transaction = server.has_active_transaction(client).await;
    if statements.len() == 1 {
        match statements[0].clone() {
            Statement::Prepare {
                name, statement, ..
            } => {
                return server
                    .prepare_sql_statement(client.pid_and_secret_key().0, &name.value, &statement)
                    .await;
            }
            Statement::Deallocate { name, .. } => {
                return server
                    .deallocate_sql_statement(client.pid_and_secret_key().0, &name.value)
                    .await;
            }
            Statement::Execute {
                name, parameters, ..
            } => {
                let name = name
                    .as_ref()
                    .map(|name| name.to_string())
                    .unwrap_or_default();
                let execution = server
                    .prepare_sql_execution(client.pid_and_secret_key().0, &name, &parameters)
                    .await?;
                statements = match execution {
                    PreparedSqlExecution::Statement(statement) => vec![statement],
                    PreparedSqlExecution::Sql(sql) => server.parse_sql(&sql)?,
                };
            }
            _ => {}
        }
    }
    let implicit_transaction = statements.len() > 1
        && !in_transaction
        && !statements
            .iter()
            .any(GatewayServer::is_transaction_control_statement);
    if implicit_transaction {
        server.begin_session_transaction(client).await?;
    }

    let mut responses = Vec::with_capacity(statements.len().max(1));
    for stmt in statements {
        let is_query_statement = matches!(stmt, Statement::Query(_));
        let ddl_implicit_commit = server.mode == GatewayMode::MySql
            && GatewayServer::is_mysql_ddl_implicit_commit_statement(&stmt);
        let locking_read = server.mode == GatewayMode::MySql
            && GatewayServer::is_mysql_locking_read_statement(&stmt);
        if ddl_implicit_commit && server.has_active_transaction(client).await {
            server.implicit_commit_active_transaction(client).await?;
        }
        if let Some(response) = server.handle_transaction_statement(client, &stmt).await? {
            responses.push(response);
            continue;
        }
        match server.plan_statement(&session, stmt).await {
            Ok(plan) => {
                if server.is_active_transaction_read_only(client).await
                    && GatewayServer::is_write_plan(&plan)
                {
                    client.set_transaction_status(TransactionStatus::Error);
                    return Err(user_error(
                        "25006",
                        "cannot execute write statement in a read-only transaction",
                    ));
                }
                let mysql_locking_plan = server.mode == GatewayMode::MySql
                    && (locking_read || GatewayServer::is_write_plan(&plan));
                let hold_mysql_locks = server.has_active_transaction(client).await;
                if mysql_locking_plan {
                    server
                        .acquire_plan_table_locks(client, &plan, locking_read)
                        .await?;
                    server
                        .enter_mysql_current_read(server.session_id(client))
                        .await;
                }
                let execute_result = server
                    .execute_plan(plan, session_id, FieldFormat::Text)
                    .await;
                if mysql_locking_plan {
                    let session_id = server.session_id(client);
                    server.exit_mysql_current_read(session_id).await;
                    if hold_mysql_locks {
                        server.release_session_statement_locks(session_id).await;
                    } else {
                        server.release_session_table_locks(session_id).await;
                    }
                }
                responses.push(execute_result?);
                if ddl_implicit_commit {
                    server.restart_mysql_autocommit_transaction(client).await?;
                }
            }
            Err(e) if is_unsupported_error(&e) => {
                if implicit_transaction {
                    client.set_transaction_status(TransactionStatus::Error);
                    let _ = server.rollback_session_transaction(client).await;
                    return Err(user_error(
                        "0A000",
                        "slow-path statements are not supported inside pg_gateway implicit multi-statement transactions yet",
                    ));
                }
                if server.has_active_transaction(client).await && !is_query_statement {
                    client.set_transaction_status(TransactionStatus::Error);
                    return Err(user_error(
                        "0A000",
                        "slow-path statements are not supported inside pg_gateway transactions yet",
                    ));
                }
                return server.execute_via_datafusion(sql, &session).await;
            }
            Err(e) => {
                if server.has_active_transaction(client).await || implicit_transaction {
                    client.set_transaction_status(TransactionStatus::Error);
                }
                if implicit_transaction {
                    let _ = server.rollback_session_transaction(client).await;
                }
                return Err(e);
            }
        }
    }
    if implicit_transaction {
        if let Err(e) = server.commit_session_transaction(client).await {
            let _ = server.rollback_session_transaction(client).await;
            return Err(e);
        }
    }
    if responses.is_empty() {
        responses.push(empty_query_response());
    }
    Ok(responses)
}

pub(super) fn response_field_names(response: &Response) -> Vec<String> {
    match response {
        Response::Query(query) => query
            .row_schema()
            .iter()
            .map(|field| field.name().to_string())
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) async fn response_text_rows(mut response: Response) -> Vec<Vec<Option<String>>> {
    let Response::Query(query) = &mut response else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    while let Some(row) = query.data_rows().next().await {
        let row = row.unwrap();
        let data = row.data.as_ref();
        let mut offset = 0usize;
        let mut values = Vec::new();
        for _ in 0..row.field_count {
            let len = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;
            if len < 0 {
                values.push(None);
                continue;
            }
            let len = len as usize;
            values.push(Some(
                String::from_utf8(data[offset..offset + len].to_vec()).unwrap(),
            ));
            offset += len;
        }
        rows.push(values);
    }
    rows
}

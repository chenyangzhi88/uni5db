use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::PgWireConnectionState;
use pgwire::api::auth::sasl::{SASLState, scram};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, ServerParameterProvider, StartupHandler,
};
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldFormat, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use pgwire::messages::response::{ReadyForQuery, TransactionStatus};
use pgwire::messages::startup::{
    Authentication, BackendKeyData, ParameterStatus, PasswordMessageFamily,
};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::GatewayServer;
use super::shared::{
    GatewayAuthMethod, GatewayAuthSource, GatewayStartupHandler, METADATA_CURRENT_SCHEMA,
    METADATA_SEARCH_PATH, log_copy_profile, parse_search_path,
};
use crate::error::{unsupported, user_error};
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

impl GatewayServer {
    pub(crate) async fn post_startup<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        self.catalog.ensure_bootstrap_for_mode(self.mode).await?;
        let requested_database = client.metadata().get(METADATA_DATABASE).cloned();
        let user = client.metadata().get(METADATA_USER).cloned();
        let database_name = self
            .resolve_startup_database(requested_database, user)
            .await?;
        client
            .metadata_mut()
            .insert(METADATA_DATABASE.to_string(), database_name.clone());
        if client.metadata().get(METADATA_CURRENT_SCHEMA).is_none() {
            let schema_name = client
                .metadata()
                .get(METADATA_SEARCH_PATH)
                .and_then(|value| parse_search_path(value).into_iter().next())
                .unwrap_or_else(|| self.default_schema_name().to_string());
            self.set_session_schema(client, &schema_name);
        }
        Ok(())
    }
}

impl GatewayStartupHandler {
    fn new(server: Arc<GatewayServer>) -> Self {
        let auth_source = Arc::new(GatewayAuthSource::from_env());
        let auth_method = GatewayAuthMethod::from_env();
        let scram_auth = scram::ScramAuth::new(auth_source.clone());
        Self {
            server,
            auth_method,
            auth_source,
            parameter_provider: DefaultServerParameterProvider::default(),
            md5_cached_password: tokio::sync::Mutex::new(None),
            scram_state: tokio::sync::Mutex::new(SASLState::Initial),
            scram_auth,
        }
    }

    async fn finish_startup<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.server.post_startup(client).await?;
        client
            .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
            .await?;
        if let Some(parameters) = self.parameter_provider.server_parameters(client) {
            for (key, value) in parameters {
                client
                    .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                        key, value,
                    )))
                    .await?;
            }
        }
        let (pid, secret_key) = client.pid_and_secret_key();
        client
            .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
                pid, secret_key,
            )))
            .await?;
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }
}

#[async_trait]
impl StartupHandler for GatewayStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                pgwire::api::auth::protocol_negotiation(client, startup).await?;
                pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
                match self.auth_method {
                    GatewayAuthMethod::Trust => self.finish_startup(client).await?,
                    GatewayAuthMethod::Cleartext => {
                        self.server.post_startup(client).await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                        client
                            .send(PgWireBackendMessage::Authentication(
                                Authentication::CleartextPassword,
                            ))
                            .await?;
                    }
                    GatewayAuthMethod::Md5 => {
                        self.server.post_startup(client).await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                        let login = LoginInfo::from_client_info(client);
                        let password = self.auth_source.get_password(&login).await?;
                        *self.md5_cached_password.lock().await = Some(password.password().to_vec());
                        client
                            .send(PgWireBackendMessage::Authentication(
                                Authentication::MD5Password(
                                    password.salt().unwrap_or(&[0, 0, 0, 0]).to_vec(),
                                ),
                            ))
                            .await?;
                    }
                    GatewayAuthMethod::Scram => {
                        self.server.post_startup(client).await?;
                        client.set_state(PgWireConnectionState::AuthenticationInProgress);
                        *self.scram_state.lock().await = SASLState::Initial;
                        client
                            .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                                vec!["SCRAM-SHA-256".to_string()],
                            )))
                            .await?;
                    }
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => match self.auth_method {
                GatewayAuthMethod::Cleartext => {
                    let password = message.into_password()?;
                    if password.password == self.auth_source.password {
                        self.finish_startup(client).await?;
                    } else {
                        return Err(PgWireError::InvalidPassword(
                            client
                                .metadata()
                                .get(METADATA_USER)
                                .cloned()
                                .unwrap_or_default(),
                        ));
                    }
                }
                GatewayAuthMethod::Md5 => {
                    let password = message.into_password()?;
                    let cached = self.md5_cached_password.lock().await;
                    if cached.as_deref() == Some(password.password.as_bytes()) {
                        self.finish_startup(client).await?;
                    } else {
                        return Err(PgWireError::InvalidPassword(
                            client
                                .metadata()
                                .get(METADATA_USER)
                                .cloned()
                                .unwrap_or_default(),
                        ));
                    }
                }
                GatewayAuthMethod::Scram => {
                    let mut state = self.scram_state.lock().await;
                    let family = if matches!(*state, SASLState::Initial) {
                        let initial = message.into_sasl_initial_response()?;
                        if initial.auth_method != "SCRAM-SHA-256" {
                            return Err(PgWireError::UnsupportedSASLAuthMethod(
                                initial.auth_method,
                            ));
                        }
                        *state = SASLState::ScramClientFirstReceived;
                        PasswordMessageFamily::SASLInitialResponse(initial)
                    } else {
                        PasswordMessageFamily::SASLResponse(message.into_sasl_response()?)
                    };
                    let (response, next_state) = self
                        .scram_auth
                        .process_scram_message(client, family, &state)
                        .await?;
                    client
                        .send(PgWireBackendMessage::Authentication(response))
                        .await?;
                    *state = next_state;
                    if matches!(*state, SASLState::Finished) {
                        self.finish_startup(client).await?;
                    }
                }
                GatewayAuthMethod::Trust => {}
            },
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for GatewayServer {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.run_query(_client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for GatewayServer {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = Self::bind_portal_sql(portal)?;
        let responses = self
            .run_query_with_format(client, &sql, Self::portal_result_format(portal))
            .await?;
        if responses.len() != 1 {
            return Err(unsupported(
                "extended query protocol supports one statement per prepared query",
            ));
        }
        Ok(responses.into_iter().next().unwrap())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let defaults = Self::placeholder_defaults(&statement.parameter_types);
        let sql = Self::replace_pg_parameter_placeholders(&statement.statement, &defaults);
        let fields = self
            .describe_sql_fields(client, &sql, FieldFormat::Text)
            .await?;
        let parameters = statement
            .parameter_types
            .iter()
            .map(|pg_type| pg_type.clone().unwrap_or(Type::UNKNOWN))
            .collect();
        Ok(DescribeStatementResponse::new(parameters, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo
            + pgwire::api::ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: pgwire::api::store::PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = Self::bind_portal_sql(portal)?;
        let fields = self
            .describe_sql_fields(client, &sql, FieldFormat::Text)
            .await?;
        Ok(DescribePortalResponse::new(fields))
    }
}

pub struct GatewayFactory {
    handler: Arc<GatewayServer>,
}

impl GatewayFactory {
    pub fn new(store: Arc<dyn KvStore>, mode: GatewayMode) -> Self {
        Self {
            handler: Arc::new(GatewayServer::with_mode(store, mode)),
        }
    }
}

impl PgWireServerHandlers for GatewayFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::new(GatewayStartupHandler::new(self.handler.clone()))
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.handler.clone()
    }
}

#[async_trait]
impl CancelHandler for GatewayServer {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        self.mark_cancelled(cancel_request.pid).await;
    }
}

#[async_trait]
impl CopyHandler for GatewayServer {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = self.session_id(client);
        let mut state = self
            .active_copy_in
            .lock()
            .await
            .remove(&session_id)
            .ok_or_else(|| user_error("XX000", "COPY IN state not initialized"))?;
        state.buffer.extend_from_slice(copy_data.data.as_ref());
        let result = self.flush_copy_buffer(session_id, &mut state, false).await;
        self.active_copy_in.lock().await.insert(session_id, state);
        result
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let copy_done_started_at = Instant::now();
        let session_id = self.session_id(client);
        let mut state = self
            .active_copy_in
            .lock()
            .await
            .remove(&session_id)
            .ok_or_else(|| user_error("XX000", "COPY IN state not initialized"))?;
        log_copy_profile(format!(
            "on_copy_done start session={} rows={} buffered_bytes={} pending_writes={} in_txn={}",
            session_id,
            state.inserted_rows,
            state.buffer.len(),
            state.pending_writes.len(),
            state.use_session_transaction
        ));
        let flush_buffer_started_at = Instant::now();
        self.flush_copy_buffer(session_id, &mut state, true).await?;
        log_copy_profile(format!(
            "on_copy_done flush_copy_buffer session={} elapsed_ms={} rows={}",
            session_id,
            flush_buffer_started_at.elapsed().as_millis(),
            state.inserted_rows
        ));
        let flush_writes_started_at = Instant::now();
        self.flush_copy_writes(&mut state).await?;
        log_copy_profile(format!(
            "on_copy_done flush_copy_writes session={} elapsed_ms={} rows={} olap_refresh=deferred",
            session_id,
            flush_writes_started_at.elapsed().as_millis(),
            state.inserted_rows
        ));
        if !state.use_session_transaction {
            let sync_started_at = Instant::now();
            self.store
                .sync_wal()
                .await
                .map_err(|e| user_error("XX000", e))?;
            log_copy_profile(format!(
                "on_copy_done sync_wal session={} elapsed_ms={} rows={}",
                session_id,
                sync_started_at.elapsed().as_millis(),
                state.inserted_rows
            ));
        }
        let send_started_at = Instant::now();
        client
            .send(PgWireBackendMessage::CommandComplete(
                Tag::new("COPY").with_rows(state.inserted_rows).into(),
            ))
            .await?;
        log_copy_profile(format!(
            "on_copy_done command_complete session={} rows={} send_elapsed_ms={} total_elapsed_ms={}",
            session_id,
            state.inserted_rows,
            send_started_at.elapsed().as_millis(),
            copy_done_started_at.elapsed().as_millis()
        ));
        Ok(())
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session_id = self.session_id(client);
        self.active_copy_in.lock().await.remove(&session_id);
        PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN terminated by client: {}", fail.message),
        )))
    }
}

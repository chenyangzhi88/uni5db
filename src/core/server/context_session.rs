use std::sync::atomic::Ordering;
use std::time::Instant;

use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, METADATA_DATABASE};
use pgwire::error::PgWireResult;
use pgwire::messages::response::TransactionStatus;

use super::GatewayServer;
use super::shared::{
    METADATA_CURRENT_SCHEMA, METADATA_SEARCH_PATH, METADATA_TRANSACTION_ISOLATION,
    MYSQL_AUTOCOMMIT_METADATA, MySqlLockDuration, SessionCatalog, TransactionIsolation,
    log_copy_profile, parse_search_path, parse_transaction_isolation,
};
use crate::catalog::DEFAULT_DATABASE_NAME;
use crate::core::response::command_complete;
use crate::error::user_error;
use crate::mode::GatewayMode;

impl GatewayServer {
    pub(super) fn session_catalog<C>(&self, client: &C) -> SessionCatalog
    where
        C: ClientInfo,
    {
        let database_name = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| DEFAULT_DATABASE_NAME.to_string());
        let schema_name = client
            .metadata()
            .get(METADATA_CURRENT_SCHEMA)
            .cloned()
            .or_else(|| {
                client
                    .metadata()
                    .get(METADATA_SEARCH_PATH)
                    .and_then(|value| parse_search_path(value).into_iter().next())
            })
            .unwrap_or_else(|| self.default_schema_name().to_string());
        let search_path = client
            .metadata()
            .get(METADATA_SEARCH_PATH)
            .map(|value| parse_search_path(value))
            .filter(|schemas| !schemas.is_empty())
            .unwrap_or_else(|| vec![schema_name.clone()]);
        SessionCatalog {
            database_name,
            schema_name,
            search_path,
        }
    }

    pub(super) fn set_session_schema<C>(&self, client: &mut C, schema_name: &str)
    where
        C: ClientInfo,
    {
        client
            .metadata_mut()
            .insert(METADATA_CURRENT_SCHEMA.to_string(), schema_name.to_string());
        client
            .metadata_mut()
            .insert(METADATA_SEARCH_PATH.to_string(), schema_name.to_string());
    }

    pub(super) async fn resolve_startup_database(
        &self,
        requested: Option<String>,
        user: Option<String>,
    ) -> PgWireResult<String> {
        let requested = requested.filter(|value| !value.trim().is_empty());

        let Some(requested) = requested else {
            return Ok(DEFAULT_DATABASE_NAME.to_string());
        };

        if self.catalog.get_database(&requested).await?.is_some() {
            return Ok(requested);
        }

        if requested == DEFAULT_DATABASE_NAME || user.as_deref() == Some(requested.as_str()) {
            return Ok(DEFAULT_DATABASE_NAME.to_string());
        }

        Err(user_error(
            "3D000",
            format!("database '{requested}' does not exist"),
        ))
    }

    pub(super) fn session_id<C>(&self, client: &C) -> i32
    where
        C: ClientInfo,
    {
        client.pid_and_secret_key().0
    }

    pub(super) async fn mark_cancelled(&self, session_id: i32) {
        self.cancelled_sessions.lock().await.insert(session_id);
    }

    pub(super) async fn check_cancelled(&self, session_id: i32) -> PgWireResult<()> {
        if self.cancelled_sessions.lock().await.remove(&session_id) {
            return Err(user_error(
                "57014",
                "canceling statement due to user request",
            ));
        }
        Ok(())
    }

    pub(super) async fn begin_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        self.begin_session_transaction_with_options(client, None, false)
            .await
    }

    pub(super) fn mysql_autocommit_disabled<C>(&self, client: &C) -> bool
    where
        C: ClientInfo,
    {
        self.mode == GatewayMode::MySql
            && client
                .metadata()
                .get(MYSQL_AUTOCOMMIT_METADATA)
                .is_some_and(|value| value == "0" || value.eq_ignore_ascii_case("off"))
    }

    pub(super) async fn restart_mysql_autocommit_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        if self.mysql_autocommit_disabled(client) {
            self.begin_session_transaction(client).await?;
        }
        Ok(())
    }

    pub(super) async fn begin_session_transaction_with_options<C>(
        &self,
        client: &mut C,
        isolation: Option<TransactionIsolation>,
        read_only: bool,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let mut txns = self.active_transactions.lock().await;
        if txns.contains_key(&session_id) {
            if self.mode == GatewayMode::MySql {
                let txn = txns.remove(&session_id);
                drop(txns);
                self.release_session_table_locks(session_id).await;
                self.active_transaction_snapshots
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_transaction_isolations
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_transaction_read_only
                    .lock()
                    .await
                    .remove(&session_id);
                self.active_mysql_current_reads
                    .lock()
                    .await
                    .remove(&session_id);
                if let Some(txn) = txn {
                    txn.commit().await.map_err(|e| user_error("40001", e))?;
                }
                client.set_transaction_status(TransactionStatus::Idle);
                txns = self.active_transactions.lock().await;
            } else {
                if read_only {
                    self.active_transaction_read_only
                        .lock()
                        .await
                        .insert(session_id);
                }
                client.set_transaction_status(TransactionStatus::Transaction);
                return Ok(vec![command_complete("BEGIN")]);
            }
        }
        if txns.contains_key(&session_id) {
            if read_only {
                self.active_transaction_read_only
                    .lock()
                    .await
                    .insert(session_id);
            }
            client.set_transaction_status(TransactionStatus::Transaction);
            return Ok(vec![command_complete("BEGIN")]);
        }
        let txn = self
            .store
            .begin_transaction()
            .await
            .map_err(|e| user_error("XX000", e))?;
        let isolation = isolation
            .or_else(|| {
                client
                    .metadata()
                    .get(METADATA_TRANSACTION_ISOLATION)
                    .and_then(|value| parse_transaction_isolation(value))
            })
            .unwrap_or_else(|| self.default_transaction_isolation());
        txns.insert(session_id, txn);
        self.active_transaction_snapshots
            .lock()
            .await
            .insert(session_id, self.mvcc_clock.load(Ordering::SeqCst));
        self.active_transaction_isolations
            .lock()
            .await
            .insert(session_id, isolation);
        let mut read_only_txns = self.active_transaction_read_only.lock().await;
        if read_only {
            read_only_txns.insert(session_id);
        } else {
            read_only_txns.remove(&session_id);
        }
        client.metadata_mut().insert(
            METADATA_TRANSACTION_ISOLATION.to_string(),
            isolation.as_pg_str().to_string(),
        );
        client.set_transaction_status(TransactionStatus::Transaction);
        Ok(vec![command_complete("BEGIN")])
    }

    pub(super) async fn commit_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            let started_at = Instant::now();
            log_copy_profile(format!("session_commit start session={session_id}"));
            match txn.commit().await {
                Ok(()) => {
                    log_copy_profile(format!(
                        "session_commit done session={} elapsed_ms={} success=true",
                        session_id,
                        started_at.elapsed().as_millis()
                    ));
                    client.set_transaction_status(TransactionStatus::Idle);
                    self.restart_mysql_autocommit_transaction(client).await?;
                    Ok(vec![command_complete("COMMIT")])
                }
                Err(error) => {
                    log_copy_profile(format!(
                        "session_commit done session={} elapsed_ms={} success=false error={}",
                        session_id,
                        started_at.elapsed().as_millis(),
                        error
                    ));
                    client.set_transaction_status(TransactionStatus::Error);
                    Err(user_error("40001", error))
                }
            }
        } else {
            client.set_transaction_status(TransactionStatus::Idle);
            self.restart_mysql_autocommit_transaction(client).await?;
            Ok(vec![command_complete("COMMIT")])
        }
    }

    pub(super) async fn rollback_session_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.rollback().await.map_err(|e| user_error("XX000", e))?;
        }
        client.set_transaction_status(TransactionStatus::Idle);
        self.restart_mysql_autocommit_transaction(client).await?;
        Ok(vec![command_complete("ROLLBACK")])
    }

    pub(crate) async fn rollback_session_by_id(&self, session_id: i32) -> PgWireResult<()> {
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.rollback().await.map_err(|e| user_error("XX000", e))?;
        }
        Ok(())
    }

    pub(super) async fn implicit_commit_active_transaction<C>(
        &self,
        client: &mut C,
    ) -> PgWireResult<()>
    where
        C: ClientInfo,
    {
        let session_id = self.session_id(client);
        let txn = self.active_transactions.lock().await.remove(&session_id);
        self.active_transaction_snapshots
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_isolations
            .lock()
            .await
            .remove(&session_id);
        self.active_transaction_read_only
            .lock()
            .await
            .remove(&session_id);
        self.active_mysql_current_reads
            .lock()
            .await
            .remove(&session_id);
        self.release_session_table_locks(session_id).await;
        if let Some(txn) = txn {
            txn.commit().await.map_err(|e| user_error("40001", e))?;
        }
        client.set_transaction_status(TransactionStatus::Idle);
        Ok(())
    }

    pub(super) async fn is_active_transaction_read_only<C>(&self, client: &C) -> bool
    where
        C: ClientInfo,
    {
        self.active_transaction_read_only
            .lock()
            .await
            .contains(&self.session_id(client))
    }

    pub(super) async fn release_session_table_locks(&self, session_id: i32) {
        self.active_mysql_locks
            .lock()
            .await
            .retain(|lock| lock.owner != session_id);
        self.active_mysql_lock_queue
            .lock()
            .await
            .retain(|waiter| waiter.session_id != session_id);
        self.active_mysql_lock_waits
            .lock()
            .await
            .remove(&session_id);
        self.mysql_lock_notify.notify_waiters();
    }

    pub(super) async fn release_session_statement_locks(&self, session_id: i32) {
        self.active_mysql_locks.lock().await.retain(|lock| {
            lock.owner != session_id || lock.duration != MySqlLockDuration::Statement
        });
        self.mysql_lock_notify.notify_waiters();
    }
}

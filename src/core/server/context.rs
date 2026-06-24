use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use datafusion::catalog::CatalogProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use pgwire::error::PgWireResult;

use super::GatewayServer;
use super::shared::TransactionIsolation;
use crate::catalog::CatalogStore;
use crate::datafusion_bridge::{
    KvAggregateOptimizerRule, KvQueryPlanner, KvTopNOptimizerRule, build_user_catalog_provider,
    register_catalog_tables_with_options_for_mode, register_pg_catalog_functions,
};
use crate::dialect;
use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

impl GatewayServer {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self::with_mode(store, GatewayMode::Postgres)
    }

    pub fn with_mode(store: Arc<dyn KvStore>, mode: GatewayMode) -> Self {
        Self {
            catalog: CatalogStore::new(store.clone()),
            mode,
            store,
            datafusion_user_catalogs: tokio::sync::RwLock::new(HashMap::new()),
            active_transactions: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_snapshots: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_isolations: tokio::sync::Mutex::new(HashMap::new()),
            active_transaction_read_only: tokio::sync::Mutex::new(HashSet::new()),
            active_mysql_current_reads: tokio::sync::Mutex::new(HashSet::new()),
            active_mysql_locks: tokio::sync::Mutex::new(Vec::new()),
            active_mysql_lock_queue: tokio::sync::Mutex::new(VecDeque::new()),
            active_mysql_lock_waits: tokio::sync::Mutex::new(HashMap::new()),
            mysql_lock_notify: tokio::sync::Notify::new(),
            active_sql_prepared: tokio::sync::Mutex::new(HashMap::new()),
            active_copy_in: tokio::sync::Mutex::new(HashMap::new()),
            cancelled_sessions: tokio::sync::Mutex::new(HashSet::new()),
            mvcc_clock: AtomicU64::new(1),
        }
    }

    pub(super) fn default_transaction_isolation(&self) -> TransactionIsolation {
        TransactionIsolation::from_level(dialect::profile(self.mode).transaction.default_isolation)
    }

    pub(super) fn default_schema_name(&self) -> &'static str {
        dialect::profile(self.mode).default_schema
    }

    pub(super) fn new_datafusion_context(database_name: &str, schema_name: &str) -> SessionContext {
        let config =
            SessionConfig::new().with_default_catalog_and_schema(database_name, schema_name);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_optimizer_rule(Arc::new(KvAggregateOptimizerRule))
            .with_optimizer_rule(Arc::new(KvTopNOptimizerRule))
            .with_query_planner(Arc::new(KvQueryPlanner))
            .build();
        SessionContext::new_with_state(state)
    }

    pub(super) async fn invalidate_datafusion_catalog(&self, database_name: &str) {
        self.datafusion_user_catalogs
            .write()
            .await
            .remove(database_name);
    }

    pub(super) async fn cached_user_catalog_provider(
        &self,
        database_name: &str,
    ) -> PgWireResult<Arc<dyn CatalogProvider>> {
        if let Some(provider) = self
            .datafusion_user_catalogs
            .read()
            .await
            .get(database_name)
            .cloned()
        {
            return Ok(provider);
        }

        let provider =
            build_user_catalog_provider(self.store.clone(), &self.catalog, database_name).await?;
        self.datafusion_user_catalogs
            .write()
            .await
            .insert(database_name.to_string(), provider.clone());
        Ok(provider)
    }

    pub(super) async fn register_datafusion_catalogs(
        &self,
        ctx: &SessionContext,
        database_name: &str,
        include_system_catalogs: bool,
    ) -> PgWireResult<()> {
        if include_system_catalogs {
            register_catalog_tables_with_options_for_mode(
                ctx,
                self.store.clone(),
                &self.catalog,
                database_name,
                true,
                self.mode,
            )
            .await
        } else {
            register_pg_catalog_functions(ctx);
            ctx.register_catalog(
                database_name,
                self.cached_user_catalog_provider(database_name).await?,
            );
            Ok(())
        }
    }
}

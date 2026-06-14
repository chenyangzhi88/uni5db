pub use crate::core::server::GatewayServer;
pub use crate::datafusion_bridge::{KvAggregateOptimizerRule, KvQueryPlanner, KvTopNOptimizerRule};
pub use crate::types::QueryPlan;

use std::sync::Arc;

use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

pub struct ExecutionEngine {
    server: GatewayServer,
}

impl ExecutionEngine {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self::with_mode(store, GatewayMode::Postgres)
    }

    pub fn with_mode(store: Arc<dyn KvStore>, mode: GatewayMode) -> Self {
        Self {
            server: GatewayServer::with_mode(store, mode),
        }
    }

    pub fn into_server(self) -> GatewayServer {
        self.server
    }
}

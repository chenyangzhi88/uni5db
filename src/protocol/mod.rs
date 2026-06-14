use std::sync::Arc;

use crate::mem_store::KvStore;
use crate::mode::GatewayMode;

pub mod mysql;
pub mod postgres;

pub async fn serve(
    mode: GatewayMode,
    store: Arc<dyn KvStore>,
    listen_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        GatewayMode::Postgres => postgres::serve(store, listen_addr).await,
        GatewayMode::MySql => mysql::serve(store, listen_addr).await,
    }
}

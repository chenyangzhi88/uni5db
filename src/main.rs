use std::sync::Arc;

use common::types::options::{FileConfig, Options};
use pg_gateway::catalog::{CatalogStore, DEFAULT_DATABASE_NAME};
use pg_gateway::kv_engine_store::KvEngineStore;
use pg_gateway::mem_store::{KvStore, MemoryKvStore};
use pg_gateway::mode::GatewayMode;
use pg_gateway::protocol;

fn load_gateway_options() -> Options {
    let config_path =
        std::env::var("PG_GATEWAY_CONFIG").unwrap_or_else(|_| "config/unidb.toml".to_string());
    let mut options = FileConfig::load_from_path(std::path::Path::new(&config_path))
        .and_then(FileConfig::into_options)
        .unwrap_or_else(|error| {
            log::warn!(
                "failed to load pg_gateway config from {}: {}; falling back to defaults",
                config_path,
                error
            );
            Options::default()
        });

    if let Ok(db_path) = std::env::var("PG_GATEWAY_DB_PATH") {
        options.db_path = db_path.into();
    }
    if let Ok(wal_dir) = std::env::var("PG_GATEWAY_WAL_DIR") {
        options.wal_dir = wal_dir.into();
    } else if std::env::var("PG_GATEWAY_DB_PATH").is_ok() {
        options.wal_dir = format!("{}/wal", options.db_path.display()).into();
    }
    if let Ok(engine_id) = std::env::var("PG_GATEWAY_ENGINE_ID") {
        if let Ok(parsed) = engine_id.parse() {
            options.engine_id = parsed;
        }
    }
    options
}

fn env_flag_enabled(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

async fn rewrite_fast_rows_if_requested(
    store: Arc<dyn KvStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !env_flag_enabled("PG_GATEWAY_REWRITE_FAST_ROWS") {
        return Ok(());
    }
    let batch_size = std::env::var("PG_GATEWAY_REWRITE_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000);
    let catalog = CatalogStore::new(store.clone());
    let tables = catalog.list_tables(DEFAULT_DATABASE_NAME).await?;
    for table in tables {
        let started_at = std::time::Instant::now();
        let (scanned, rewritten) = store
            .rewrite_table_rows_to_fast_format(table.schema.clone(), batch_size)
            .await
            .map_err(|e| format!("rewrite fast rows failed for {}: {e}", table.table_name))?;
        log::info!(
            "pg_gateway fast-row rewrite table={} scanned={} rewritten={} elapsed_ms={}",
            table.table_name,
            scanned,
            rewritten,
            started_at.elapsed().as_millis()
        );
    }
    let started_at = std::time::Instant::now();
    store
        .compact_storage()
        .await
        .map_err(|e| format!("compact storage after fast-row rewrite failed: {e}"))?;
    log::info!(
        "pg_gateway fast-row rewrite compaction elapsed_ms={}",
        started_at.elapsed().as_millis()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::logging::init_logging()?;
    let store: Arc<dyn KvStore> = match std::env::var("PG_GATEWAY_BACKEND").ok().as_deref() {
        Some("memory") => Arc::new(MemoryKvStore::new()),
        _ => {
            let options = load_gateway_options();
            Arc::new(KvEngineStore::open(Arc::new(options))?)
        }
    };
    rewrite_fast_rows_if_requested(store.clone()).await?;
    let mode = GatewayMode::from_env()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let listen_addr =
        std::env::var("PG_GATEWAY_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:55432".to_string());
    protocol::serve(mode, store, listen_addr).await
}

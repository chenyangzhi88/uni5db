use std::sync::Arc;
use std::time::Instant;

use common::types::options::Options;
use pg_gateway::catalog::{CatalogStore, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use pg_gateway::kv_engine_store::KvEngineStore;
use pg_gateway::mem_store::KvStore;
use pg_gateway::mode::GatewayMode;
use pg_gateway::storage_layout;
use pg_gateway::types::{ColumnSchema, ColumnValue, DataType, RowMap, TableSchema};

const TARGET_ROWS: i64 = 10_000_000;
const DEFAULT_BATCH_ROWS: i64 = 100_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = common::logging::init_logging();
    let db_path =
        std::env::var("PG_GATEWAY_DB_PATH").unwrap_or_else(|_| "/nvme_data_0/mysql".to_string());
    let wal_dir = std::env::var("PG_GATEWAY_WAL_DIR").unwrap_or_else(|_| format!("{db_path}/wal"));
    let gateway_mode = std::env::var("SEED_GATEWAY_MODE")
        .ok()
        .as_deref()
        .map(|value| {
            value
                .parse::<GatewayMode>()
                .map_err(|_| format!("SEED_GATEWAY_MODE must be postgres or mysql; got '{value}'"))
        })
        .transpose()?
        .unwrap_or(GatewayMode::MySql);
    let batch_rows = std::env::var("SEED_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BATCH_ROWS);

    let mut options = Options::default();
    options.db_path = db_path.into();
    options.wal_dir = wal_dir.into();

    let raw_store = Arc::new(KvEngineStore::open(Arc::new(options))?);
    let store: Arc<dyn KvStore> = raw_store.clone();
    if std::env::var("SEED_COMPACT_ONLY").as_deref() == Ok("1") {
        let started = Instant::now();
        store.compact_storage().await?;
        store.sync_wal().await?;
        println!(
            "compacted storage elapsed_s={}",
            started.elapsed().as_secs()
        );
        return Ok(());
    }

    let catalog = CatalogStore::new(store.clone());
    let table = ensure_orders_table(&catalog, gateway_mode).await?;
    let schema = table.schema;
    if std::env::var("SEED_SCAN_ONLY").as_deref() == Ok("1") {
        let key_only = matches!(
            std::env::var("SEED_SCAN_PROJECTION").as_deref(),
            Ok("key") | Ok("key-only")
        );
        let range = storage_layout::row_range(schema.table_id, schema.table_epoch, None);
        let projection = if key_only { "key-only" } else { "key-value" };
        let method = std::env::var("SEED_SCAN_METHOD").unwrap_or_else(|_| "next_batch".to_string());
        let scan_method = method.clone();
        let started = Instant::now();
        let rows = tokio::task::spawn_blocking(move || {
            raw_store.scan_range_count_for_experiment(
                range.start,
                range.end,
                key_only,
                &scan_method,
            )
        })
        .await??;
        let elapsed = started.elapsed();
        println!(
            "scan method={} projection={} rows={} elapsed_ms={} rows_per_s={:.0}",
            method,
            projection,
            rows,
            elapsed.as_millis(),
            rows as f64 / elapsed.as_secs_f64()
        );
        return Ok(());
    }
    let started = Instant::now();

    let mut next_id = 1_i64;
    while next_id <= TARGET_ROWS {
        let end_id = (next_id + batch_rows - 1).min(TARGET_ROWS);
        let mut entries = Vec::with_capacity((end_id - next_id + 1) as usize);
        for id in next_id..=end_id {
            let mut row = RowMap::new();
            row.insert("id".to_string(), ColumnValue::Int64(id));
            row.insert(
                "customer_id".to_string(),
                ColumnValue::Int64(((id - 1) % 100_000) + 1),
            );
            row.insert(
                "product_id".to_string(),
                ColumnValue::Int64(((id - 1) % 1_000) + 1),
            );
            row.insert(
                "quantity".to_string(),
                ColumnValue::Int32((((id - 1) % 5) + 1) as i32),
            );
            row.insert(
                "amount_cents".to_string(),
                ColumnValue::Int64(1_000 + ((id - 1) % 50_000)),
            );
            row.insert("status".to_string(), ColumnValue::Text("paid".to_string()));
            row.insert(
                "created_at".to_string(),
                ColumnValue::Timestamp("2026-06-13 00:00:00".to_string()),
            );
            let pk = ColumnValue::Int64(id);
            entries.push((
                storage_layout::row_key(schema.table_id, schema.table_epoch, &pk),
                storage_layout::encode_row_record(&schema, &row),
            ));
        }
        store.put_batch(entries).await?;
        println!(
            "seeded rows {}..={} elapsed_s={}",
            next_id,
            end_id,
            started.elapsed().as_secs()
        );
        next_id = end_id + 1;
    }

    let stats = storage_layout::TableStats {
        row_count: TARGET_ROWS as u64,
        zones: Vec::new(),
    };
    store
        .put(
            &storage_layout::stats_key(schema.table_id, schema.table_epoch, None),
            &storage_layout::encode_table_stats(&stats),
        )
        .await?;
    store.sync_wal().await?;
    if std::env::var("SEED_COMPACT_AFTER").as_deref() == Ok("1") {
        store.compact_storage().await?;
        store.sync_wal().await?;
    }
    println!(
        "seeded orders rows={} elapsed_s={}",
        TARGET_ROWS,
        started.elapsed().as_secs()
    );
    Ok(())
}

async fn ensure_orders_table(
    catalog: &CatalogStore,
    gateway_mode: GatewayMode,
) -> Result<pg_gateway::catalog::TableCatalog, Box<dyn std::error::Error>> {
    catalog.ensure_bootstrap_for_mode(gateway_mode).await?;
    if let Some(table) = catalog
        .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "orders")
        .await?
    {
        return Ok(table);
    }

    let table_id = catalog.allocate_table_id().await?;
    let schema = TableSchema {
        table_name: "orders".to_string(),
        table_id,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".to_string(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            column(1, "id", DataType::Int64, true, false),
            column(2, "customer_id", DataType::Int64, false, true),
            column(3, "product_id", DataType::Int64, false, true),
            column(4, "quantity", DataType::Int32, false, true),
            column(5, "amount_cents", DataType::Int64, false, true),
            column(6, "status", DataType::Text, false, true),
            column(7, "created_at", DataType::Timestamp, false, true),
        ],
    };
    catalog
        .store_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &schema)
        .await?;
    Ok(catalog
        .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "orders")
        .await?
        .ok_or("orders table was not persisted")?)
}

fn column(
    column_id: u32,
    name: &str,
    data_type: DataType,
    primary_key: bool,
    nullable: bool,
) -> ColumnSchema {
    ColumnSchema {
        column_id,
        name: name.to_string(),
        data_type,
        primary_key,
        nullable,
        default: None,
        on_update: None,
        character_set: None,
        collation: None,
    }
}

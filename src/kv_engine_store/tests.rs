use std::sync::Arc;

use common::types::options::Options;

use super::profile::{KvEngineStore, next_prefix};
use crate::mem_store::{
    KvAggregateOp, KvAggregateScan, KvCompareOp, KvPredicate, KvScanProjection, KvStore,
};
use crate::storage_layout;
use crate::types::{ColumnSchema, ColumnValue, DataType, RowMap, TableSchema};
use tempfile::tempdir;

fn open_test_store() -> (tempfile::TempDir, KvEngineStore) {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let mut options = Options::default();
    options.db_path = temp_dir.path().join("db");
    options.wal_dir = temp_dir.path().join("wal");
    let store = KvEngineStore::open(Arc::new(options)).expect("failed to open kv store");
    (temp_dir, store)
}

fn numeric_group_schema() -> TableSchema {
    TableSchema {
        table_name: "orders".into(),
        table_id: 99,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 1,
                name: "id".into(),
                data_type: DataType::Int32,
                primary_key: true,
                nullable: false,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 2,
                name: "product_id".into(),
                data_type: DataType::Int32,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 3,
                name: "amount".into(),
                data_type: DataType::Int32,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
        ],
    }
}

fn numeric_group_row(id: i32, product_id: i32, amount: i32) -> RowMap {
    let mut row = RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(id));
    row.insert("product_id".into(), ColumnValue::Int32(product_id));
    row.insert("amount".into(), ColumnValue::Int32(amount));
    row
}

#[tokio::test]
async fn aggregate_scan_groups_fixed_numeric_columns() {
    let (_temp_dir, store) = open_test_store();
    let schema = numeric_group_schema();

    for row in [
        numeric_group_row(1, 10, 5),
        numeric_group_row(2, 10, 15),
        numeric_group_row(3, 20, 7),
        numeric_group_row(4, 20, 25),
        numeric_group_row(5, 30, 50),
    ] {
        let pk = row.get(&schema.primary_key).expect("missing pk");
        let key = storage_layout::row_key(schema.table_id, schema.table_epoch, pk);
        let value = storage_layout::encode_row_record(&schema, &row);
        store.put(&key, &value).await.expect("failed to put row");
    }

    let output = store
        .aggregate_scan(KvAggregateScan {
            schema: schema.clone(),
            range_start: storage_layout::row_prefix(schema.table_id, schema.table_epoch),
            range_end: next_prefix(&storage_layout::row_prefix(
                schema.table_id,
                schema.table_epoch,
            )),
            scan_prefix: Some(storage_layout::row_prefix(
                schema.table_id,
                schema.table_epoch,
            )),
            required_indices: vec![1, 2],
            projection: KvScanProjection::KeyValue,
            filters: vec![KvPredicate::ColumnCompare {
                column_idx: 2,
                op: KvCompareOp::GtEq,
                value: ColumnValue::Int32(10),
            }],
            group_indices: vec![1],
            aggregates: vec![
                KvAggregateOp::CountStar,
                KvAggregateOp::SumColumn { column_idx: 2 },
                KvAggregateOp::AvgColumn { column_idx: 2 },
                KvAggregateOp::MinColumn { column_idx: 2 },
                KvAggregateOp::MaxColumn { column_idx: 2 },
            ],
        })
        .await
        .expect("aggregate scan failed");

    fn aggregate_values_for_group(rows: &[Vec<ColumnValue>], product_id: i32) -> Vec<ColumnValue> {
        rows.iter()
            .find(|row| row.first() == Some(&ColumnValue::Int32(product_id)))
            .map(|row| row[1..].to_vec())
            .unwrap_or_else(|| panic!("missing group {product_id}"))
    }

    assert_eq!(
        aggregate_values_for_group(&output, 10),
        vec![
            ColumnValue::Int64(1),
            ColumnValue::Int64(15),
            ColumnValue::Float64(15.0),
            ColumnValue::Int32(15),
            ColumnValue::Int32(15),
        ]
    );
    assert_eq!(
        aggregate_values_for_group(&output, 20),
        vec![
            ColumnValue::Int64(1),
            ColumnValue::Int64(25),
            ColumnValue::Float64(25.0),
            ColumnValue::Int32(25),
            ColumnValue::Int32(25),
        ]
    );
    assert_eq!(
        aggregate_values_for_group(&output, 30),
        vec![
            ColumnValue::Int64(1),
            ColumnValue::Int64(50),
            ColumnValue::Float64(50.0),
            ColumnValue::Int32(50),
            ColumnValue::Int32(50),
        ]
    );
    assert_eq!(output.len(), 3);
}

#[tokio::test]
async fn transaction_range_scan_merges_pending_puts_deletes_limit_and_reverse() {
    let (_temp_dir, store) = open_test_store();
    store.put(b"a", b"1").await.expect("put a");
    store.put(b"b", b"2").await.expect("put b");
    store.put(b"d", b"4").await.expect("put d");

    let txn = store.begin_transaction().await.expect("begin transaction");
    txn.delete(b"b").await.expect("delete b");
    txn.put(b"c", b"3").await.expect("put c");

    let rows = txn
        .scan_range(b"b", Some(b"e"), None, false)
        .await
        .expect("scan merged range");
    assert_eq!(
        rows,
        vec![
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec())
        ]
    );

    let reverse_limited = txn
        .scan_range(b"a", Some(b"e"), Some(2), true)
        .await
        .expect("scan reverse limited range");
    assert_eq!(
        reverse_limited,
        vec![
            (b"d".to_vec(), b"4".to_vec()),
            (b"c".to_vec(), b"3".to_vec())
        ]
    );

    txn.commit().await.expect("commit");
    assert_eq!(store.get(b"b").await.expect("get b"), None);
    assert_eq!(store.get(b"c").await.expect("get c"), Some(b"3".to_vec()));
}

#[tokio::test]
async fn transaction_rollback_discards_pending_range_changes() {
    let (_temp_dir, store) = open_test_store();
    store.put(b"a", b"1").await.expect("put a");
    store.put(b"b", b"2").await.expect("put b");

    let txn = store.begin_transaction().await.expect("begin transaction");
    txn.delete(b"a").await.expect("delete a");
    txn.put(b"c", b"3").await.expect("put c");
    assert_eq!(
        txn.scan_prefix(b"").await.expect("scan in txn"),
        vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec())
        ]
    );

    txn.rollback().await.expect("rollback");
    assert_eq!(store.get(b"a").await.expect("get a"), Some(b"1".to_vec()));
    assert_eq!(store.get(b"c").await.expect("get c"), None);
}

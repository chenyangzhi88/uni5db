use std::sync::Arc;

use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::codec::{cell_key, encode_cell_value, row_marker_key, schema_key};
use crate::mem_store::KvStore;
use crate::types::{ColumnSchema, ColumnValue, DataType, RowMap, TableSchema};

pub(super) fn test_schema() -> TableSchema {
    TableSchema {
        table_name: "t".into(),
        table_id: 1,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 0,
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
                column_id: 0,
                name: "name".into(),
                data_type: DataType::Text,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 0,
                name: "active".into(),
                data_type: DataType::Boolean,
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

pub(super) fn olap_schema() -> TableSchema {
    TableSchema {
        table_name: "sales".into(),
        table_id: 7,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 0,
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
                column_id: 0,
                name: "region".into(),
                data_type: DataType::Text,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 0,
                name: "amount".into(),
                data_type: DataType::Int32,
                primary_key: false,
                nullable: true,
                default: None,

                on_update: None,

                character_set: None,

                collation: None,
            },
            ColumnSchema {
                column_id: 0,
                name: "active".into(),
                data_type: DataType::Boolean,
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

pub(super) fn event_schema() -> TableSchema {
    TableSchema {
        table_name: "events".into(),
        table_id: 8,
        schema_version: 1,
        table_epoch: 1,
        primary_key: "id".into(),
        check_constraints: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        columns: vec![
            ColumnSchema {
                column_id: 0,
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
                column_id: 0,
                name: "event_ts".into(),
                data_type: DataType::Text,
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

pub(super) async fn register_schema_and_rows(
    store: Arc<dyn KvStore>,
    schema: &TableSchema,
    rows: Vec<RowMap>,
) {
    let schema_json = serde_json::json!({
        "table_name": schema.table_name,
        "table_id": schema.table_id,
        "primary_key": schema.primary_key,
        "columns": schema.columns.iter().map(|c| serde_json::json!({
            "name": c.name,
            "data_type": c.data_type.to_str(),
            "primary_key": c.primary_key,
            "nullable": c.nullable,
        })).collect::<Vec<_>>(),
    });
    let key = schema_key(&schema.table_name);
    store
        .put(key.as_bytes(), &serde_json::to_vec(&schema_json).unwrap())
        .await
        .unwrap();

    for row in rows {
        let pk = row.get(&schema.primary_key).unwrap().clone();
        store
            .put(
                &row_marker_key(
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema.table_name,
                    &pk,
                ),
                &[1],
            )
            .await
            .unwrap();
        for column in &schema.columns {
            let key = cell_key(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                &schema.table_name,
                &column.name,
                &pk,
            );
            let value = row.get(&column.name).cloned().unwrap_or(ColumnValue::Null);
            store.put(&key, &encode_cell_value(&value)).await.unwrap();
        }
    }
}

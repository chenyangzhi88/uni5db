use super::*;

#[test]
fn to_arrow_schema_maps_types() {
    let schema = test_schema();
    let arrow = to_arrow_schema(&schema);
    assert_eq!(arrow.fields().len(), 3);
    assert_eq!(*arrow.field(0).data_type(), ArrowDataType::Int32);
    assert_eq!(*arrow.field(1).data_type(), ArrowDataType::Utf8);
    assert_eq!(*arrow.field(2).data_type(), ArrowDataType::Boolean);
    assert!(!arrow.field(0).is_nullable());
    assert!(arrow.field(1).is_nullable());
}

#[test]
fn column_builder_int32() {
    let mut b = ColumnBuilder::new(&DataType::Int32);
    b.push(&ColumnValue::Int32(1));
    b.push(&ColumnValue::Null);
    b.push(&ColumnValue::Int32(3));
    let arr = b.finish();
    let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(a.value(0), 1);
    assert!(a.is_null(1));
    assert_eq!(a.value(2), 3);
}

#[test]
fn column_builder_text() {
    let mut b = ColumnBuilder::new(&DataType::Text);
    b.push(&ColumnValue::Text("hello".into()));
    b.push(&ColumnValue::Null);
    let arr = b.finish();
    let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(a.value(0), "hello");
    assert!(a.is_null(1));
}

#[test]
fn arrow_to_pgwire_empty_batches() {
    let resp = arrow_to_pgwire_response(vec![]).unwrap();
    assert!(matches!(resp, Response::Query(_)));
}

#[test]
fn arrow_to_pgwire_with_data() {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap();
    let resp = arrow_to_pgwire_response(vec![batch]).unwrap();
    assert!(matches!(resp, Response::Query(_)));
}

#[test]
fn arrow_timestamp_formats_like_postgres_text() {
    let array: ArrayRef = Arc::new(arrow::array::TimestampSecondArray::from(vec![Some(
        1_735_787_045,
    )]));
    let rendered = arrow_array_value_to_string(&array, 0);
    assert!(rendered.contains("2025"));
    assert!(rendered.contains(' '));
    assert!(!rendered.contains('T'));
}

#[test]
fn arrow_list_formats_like_postgres_array_text() {
    let value_builder = arrow::array::StringBuilder::new();
    let mut builder = arrow::array::ListBuilder::new(value_builder);
    builder.values().append_value("alpha");
    builder.values().append_value("beta");
    builder.append(true);
    let array: ArrayRef = Arc::new(builder.finish());
    let rendered = arrow_array_value_to_string(&array, 0);
    assert_eq!(rendered, "{alpha,beta}");
}

#[test]
fn arrow_type_mapping() {
    assert_eq!(arrow_type_to_pg(&ArrowDataType::Int32), Type::INT4);
    assert_eq!(arrow_type_to_pg(&ArrowDataType::Int64), Type::INT8);
    assert_eq!(arrow_type_to_pg(&ArrowDataType::Boolean), Type::BOOL);
    assert_eq!(arrow_type_to_pg(&ArrowDataType::Utf8), Type::TEXT);
    assert_eq!(arrow_type_to_pg(&ArrowDataType::Float64), Type::FLOAT8);
}

#[tokio::test]
async fn kv_table_provider_scan_empty() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = test_schema();
    let provider = KvTableProvider::new(
        DEFAULT_DATABASE_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        schema,
        store,
    );
    let batches = provider.load_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}

#[tokio::test]
async fn kv_table_provider_scan_with_data() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = test_schema();
    let mut row1 = crate::types::RowMap::new();
    row1.insert("id".into(), ColumnValue::Int32(1));
    row1.insert("name".into(), ColumnValue::Text("alice".into()));
    row1.insert("active".into(), ColumnValue::Boolean(true));

    let mut row2 = crate::types::RowMap::new();
    row2.insert("id".into(), ColumnValue::Int32(2));
    row2.insert("name".into(), ColumnValue::Text("bob".into()));
    row2.insert("active".into(), ColumnValue::Boolean(false));

    register_schema_and_rows(store.clone(), &schema, vec![row1, row2]).await;

    let provider = KvTableProvider::new(
        DEFAULT_DATABASE_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        schema,
        store,
    );
    let batches = provider.load_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);

    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
}

#[tokio::test]
async fn kv_table_provider_treats_missing_cells_as_nulls() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = test_schema();

    let mut row1 = crate::types::RowMap::new();
    row1.insert("id".into(), ColumnValue::Int32(1));
    row1.insert("name".into(), ColumnValue::Text("alive".into()));
    row1.insert("active".into(), ColumnValue::Boolean(true));
    register_schema_and_rows(store.clone(), &schema, vec![row1]).await;

    let marker = row_marker_key(
        DEFAULT_DATABASE_NAME,
        DEFAULT_SCHEMA_NAME,
        &schema.table_name,
        &ColumnValue::Int32(2),
    );
    store.put(&marker, &[1]).await.unwrap();
    store
        .put(
            &cell_key(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                &schema.table_name,
                "id",
                &ColumnValue::Int32(2),
            ),
            &encode_cell_value(&ColumnValue::Int32(2)),
        )
        .await
        .unwrap();

    let provider = KvTableProvider::new(
        DEFAULT_DATABASE_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        schema,
        store,
    );
    let batches = provider.load_batches().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alive");
    assert!(names.is_null(1));
}

#[tokio::test]
async fn register_and_query_via_datafusion() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = test_schema();

    // Store the schema in KV
    let schema_json = serde_json::json!({
        "table_name": "t",
        "table_id": 1,
        "primary_key": "id",
        "columns": [
            {"name": "id", "data_type": "INT4", "primary_key": true, "nullable": false},
            {"name": "name", "data_type": "TEXT", "primary_key": false, "nullable": true},
            {"name": "active", "data_type": "BOOL", "primary_key": false, "nullable": true},
        ]
    });
    let key = schema_key("t");
    store
        .put(key.as_bytes(), &serde_json::to_vec(&schema_json).unwrap())
        .await
        .unwrap();

    // Insert a row
    let mut row = crate::types::RowMap::new();
    row.insert("id".into(), ColumnValue::Int32(1));
    row.insert("name".into(), ColumnValue::Text("alice".into()));
    row.insert("active".into(), ColumnValue::Boolean(true));
    register_schema_and_rows(store.clone(), &schema, vec![row]).await;

    // Register tables and run query
    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, |table_name, bytes| {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|e| user_error("XX000", format!("{e}")))?;
        let table_id = value.get("table_id").and_then(|v| v.as_u64()).unwrap() as u32;
        let primary_key = value
            .get("primary_key")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let columns = value
            .get("columns")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(crate::types::parse_column_schema)
            .collect::<PgWireResult<Vec<_>>>()
            .unwrap();
        Ok(TableSchema {
            table_name: table_name.to_string(),
            table_id,
            schema_version: 1,
            table_epoch: 1,
            primary_key,
            check_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            columns,
        })
    })
    .await
    .unwrap();

    let df = ctx
        .sql("SELECT id, name FROM t WHERE id = 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
}

#[tokio::test]
async fn datafusion_aggregate_avg_min_max() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "east", 10, true),
        (2, "east", 25, false),
        (3, "west", 8, true),
        (4, "west", 12, true),
        (5, "north", 20, false),
    ] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("region".into(), ColumnValue::Text(region.into()));
        row.insert("amount".into(), ColumnValue::Int32(amount));
        row.insert("active".into(), ColumnValue::Boolean(active));
        rows.push(row);
    }
    register_schema_and_rows(store.clone(), &schema, rows).await;

    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, decode_table_schema)
        .await
        .unwrap();

    let df = ctx
            .sql("SELECT avg(amount) AS avg_amount, min(amount) AS min_amount, max(amount) AS max_amount FROM sales")
            .await
            .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let avg = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let min = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let max = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert!((avg.value(0) - 15.0).abs() < f64::EPSILON);
    assert_eq!(min.value(0), 8);
    assert_eq!(max.value(0), 25);
}

#[tokio::test]
async fn datafusion_expression_filter_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "east", 10, true),
        (2, "east", 25, false),
        (3, "west", 8, true),
        (4, "west", 12, true),
        (5, "north", 20, false),
    ] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("region".into(), ColumnValue::Text(region.into()));
        row.insert("amount".into(), ColumnValue::Int32(amount));
        row.insert("active".into(), ColumnValue::Boolean(active));
        rows.push(row);
    }
    register_schema_and_rows(store.clone(), &schema, rows).await;

    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, decode_table_schema)
        .await
        .unwrap();

    let df = ctx
        .sql(
            "SELECT id, region, amount FROM sales \
                 WHERE amount >= 20 AND active = false \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let regions = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amounts = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(ids.value(0), 2);
    assert_eq!(regions.value(0), "east");
    assert_eq!(amounts.value(0), 25);
    assert_eq!(ids.value(1), 5);
    assert_eq!(regions.value(1), "north");
    assert_eq!(amounts.value(1), 20);
}

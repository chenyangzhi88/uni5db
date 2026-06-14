use super::*;

#[tokio::test]
async fn datafusion_between_and_not_between_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "beta", 20, false),
        (3, "gamma", 30, true),
        (4, "delta", 40, false),
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
            "SELECT id FROM sales \
                 WHERE amount BETWEEN 15 AND 30 OR amount NOT BETWEEN 15 AND 35 \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3, 4]);
}

#[tokio::test]
async fn datafusion_union_all_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "beta", 20, false),
        (3, "gamma", 30, true),
        (4, "delta", 40, false),
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
            "SELECT id FROM sales WHERE amount < 15 \
                 UNION ALL \
                 SELECT id FROM sales WHERE amount >= 30 \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3, 4]);
}

#[tokio::test]
async fn datafusion_scalar_subquery_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "beta", 20, false),
        (3, "gamma", 30, true),
        (4, "delta", 40, false),
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
            "SELECT id, amount FROM sales \
                 WHERE amount > (SELECT avg(amount) FROM sales) \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let amounts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[3, 4]);
    assert_eq!(amounts.values(), &[30, 40]);
}

#[tokio::test]
async fn datafusion_join_group_order_subquery_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let customers = TableSchema {
        table_name: "customers".into(),
        table_id: 9,
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
        ],
    };
    let orders = TableSchema {
        table_name: "orders".into(),
        table_id: 10,
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
                name: "customer_id".into(),
                data_type: DataType::Int32,
                primary_key: false,
                nullable: false,
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
        ],
    };

    let mut customer_rows = Vec::new();
    for (id, region) in [(1, "east"), (2, "west")] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("region".into(), ColumnValue::Text(region.into()));
        customer_rows.push(row);
    }
    register_schema_and_rows(store.clone(), &customers, customer_rows).await;

    let mut order_rows = Vec::new();
    for (id, customer_id, amount) in [(1, 1, 10), (2, 1, 50), (3, 2, 30), (4, 2, 40)] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("customer_id".into(), ColumnValue::Int32(customer_id));
        row.insert("amount".into(), ColumnValue::Int32(amount));
        order_rows.push(row);
    }
    register_schema_and_rows(store.clone(), &orders, order_rows).await;

    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, decode_table_schema)
        .await
        .unwrap();

    let df = ctx
        .sql(
            "SELECT c.region, sum(o.amount) AS total \
                 FROM customers c \
                 JOIN orders o ON c.id = o.customer_id \
                 WHERE o.amount > (SELECT avg(amount) FROM orders) \
                 GROUP BY c.region \
                 ORDER BY total DESC",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);

    let regions = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(regions.value(0), "east");
    assert_eq!(totals.value(0), 50);
    assert_eq!(regions.value(1), "west");
    assert_eq!(totals.value(1), 40);
}

#[tokio::test]
async fn datafusion_datetime_cast_and_extract_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = event_schema();

    let mut rows = Vec::new();
    for (id, event_ts) in [(1, "2025-01-02T03:04:05"), (2, "2025-07-15T12:30:00")] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("event_ts".into(), ColumnValue::Text(event_ts.into()));
        rows.push(row);
    }
    register_schema_and_rows(store.clone(), &schema, rows).await;

    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, decode_table_schema)
        .await
        .unwrap();

    let df = ctx
        .sql(
            "SELECT id, \
                        EXTRACT(YEAR FROM CAST(event_ts AS TIMESTAMP)) AS year_part, \
                        EXTRACT(MONTH FROM CAST(event_ts AS TIMESTAMP)) AS month_part \
                 FROM events \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(arrow_array_value_to_string(batches[0].column(1), 0), "2025");
    assert_eq!(arrow_array_value_to_string(batches[0].column(1), 1), "2025");
    assert_eq!(arrow_array_value_to_string(batches[0].column(2), 0), "1");
    assert_eq!(arrow_array_value_to_string(batches[0].column(2), 1), "7");
}

#[tokio::test]
async fn datafusion_array_agg_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "alps", 30, false),
        (3, "beta", 20, false),
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
        .sql("SELECT array_agg(region) AS regions FROM sales")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let arrays = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let values = arrays.value(0);
    let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "alpha");
    assert_eq!(strings.value(1), "alps");
    assert_eq!(strings.value(2), "beta");
}

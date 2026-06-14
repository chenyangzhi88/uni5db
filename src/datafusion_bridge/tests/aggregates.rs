use super::*;

#[tokio::test]
async fn datafusion_boolean_filter_pushdown_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "east", Some(10), Some(true)),
        (2, "east", Some(25), Some(false)),
        (3, "west", Some(30), Some(true)),
        (4, "north", None, None),
    ] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert("region".into(), ColumnValue::Text(region.into()));
        row.insert(
            "amount".into(),
            amount.map(ColumnValue::Int32).unwrap_or(ColumnValue::Null),
        );
        row.insert(
            "active".into(),
            active
                .map(ColumnValue::Boolean)
                .unwrap_or(ColumnValue::Null),
        );
        rows.push(row);
    }
    register_schema_and_rows(store.clone(), &schema, rows).await;

    let ctx = SessionContext::new();
    register_all_tables(&ctx, store, decode_table_schema)
        .await
        .unwrap();

    let df = ctx
        .sql("SELECT id FROM sales WHERE active ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3]);

    let df = ctx
        .sql("SELECT id FROM sales WHERE NOT active ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2]);

    let df = ctx
        .sql("SELECT id FROM sales WHERE active OR amount IS NULL ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3, 4]);

    let df = ctx
        .sql("SELECT id FROM sales WHERE NOT (amount = 10) ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2, 3]);
}

#[tokio::test]
async fn datafusion_group_by_having_query() {
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
            "SELECT region, sum(amount) AS total \
                 FROM sales \
                 GROUP BY region \
                 HAVING sum(amount) >= 20 \
                 ORDER BY region",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

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
    assert_eq!(totals.value(0), 35);
    assert_eq!(regions.value(1), "north");
    assert_eq!(totals.value(1), 20);
    assert_eq!(regions.value(2), "west");
    assert_eq!(totals.value(2), 20);
}

#[tokio::test]
async fn datafusion_count_distinct_query() {
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
        .sql("SELECT count(DISTINCT region) AS distinct_regions FROM sales")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let distinct_regions = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(distinct_regions.value(0), 3);
}

#[tokio::test]
async fn datafusion_group_by_multiple_columns_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "east", 10, true),
        (2, "east", 25, false),
        (3, "west", 8, true),
        (4, "west", 12, true),
        (5, "west", 20, false),
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
            "SELECT region, active, count(*) AS cnt, sum(amount) AS total \
                 FROM sales \
                 GROUP BY region, active \
                 ORDER BY region, active",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 4);

    let regions = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actives = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let counts = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(regions.value(0), "east");
    assert!(!actives.value(0));
    assert_eq!(counts.value(0), 1);
    assert_eq!(totals.value(0), 25);

    assert_eq!(regions.value(1), "east");
    assert!(actives.value(1));
    assert_eq!(counts.value(1), 1);
    assert_eq!(totals.value(1), 10);

    assert_eq!(regions.value(2), "west");
    assert!(!actives.value(2));
    assert_eq!(counts.value(2), 1);
    assert_eq!(totals.value(2), 20);

    assert_eq!(regions.value(3), "west");
    assert!(actives.value(3));
    assert_eq!(counts.value(3), 2);
    assert_eq!(totals.value(3), 20);
}

#[tokio::test]
async fn datafusion_combined_where_group_by_having_order_limit_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "east", 10, true),
        (2, "east", 30, true),
        (3, "west", 20, true),
        (4, "west", 40, true),
        (5, "north", 15, true),
        (6, "north", 5, false),
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
            "SELECT region, sum(amount) AS total \
                 FROM sales \
                 WHERE active = true \
                 GROUP BY region \
                 HAVING sum(amount) >= 30 \
                 ORDER BY total DESC \
                 LIMIT 2",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
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

    assert_eq!(regions.value(0), "west");
    assert_eq!(totals.value(0), 60);
    assert_eq!(regions.value(1), "east");
    assert_eq!(totals.value(1), 40);
}

#[tokio::test]
async fn datafusion_null_aggregate_and_count_distinct_semantics() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, Some("east"), Some(10), true),
        (2, Some("east"), None, false),
        (3, Some("west"), Some(20), true),
        (4, None, Some(30), false),
        (5, None, None, true),
    ] {
        let mut row = RowMap::new();
        row.insert("id".into(), ColumnValue::Int32(id));
        row.insert(
            "region".into(),
            region
                .map(|value| ColumnValue::Text(value.into()))
                .unwrap_or(ColumnValue::Null),
        );
        row.insert(
            "amount".into(),
            amount.map(ColumnValue::Int32).unwrap_or(ColumnValue::Null),
        );
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
            "SELECT count(amount) AS count_amount, avg(amount) AS avg_amount, \
                 min(amount) AS min_amount, max(amount) AS max_amount, \
                 count(DISTINCT region) AS distinct_regions \
                 FROM sales",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let count_amount = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg_amount = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let min_amount = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let max_amount = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let distinct_regions = batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(count_amount.value(0), 3);
    assert!((avg_amount.value(0) - 20.0).abs() < f64::EPSILON);
    assert_eq!(min_amount.value(0), 10);
    assert_eq!(max_amount.value(0), 30);
    assert_eq!(distinct_regions.value(0), 2);
}

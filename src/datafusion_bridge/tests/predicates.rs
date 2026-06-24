use super::{
    Arc, ColumnValue, Int32Array, Int64Array, RowMap, SessionContext, StringArray,
    decode_table_schema, olap_schema, register_all_tables, register_schema_and_rows,
};

#[tokio::test]
async fn datafusion_order_by_nulls_first_last() {
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
        .sql("SELECT id, amount FROM sales ORDER BY amount NULLS FIRST, id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2, 5, 1, 3, 4]);

    let df = ctx
        .sql("SELECT id, amount FROM sales ORDER BY amount NULLS LAST, id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3, 4, 2, 5]);
}

#[tokio::test]
async fn datafusion_is_null_and_is_not_null_queries() {
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
        .sql("SELECT id FROM sales WHERE region IS NULL ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[4, 5]);

    let df = ctx
        .sql("SELECT id FROM sales WHERE amount IS NOT NULL ORDER BY id")
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
async fn datafusion_text_like_in_case_when_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "alps", 30, false),
        (3, "beta", 20, false),
        (4, "gamma", 40, true),
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
            "SELECT id, region, CASE WHEN amount >= 20 THEN 'high' ELSE 'low' END AS bucket \
                 FROM sales \
                 WHERE region LIKE 'al%' OR region IN ('beta') \
                 ORDER BY id",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);

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
    let buckets = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(regions.value(0), "alpha");
    assert_eq!(buckets.value(0), "low");
    assert_eq!(regions.value(1), "alps");
    assert_eq!(buckets.value(1), "high");
    assert_eq!(regions.value(2), "beta");
    assert_eq!(buckets.value(2), "high");
}

#[tokio::test]
async fn datafusion_not_in_and_not_like_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "alps", 30, false),
        (3, "beta", 20, false),
        (4, "gamma", 40, true),
        (5, "delta", 15, false),
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
            "SELECT id, region FROM sales \
                 WHERE region NOT LIKE 'al%' AND region NOT IN ('gamma') \
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
    let regions = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.values(), &[3, 5]);
    assert_eq!(regions.value(0), "beta");
    assert_eq!(regions.value(1), "delta");
}

#[tokio::test]
async fn datafusion_nested_case_when_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "beta", 25, false),
        (3, "gamma", 35, true),
        (4, "delta", 50, false),
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
                "SELECT id, \
                        CASE \
                          WHEN amount >= 40 THEN 'xlarge' \
                          WHEN amount >= 20 THEN CASE WHEN active THEN 'medium-active' ELSE 'medium-idle' END \
                          ELSE 'small' \
                        END AS bucket \
                 FROM sales \
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
    let buckets = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.values(), &[1, 2, 3, 4]);
    assert_eq!(buckets.value(0), "small");
    assert_eq!(buckets.value(1), "medium-idle");
    assert_eq!(buckets.value(2), "medium-active");
    assert_eq!(buckets.value(3), "xlarge");
}

#[tokio::test]
async fn datafusion_coalesce_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, Some("alpha"), Some(10), true),
        (2, None, Some(25), false),
        (3, Some("gamma"), None, true),
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
                "SELECT id, COALESCE(region, 'unknown') AS region_name, COALESCE(amount, 0) AS safe_amount \
                 FROM sales \
                 ORDER BY id",
            )
            .await
            .unwrap();
    let batches = df.collect().await.unwrap();
    let regions = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amounts = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(regions.value(0), "alpha");
    assert_eq!(regions.value(1), "unknown");
    assert_eq!(regions.value(2), "gamma");
    assert_eq!(amounts.value(0), 10);
    assert_eq!(amounts.value(1), 25);
    assert_eq!(amounts.value(2), 0);
}

#[tokio::test]
async fn datafusion_string_and_arithmetic_aggregate_query() {
    let store = Arc::new(crate::mem_store::MemoryKvStore::new());
    let schema = olap_schema();

    let mut rows = Vec::new();
    for (id, region, amount, active) in [
        (1, "alpha", 10, true),
        (2, "alps", 30, false),
        (3, "beta", 20, false),
        (4, "gamma", 40, true),
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
            "SELECT upper(substr(region, 1, 1)) AS initial, \
                        sum(amount * 2 + 1) AS adjusted_total \
                 FROM sales \
                 GROUP BY upper(substr(region, 1, 1)) \
                 ORDER BY initial",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let initials = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(initials.value(0), "A");
    assert_eq!(totals.value(0), 82);
    assert_eq!(initials.value(1), "B");
    assert_eq!(totals.value(1), 41);
    assert_eq!(initials.value(2), "G");
    assert_eq!(totals.value(2), 81);
}

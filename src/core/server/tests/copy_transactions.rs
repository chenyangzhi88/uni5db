use super::*;

#[tokio::test]
async fn copy_text_terminator_is_not_treated_as_data_row() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE history (tid INT, bid INT, aid INT, delta INT)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "history")
        .await
        .unwrap()
        .unwrap();
    let mut state = CopyInState {
        session_id: 0,
        use_session_transaction: false,
        database_name: DEFAULT_DATABASE_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        schema: schema.clone(),
        indexes: Vec::new(),
        columns: vec!["tid".into(), "bid".into(), "aid".into(), "delta".into()],
        options: CopyInOptions::default(),
        buffer: Vec::new(),
        pending_writes: Vec::new(),
        pending_rows: 0,
        inserted_rows: 0,
    };

    server.apply_copy_line(0, &mut state, br"\.").await.unwrap();
    assert_eq!(state.inserted_rows, 0);
    assert!(state.pending_writes.is_empty());
}

#[tokio::test]
async fn copy_csv_header_and_null_options_are_applied() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE copy_csv (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "copy_csv")
        .await
        .unwrap()
        .unwrap();
    let mut state = CopyInState {
        session_id: 0,
        use_session_transaction: false,
        database_name: DEFAULT_DATABASE_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        schema: schema.clone(),
        indexes: Vec::new(),
        columns: vec!["id".into(), "name".into()],
        options: CopyInOptions {
            format: CopyInFormat::Csv,
            delimiter: ',',
            null: "".to_string(),
            header: true,
            quote: '"',
            escape: '"',
            header_pending: true,
        },
        buffer: Vec::new(),
        pending_writes: Vec::new(),
        pending_rows: 0,
        inserted_rows: 0,
    };

    server
        .apply_copy_line(0, &mut state, br"id,name")
        .await
        .unwrap();
    server
        .apply_copy_line(0, &mut state, br#"1,"alice, a.""#)
        .await
        .unwrap();
    assert_eq!(state.inserted_rows, 1);
}

#[tokio::test]
async fn memory_backend_transaction_commit_makes_write_visible_to_other_session() {
    let server = GatewayServer::new(new_store());
    let mut writer = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
    )
    .await;

    server
        .handle_session_command(&mut writer, "BEGIN")
        .await
        .unwrap();
    let session = server.session_catalog(&writer);
    let plan = plan_sql(
        &server,
        &session,
        "INSERT INTO t (id, val) VALUES (1, 'uncommitted')",
    )
    .await;
    server
        .execute_plan(plan, Some(writer.pid_and_secret_key().0), FieldFormat::Text)
        .await
        .unwrap();

    let before = plan_sql(&server, &default_session(), "SELECT * FROM t").await;
    if let QueryPlan::SelectRows { schema, access, .. } = before {
        let rows = match access {
            ReadAccess::PointLookup { key } => server
                .read_visible_row(None, &schema, &key)
                .await
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            ReadAccess::PrimaryKeyInLookup { keys } => server
                .read_visible_rows_by_pk_in_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &keys,
                )
                .await
                .unwrap(),
            ReadAccess::PrefixScan { filter } => server
                .scan_visible_rows(None, &schema, filter.as_ref())
                .await
                .unwrap(),
            ReadAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => server
                .scan_visible_row_entries_by_pk_range_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    filter.as_ref(),
                    None,
                    0,
                )
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row)| row)
                .collect(),
            ReadAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
                ..
            } => server
                .scan_index_rows_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &index_name,
                    &key,
                    filter.as_ref(),
                )
                .await
                .unwrap(),
            ReadAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
                ..
            } => server
                .scan_index_row_entries_by_range_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &index_name,
                    lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    filter.as_ref(),
                )
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row)| row)
                .collect(),
        };
        assert!(rows.is_empty());
    } else {
        panic!("expected select rows");
    }

    server
        .handle_session_command(&mut writer, "COMMIT")
        .await
        .unwrap();

    let after = plan_sql(&server, &default_session(), "SELECT * FROM t").await;
    if let QueryPlan::SelectRows { schema, access, .. } = after {
        let rows = match access {
            ReadAccess::PointLookup { key } => server
                .read_visible_row(None, &schema, &key)
                .await
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            ReadAccess::PrimaryKeyInLookup { keys } => server
                .read_visible_rows_by_pk_in_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &keys,
                )
                .await
                .unwrap(),
            ReadAccess::PrefixScan { filter } => server
                .scan_visible_rows(None, &schema, filter.as_ref())
                .await
                .unwrap(),
            ReadAccess::PrimaryKeyRangeScan {
                lower,
                upper,
                filter,
            } => server
                .scan_visible_row_entries_by_pk_range_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    filter.as_ref(),
                    None,
                    0,
                )
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row)| row)
                .collect(),
            ReadAccess::SecondaryIndexLookup {
                index_name,
                key,
                filter,
                ..
            } => server
                .scan_index_rows_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &index_name,
                    &key,
                    filter.as_ref(),
                )
                .await
                .unwrap(),
            ReadAccess::SecondaryIndexRangeScan {
                index_name,
                lower,
                upper,
                filter,
                ..
            } => server
                .scan_index_row_entries_by_range_at(
                    None,
                    DEFAULT_DATABASE_NAME,
                    DEFAULT_SCHEMA_NAME,
                    &schema,
                    &index_name,
                    lower.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    upper.as_ref().map(|(value, inclusive)| (value, *inclusive)),
                    filter.as_ref(),
                )
                .await
                .unwrap()
                .into_iter()
                .map(|(_, row)| row)
                .collect(),
        };
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected select rows");
    }
}

#[tokio::test]
async fn kv_engine_backend_transaction_commit_makes_write_visible_after_commit_only() {
    let (store, _temp_dir) = new_kv_engine_store();
    let server = GatewayServer::new(store);
    let mut writer = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
    )
    .await;

    server
        .handle_session_command(&mut writer, "BEGIN")
        .await
        .unwrap();
    let session = server.session_catalog(&writer);
    let plan = plan_sql(
        &server,
        &session,
        "INSERT INTO t (id, val) VALUES (1, 'tx')",
    )
    .await;
    server
        .execute_plan(plan, Some(writer.pid_and_secret_key().0), FieldFormat::Text)
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        server
            .read_visible_row(
                Some(writer.pid_and_secret_key().0),
                &schema,
                &ColumnValue::Int32(1)
            )
            .await
            .unwrap()
            .is_some()
    );

    server
        .handle_session_command(&mut writer, "COMMIT")
        .await
        .unwrap();

    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn session_transaction_status_and_select_1_fast_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
    exec_sql_for_client(&server, &mut client, "SELECT 1")
        .await
        .unwrap();
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

    exec_sql_for_client(&server, &mut client, "SELECT 1")
        .await
        .unwrap();
    assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn end_alias_commits_transaction() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
    )
    .await;

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO t (id, val) VALUES (1, 'ok')",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "END")
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn session_transaction_rollback_discards_insert() {
    let (store, _temp_dir) = new_kv_engine_store();
    let server = GatewayServer::new(store);
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
    )
    .await;

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO t (id, val) VALUES (1, 'tx')",
    )
    .await
    .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(
                Some(client.pid_and_secret_key().0),
                &schema,
                &ColumnValue::Int32(1)
            )
            .await
            .unwrap()
            .is_some()
    );

    exec_sql_for_client(&server, &mut client, "ROLLBACK")
        .await
        .unwrap();
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn simple_query_multi_statement_rolls_back_on_error() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE multi_tx (id INT PRIMARY KEY, val TEXT)",
    )
    .await
    .unwrap();

    let err = exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO multi_tx (id, val) VALUES (1, 'a'); \
             INSERT INTO multi_tx (id, missing) VALUES (2, 'bad');",
    )
    .await
    .unwrap_err();
    assert!(matches!(err, PgWireError::UserError(_)));
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "multi_tx")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test]
async fn foreign_key_insert_and_delete_are_enforced() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE fk_parent (id INT PRIMARY KEY)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE fk_child (id INT PRIMARY KEY, parent_id INT REFERENCES fk_parent(id))",
    )
    .await
    .unwrap();

    let missing_parent = exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO fk_child (id, parent_id) VALUES (1, 99)",
    )
    .await
    .unwrap_err();
    assert!(format!("{missing_parent:?}").contains("23503"));

    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO fk_parent (id) VALUES (99)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO fk_child (id, parent_id) VALUES (1, 99)",
    )
    .await
    .unwrap();

    let referenced_parent =
        exec_sql_for_client(&server, &mut client, "DELETE FROM fk_parent WHERE id = 99")
            .await
            .unwrap_err();
    assert!(format!("{referenced_parent:?}").contains("23503"));
}

#[tokio::test]
async fn savepoint_rollback_discards_later_writes() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
    )
    .await;

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO t (id, val) VALUES (1, 'kept')",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "SAVEPOINT sp1")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO t (id, val) VALUES (2, 'rolled_back')",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "ROLLBACK TO SAVEPOINT sp1")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO t (id, val) VALUES (3, 'after')",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "RELEASE SAVEPOINT sp1")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(3))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn defaults_and_sequences_fill_omitted_columns() {
    let server = GatewayServer::new(new_store());

    exec_sql(
        &server,
        &default_session(),
        "CREATE SEQUENCE custom_seq INCREMENT BY 5 START 7",
    )
    .await;
    exec_sql(
            &server,
            &default_session(),
            "CREATE TABLE t (id INT PRIMARY KEY DEFAULT nextval('custom_seq'), name TEXT DEFAULT 'anon', active BOOL DEFAULT true)",
        )
        .await;

    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO t (name) VALUES (DEFAULT), ('bob')",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    let first = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(7))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.get("name"), Some(&ColumnValue::Text("anon".into())));
    assert_eq!(first.get("active"), Some(&ColumnValue::Boolean(true)));

    let second = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(12))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.get("name"), Some(&ColumnValue::Text("bob".into())));
    assert_eq!(second.get("active"), Some(&ColumnValue::Boolean(true)));

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE serial_t (id SERIAL PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO serial_t (name) VALUES ('a'), ('b')",
    )
    .await;
    let serial_schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "serial_t")
        .await
        .unwrap()
        .unwrap();
    assert!(
        serial_schema
            .find_column("id")
            .unwrap()
            .default
            .as_deref()
            .unwrap()
            .starts_with("nextval(")
    );
    assert!(
        server
            .read_visible_row(None, &serial_schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        server
            .read_visible_row(None, &serial_schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn compatibility_sprint_sql_smoke() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE src (id INT PRIMARY KEY, amount INT, label TEXT)",
        "INSERT INTO src (id, amount, label) VALUES (1, 10, 'a'), (2, 20, 'b')",
        "ALTER TABLE src ADD COLUMN active BOOL DEFAULT true",
        "ALTER TABLE src ALTER COLUMN label SET NOT NULL",
        "ALTER TABLE src ALTER COLUMN active DROP DEFAULT",
        "ALTER TABLE src ADD CONSTRAINT src_amount_check CHECK (amount > 0)",
        "ALTER TABLE src ADD CONSTRAINT src_label_key UNIQUE (label)",
        "ALTER TABLE src RENAME COLUMN label TO name",
        "CREATE TABLE copied AS SELECT id, amount, name FROM src WHERE amount >= 10",
        "CREATE TABLE dest (id INT PRIMARY KEY, amount INT, name TEXT)",
        "INSERT INTO dest SELECT id, amount, name FROM copied",
        "CREATE VIEW src_view AS SELECT name, amount FROM src",
        "CREATE INDEX src_amount_name_idx ON src (amount, name)",
        "CREATE TABLE parent (id INT PRIMARY KEY)",
        "ALTER TABLE src ADD CONSTRAINT src_parent_fk FOREIGN KEY (id) REFERENCES parent(id)",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "src")
        .await
        .unwrap()
        .unwrap();
    assert!(schema.find_column("active").is_some());
    assert!(schema.find_column("name").is_some());
    assert_eq!(schema.check_constraints.len(), 1);
    assert_eq!(schema.unique_constraints.len(), 2);
    assert_eq!(schema.foreign_keys.len(), 1);
    let indexes = server
        .catalog
        .list_indexes_for_table(schema.table_id)
        .await
        .unwrap();
    let multi_index = indexes
        .iter()
        .find(|index| index.index_name == "src_amount_name_idx")
        .unwrap();
    assert_eq!(multi_index.column_names, vec!["amount", "name"]);

    let dest_schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "dest")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &dest_schema, &ColumnValue::Int32(1))
            .await
            .unwrap()
            .is_some()
    );

    exec_sql_for_client(
            &server,
            &mut client,
            "PREPARE ins_plan (int, int, text) AS INSERT INTO dest (id, amount, name) VALUES ($1, $2, $3)",
        )
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut client, "EXECUTE ins_plan(3, 30, 'c')")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut client, "DEALLOCATE ins_plan")
        .await
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &dest_schema, &ColumnValue::Int32(3))
            .await
            .unwrap()
            .is_some()
    );

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "SELECT name, sum(amount) FROM src GROUP BY name ORDER BY name",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();

    for sql in [
        "DROP INDEX IF EXISTS src_label_key",
        "DROP VIEW IF EXISTS src_view RESTRICT",
        "CREATE SEQUENCE temp_seq",
        "DROP SEQUENCE temp_seq",
        "CREATE SCHEMA scratch",
        "DROP SCHEMA scratch",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn compatibility_sprint_query_smoke() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE customers (id INT PRIMARY KEY, region TEXT, active BOOL)",
        "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)",
        "INSERT INTO customers (id, region, active) VALUES (1, 'east', true), (2, 'west', true), (3, 'north', false)",
        "INSERT INTO orders (id, customer_id, amount, status) VALUES (10, 1, 15, 'open'), (11, 1, 25, 'paid'), (12, 2, 5, 'open'), (13, 4, 40, 'orphan')",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    for sql in [
        "SELECT c.region, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id AND o.amount >= 10 ORDER BY c.region, o.amount",
        "SELECT c.id, count(o.id) AS order_count FROM customers c LEFT JOIN orders o ON c.id = o.customer_id GROUP BY c.id ORDER BY c.id",
        "SELECT o.id, c.region FROM customers c RIGHT JOIN orders o ON c.id = o.customer_id ORDER BY o.id",
        "SELECT count(*) AS joined_rows FROM customers c FULL JOIN orders o ON c.id = o.customer_id",
        "SELECT count(*) AS total, sum(amount) AS sum_amount, avg(amount) AS avg_amount, min(amount) AS min_amount, max(amount) AS max_amount FROM orders",
        "SELECT customer_id, status, count(*) AS cnt, sum(amount) AS total FROM orders GROUP BY customer_id, status HAVING sum(amount) >= 5 ORDER BY customer_id, status",
        "SELECT DISTINCT status FROM orders ORDER BY status",
        "SELECT count(DISTINCT status) AS statuses FROM orders",
        "SELECT id FROM orders WHERE amount > (SELECT avg(amount) FROM orders) ORDER BY id",
        "SELECT id FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND orders.amount > 20) ORDER BY id",
        "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount >= 15) ORDER BY id",
        "WITH high_orders AS (SELECT * FROM orders WHERE amount >= 15) SELECT count(*) AS high_count FROM high_orders",
        "SELECT customer_id FROM orders WHERE amount < 20 UNION SELECT id FROM customers WHERE active = true ORDER BY customer_id",
        "SELECT customer_id FROM orders WHERE amount < 20 UNION ALL SELECT id FROM customers WHERE active = true ORDER BY customer_id",
        "SELECT customer_id FROM orders INTERSECT SELECT id FROM customers ORDER BY customer_id",
        "SELECT id FROM customers EXCEPT SELECT customer_id FROM orders ORDER BY id",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn compatibility_sprint_transaction_smoke() {
    let server = GatewayServer::new(new_store());
    let mut c1 = TestClient::default();
    let mut c2 = TestClient::default();
    c2.set_pid_and_secret_key(2, SecretKey::I32(43));

    for sql in [
        "CREATE TABLE tx_t (id INT PRIMARY KEY, val INT)",
        "INSERT INTO tx_t (id, val) VALUES (1, 10), (2, 20)",
    ] {
        exec_sql_for_client(&server, &mut c1, sql).await.unwrap();
    }
    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "tx_t")
        .await
        .unwrap()
        .unwrap();

    exec_sql_for_client(&server, &mut c1, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "UPDATE tx_t SET val = 11 WHERE id = 1")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "SAVEPOINT sp")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "UPDATE tx_t SET val = 12 WHERE id = 1")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "ROLLBACK TO SAVEPOINT sp")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "RELEASE SAVEPOINT sp")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c1, "COMMIT")
        .await
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("val"), Some(&ColumnValue::Int32(11)));

    exec_sql_for_client(&server, &mut c1, "BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c2, "UPDATE tx_t SET val = 21 WHERE id = 2")
        .await
        .unwrap();
    let read_committed = server
        .read_visible_row(
            Some(c1.pid_and_secret_key().0),
            &schema,
            &ColumnValue::Int32(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_committed.get("val"), Some(&ColumnValue::Int32(21)));
    exec_sql_for_client(&server, &mut c1, "ROLLBACK")
        .await
        .unwrap();

    exec_sql_for_client(&server, &mut c1, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut c2, "UPDATE tx_t SET val = 22 WHERE id = 2")
        .await
        .unwrap();
    let repeatable = server
        .read_visible_row(
            Some(c1.pid_and_secret_key().0),
            &schema,
            &ColumnValue::Int32(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeatable.get("val"), Some(&ColumnValue::Int32(21)));
    let stale_update =
        exec_sql_for_client(&server, &mut c1, "UPDATE tx_t SET val = 23 WHERE id = 2")
            .await
            .unwrap_err();
    assert!(format!("{stale_update:?}").contains("40001"));
    exec_sql_for_client(&server, &mut c1, "ROLLBACK")
        .await
        .unwrap();

    exec_sql_for_client(
        &server,
        &mut c1,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    )
    .await
    .unwrap();
    assert_eq!(
        c1.metadata()
            .get(METADATA_TRANSACTION_ISOLATION)
            .map(String::as_str),
        Some("serializable")
    );
    exec_sql_for_client(
        &server,
        &mut c1,
        "SELECT * FROM tx_t WHERE id = 1 FOR UPDATE",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut c1,
        "SELECT * FROM tx_t WHERE id = 1 FOR SHARE NOWAIT",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut c1,
        "SELECT * FROM tx_t WHERE id = 1 FOR UPDATE SKIP LOCKED",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn phase_a_sqllogictest_baseline_runs() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    let script = include_str!("../../../../tests/sqllogictest/phase_a.slt");
    let mut lines = script.lines().peekable();
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line == "statement ok" {
            let mut sql_lines = Vec::new();
            while let Some(next) = lines.peek().copied() {
                if next.trim().is_empty() {
                    lines.next();
                    break;
                }
                sql_lines.push(lines.next().unwrap());
            }
            exec_sql_for_client(&server, &mut client, &sql_lines.join("\n"))
                .await
                .unwrap();
        } else if line.starts_with("query ") {
            let mut sql_lines = Vec::new();
            while let Some(next) = lines.peek().copied() {
                if next.trim() == "----" {
                    lines.next();
                    break;
                }
                sql_lines.push(lines.next().unwrap());
            }
            while let Some(next) = lines.peek().copied() {
                if next.trim().is_empty() {
                    lines.next();
                    break;
                }
                lines.next();
            }
            let responses = exec_sql_for_client(&server, &mut client, &sql_lines.join("\n"))
                .await
                .unwrap();
            assert!(matches!(responses.as_slice(), [Response::Query(_)]));
        } else {
            panic!("unsupported sqllogictest directive: {line}");
        }
    }
}

#[tokio::test]
async fn real_view_catalog_and_common_pg_functions_work() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    for sql in [
        "CREATE TABLE view_base (id INT PRIMARY KEY, name VARCHAR(8), at TIME, tz TIMETZ, span INTERVAL)",
        "INSERT INTO view_base (id, name, at, tz, span) VALUES (1, 'alice', '12:34:56', '12:34:56+08', '1 day')",
        "CREATE VIEW view_smoke AS SELECT name, at, span FROM view_base WHERE id = 1",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "view_base")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.find_column("name").unwrap().data_type,
        DataType::VarChar(Some(8))
    );
    assert_eq!(schema.find_column("at").unwrap().data_type, DataType::Time);
    assert_eq!(
        schema.find_column("tz").unwrap().data_type,
        DataType::TimeTz
    );
    assert_eq!(
        schema.find_column("span").unwrap().data_type,
        DataType::Interval
    );

    for sql in [
        "SELECT name FROM view_smoke",
        "SELECT viewname, definition FROM pg_catalog.pg_views WHERE viewname = 'view_smoke'",
        "SELECT table_name, view_definition FROM information_schema.views WHERE table_name = 'view_smoke'",
        "SELECT pg_catalog.format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute WHERE attname = 'name'",
        "SELECT pg_catalog.pg_get_expr(conbin, conrelid) FROM pg_catalog.pg_constraint",
        "SELECT pg_catalog.pg_get_viewdef(oid) FROM pg_catalog.pg_class WHERE relname = 'view_smoke'",
        "SELECT pg_catalog.obj_description(oid) FROM pg_catalog.pg_class WHERE relname = 'view_smoke'",
        "SELECT pg_catalog.col_description(attrelid, attnum) FROM pg_catalog.pg_attribute WHERE attname = 'name'",
    ] {
        let responses = exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(matches!(responses.as_slice(), [Response::Query(_)]));
    }

    let viewdef_batches = server
            .collect_datafusion_batches(
                "SELECT pg_catalog.pg_get_viewdef(oid) FROM pg_catalog.pg_class WHERE relname = 'view_smoke'",
                &default_session(),
            )
            .await
            .unwrap();
    let viewdef_batch = viewdef_batches.first().unwrap();
    let viewdef = arrow_array_value_to_string(viewdef_batch.column(0), 0);
    assert!(viewdef.contains("SELECT name, at, span FROM view_base"));

    let too_long = exec_sql_for_client(
            &server,
            &mut client,
            "INSERT INTO view_base (id, name, at, tz, span) VALUES (2, 'too-long-name', '01:02:03', '01:02:03+00', '2 days')",
        )
        .await
        .unwrap_err();
    assert!(format!("{too_long:?}").contains("22001"));
}

#[tokio::test]
async fn session_transaction_update_and_delete_follow_transaction_visibility() {
    let (store, _temp_dir) = new_kv_engine_store();
    let server = GatewayServer::new(store);
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO t (id, val) VALUES (1, 'old')",
        "INSERT INTO t (id, val) VALUES (2, 'gone')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE t SET val = 'new' WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "DELETE FROM t WHERE id = 2")
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    let txn_id = Some(client.pid_and_secret_key().0);
    let tx_row = server
        .read_visible_row(txn_id, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tx_row.get("val"), Some(&ColumnValue::Text("new".into())));
    assert!(
        server
            .read_visible_row(txn_id, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );

    let outside_row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outside_row.get("val"),
        Some(&ColumnValue::Text("old".into()))
    );
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_some()
    );

    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();

    let committed_row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        committed_row.get("val"),
        Some(&ColumnValue::Text("new".into()))
    );
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );
}

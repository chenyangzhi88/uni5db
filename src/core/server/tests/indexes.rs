use super::{
    ClientInfo, ColumnValue, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DataType, FieldFormat,
    GatewayServer, METADATA_SEARCH_PATH, PgWireError, QueryPlan, ReadAccess, Response, TestClient,
    Type, WriteAccess, analytics_session, default_session, exec_sql, exec_sql_for_client,
    index_entry_prefix, new_store, plan_sql, response_field_names,
};

#[tokio::test]
async fn insert_returning_with_expression_projection() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, amount INT)",
        "INSERT INTO users (id, amount) VALUES (1, 10)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO users (id, amount) VALUES (2, 20) RETURNING id, amount + 1 AS amount_plus_one",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(
        response_field_names(&responses[0]),
        vec!["id".to_string(), "amount_plus_one".to_string()]
    );
}

#[tokio::test]
async fn update_set_expression_and_returning_alias() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, amount INT)",
        "INSERT INTO users (id, amount) VALUES (1, 10)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE users SET amount = amount + 5 WHERE id = 1 RETURNING amount + 1 AS amount_plus_one",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(
        response_field_names(&responses[0]),
        vec!["amount_plus_one".to_string()]
    );

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("amount"), Some(&ColumnValue::Int32(15)));
}

#[tokio::test]
async fn insert_on_conflict_do_update_with_selection_filter() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, amount INT)",
        "INSERT INTO users (id, amount) VALUES (1, 10)",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(
            &server,
            &mut client,
            "INSERT INTO users (id, amount) VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET amount = excluded.amount WHERE false",
        )
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("amount"), Some(&ColumnValue::Int32(10)));
}

#[tokio::test]
async fn insert_on_conflict_on_constraint_update() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT, CONSTRAINT users_email_uniq UNIQUE (email))",
        "INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'alice')",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(
            &server,
            &mut client,
            "INSERT INTO users (id, email, name) VALUES (2, 'a@example.com', 'bob') ON CONFLICT ON CONSTRAINT users_email_uniq DO UPDATE SET name = EXCLUDED.name",
        )
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("bob".into())));
}

#[tokio::test]
async fn insert_duplicate_without_on_conflict_errors() {
    let server = GatewayServer::new(new_store());

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
    )
    .await;

    let plan = plan_sql(
        &server,
        &default_session(),
        "INSERT INTO users (id, name) VALUES (1, 'bob')",
    )
    .await;
    let err = server
        .execute_plan(plan, None, FieldFormat::Text)
        .await
        .unwrap_err();
    match err {
        PgWireError::UserError(info) => assert_eq!(info.code, "23505"),
        other => panic!("expected duplicate key error, got {other:?}"),
    }
}

#[tokio::test]
async fn create_index_backfills_existing_rows() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
        "INSERT INTO users (id, email) VALUES (1, 'a@example.com'), (2, 'b@example.com')",
        "CREATE INDEX users_email_idx ON users (email)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let table = server
        .catalog
        .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let indexes = server
        .catalog
        .list_indexes_for_table(table.schema.table_id)
        .await
        .unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_name, "users_email_idx");

    let prefix = index_entry_prefix(
        DEFAULT_DATABASE_NAME,
        DEFAULT_SCHEMA_NAME,
        "users",
        "users_email_idx",
        &ColumnValue::Text("a@example.com".into()),
    );
    let entries = server.store.scan_prefix(&prefix).await.unwrap();
    assert_eq!(entries.len(), 1);

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT id FROM users WHERE email = 'a@example.com'",
    )
    .await;
    match plan {
        QueryPlan::SelectRows {
            access:
                ReadAccess::SecondaryIndexLookup {
                    index_name, key, ..
                },
            ..
        } => {
            assert_eq!(index_name, "users_email_idx");
            assert_eq!(key, ColumnValue::Text("a@example.com".into()));
        }
        other => panic!("expected secondary index lookup, got {other:?}"),
    }
}

#[tokio::test]
async fn create_unique_index_rejects_existing_duplicates() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
        "INSERT INTO users (id, email) VALUES (1, 'dup@example.com'), (2, 'dup@example.com')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let plan = plan_sql(
        &server,
        &default_session(),
        "CREATE UNIQUE INDEX users_email_idx ON users (email)",
    )
    .await;
    let err = server
        .execute_plan(plan, None, FieldFormat::Text)
        .await
        .unwrap_err();
    match err {
        PgWireError::UserError(info) => assert_eq!(info.code, "23505"),
        other => panic!("expected duplicate key error, got {other:?}"),
    }
}

#[tokio::test]
async fn unique_index_rejects_duplicate_inserts() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
        "CREATE UNIQUE INDEX users_email_idx ON users (email)",
        "INSERT INTO users (id, email) VALUES (1, 'a@example.com')",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    let err = exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO users (id, email) VALUES (2, 'a@example.com')",
    )
    .await
    .unwrap_err();
    match err {
        PgWireError::UserError(info) => assert_eq!(info.code, "23505"),
        other => panic!("expected duplicate key error, got {other:?}"),
    }
}

#[tokio::test]
async fn update_maintains_secondary_index_entries() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
        "INSERT INTO users (id, email) VALUES (1, 'old@example.com')",
        "CREATE INDEX users_email_idx ON users (email)",
        "UPDATE users SET email = 'new@example.com' WHERE id = 1",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

    let old_prefix = index_entry_prefix(
        DEFAULT_DATABASE_NAME,
        DEFAULT_SCHEMA_NAME,
        "users",
        "users_email_idx",
        &ColumnValue::Text("old@example.com".into()),
    );
    let new_prefix = index_entry_prefix(
        DEFAULT_DATABASE_NAME,
        DEFAULT_SCHEMA_NAME,
        "users",
        "users_email_idx",
        &ColumnValue::Text("new@example.com".into()),
    );
    assert!(
        server
            .store
            .scan_prefix(&old_prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        server.store.scan_prefix(&new_prefix).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn planner_uses_primary_key_range_scan() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE events (id INT PRIMARY KEY, label TEXT)",
        "INSERT INTO events (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT id FROM events WHERE id >= 2 AND id < 4",
    )
    .await;
    let QueryPlan::SelectRows { schema, access, .. } = plan else {
        panic!("expected select plan");
    };
    match access {
        ReadAccess::PrimaryKeyRangeScan { lower, upper, .. } => {
            assert_eq!(lower, Some((ColumnValue::Int32(2), true)));
            assert_eq!(upper, Some((ColumnValue::Int32(4), false)));
        }
        other => panic!("expected primary key range scan, got {other:?}"),
    }
    let rows = server
        .scan_visible_row_entries_by_pk_range_at(
            None,
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            &schema,
            Some((&ColumnValue::Int32(2), true)),
            Some((&ColumnValue::Int32(4), false)),
            None,
            None,
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        rows.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
        vec![ColumnValue::Int32(2), ColumnValue::Int32(3)]
    );
}

#[tokio::test]
async fn update_and_delete_can_use_secondary_index_lookup() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, active BOOLEAN)",
        "INSERT INTO users (id, email, active) VALUES (1, 'a@example.com', true), (2, 'b@example.com', true)",
        "CREATE INDEX users_email_idx ON users (email)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let update_plan = plan_sql(
        &server,
        &default_session(),
        "UPDATE users SET active = false WHERE email = 'a@example.com'",
    )
    .await;
    match &update_plan {
        QueryPlan::UpdateRows {
            access:
                WriteAccess::SecondaryIndexLookup {
                    index_name, key, ..
                },
            ..
        } => {
            assert_eq!(index_name, "users_email_idx");
            assert_eq!(key, &ColumnValue::Text("a@example.com".into()));
        }
        other => panic!("expected secondary index update, got {other:?}"),
    }
    server
        .execute_plan(update_plan, None, FieldFormat::Text)
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("active"), Some(&ColumnValue::Boolean(false)));

    let delete_plan = plan_sql(
        &server,
        &default_session(),
        "DELETE FROM users WHERE email = 'b@example.com'",
    )
    .await;
    match &delete_plan {
        QueryPlan::DeleteRows {
            access:
                WriteAccess::SecondaryIndexLookup {
                    index_name, key, ..
                },
            ..
        } => {
            assert_eq!(index_name, "users_email_idx");
            assert_eq!(key, &ColumnValue::Text("b@example.com".into()));
        }
        other => panic!("expected secondary index delete, got {other:?}"),
    }
    server
        .execute_plan(delete_plan, None, FieldFormat::Text)
        .await
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn update_and_delete_can_use_primary_key_range_scan() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE events (id INT PRIMARY KEY, label TEXT)",
        "INSERT INTO events (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let update_plan = plan_sql(
        &server,
        &default_session(),
        "UPDATE events SET label = 'hit' WHERE id >= 2 AND id <= 3",
    )
    .await;
    match &update_plan {
        QueryPlan::UpdateRows {
            access: WriteAccess::PrimaryKeyRangeScan { lower, upper, .. },
            ..
        } => {
            assert_eq!(lower, &Some((ColumnValue::Int32(2), true)));
            assert_eq!(upper, &Some((ColumnValue::Int32(3), true)));
        }
        other => panic!("expected primary key range update, got {other:?}"),
    }
    server
        .execute_plan(update_plan, None, FieldFormat::Text)
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "events")
        .await
        .unwrap()
        .unwrap();
    for id in [2, 3] {
        let row = server
            .read_visible_row(None, &schema, &ColumnValue::Int32(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.get("label"), Some(&ColumnValue::Text("hit".into())));
    }

    let delete_plan = plan_sql(
        &server,
        &default_session(),
        "DELETE FROM events WHERE id > 1 AND id < 4",
    )
    .await;
    match &delete_plan {
        QueryPlan::DeleteRows {
            access: WriteAccess::PrimaryKeyRangeScan { lower, upper, .. },
            ..
        } => {
            assert_eq!(lower, &Some((ColumnValue::Int32(1), false)));
            assert_eq!(upper, &Some((ColumnValue::Int32(4), false)));
        }
        other => panic!("expected primary key range delete, got {other:?}"),
    }
    server
        .execute_plan(delete_plan, None, FieldFormat::Text)
        .await
        .unwrap();
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
            .is_none()
    );
}

#[tokio::test]
async fn not_null_constraints_apply_to_insert_and_update() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;

    let insert_plan = plan_sql(
        &server,
        &default_session(),
        "INSERT INTO users (id) VALUES (1)",
    )
    .await;
    let err = server
        .execute_plan(insert_plan, None, FieldFormat::Text)
        .await
        .unwrap_err();
    match err {
        PgWireError::UserError(info) => assert_eq!(info.code, "23502"),
        other => panic!("expected not-null error, got {other:?}"),
    }

    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
    )
    .await;
    let update_plan = plan_sql(
        &server,
        &default_session(),
        "UPDATE users SET name = NULL WHERE id = 1",
    )
    .await;
    let err = server
        .execute_plan(update_plan, None, FieldFormat::Text)
        .await
        .unwrap_err();
    match err {
        PgWireError::UserError(info) => assert_eq!(info.code, "23502"),
        other => panic!("expected not-null error, got {other:?}"),
    }
}

#[tokio::test]
async fn update_returning_fast_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE users SET name = 'bob' WHERE id = 1 RETURNING id, name",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    match &responses[0] {
        Response::Query(_) => {}
        other => panic!("expected query response, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_returning_fast_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "DELETE FROM users WHERE id = 1 RETURNING id, name",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    match &responses[0] {
        Response::Query(_) => {}
        other => panic!("expected query response, got {other:?}"),
    }
}

#[tokio::test]
async fn dml_filters_cover_common_postgres_predicates() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, score INT, tag TEXT)",
        "INSERT INTO items (id, name, score, tag) VALUES \
             (1, 'Alice', 10, NULL), \
             (2, 'bob', 20, 'keep'), \
             (3, 'carol', 30, 'drop'), \
             (4, 'ALAN', 40, 'keep')",
        "UPDATE items SET score = score + 1 WHERE id BETWEEN 2 AND 4",
        "UPDATE items SET score = score + 10 WHERE tag IS NOT NULL AND name ILIKE 'a%'",
        "UPDATE items SET score = score + 100 WHERE tag IS NULL OR name LIKE 'c%'",
        "DELETE FROM items WHERE id NOT IN (1, 2, 4) OR tag IS NULL",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "items")
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
            .read_visible_row(None, &schema, &ColumnValue::Int32(3))
            .await
            .unwrap()
            .is_none()
    );
    let row2 = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row2.get("score"), Some(&ColumnValue::Int32(21)));
    let row4 = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(4))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row4.get("score"), Some(&ColumnValue::Int32(51)));
}

#[tokio::test]
async fn scan_with_filter() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, score INT)",
        "INSERT INTO t (id, score) VALUES (1, 10)",
        "INSERT INTO t (id, score) VALUES (2, 20)",
        "INSERT INTO t (id, score) VALUES (3, 30)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT * FROM t WHERE score > 15",
    )
    .await;
    match plan {
        QueryPlan::SelectRows {
            access: ReadAccess::PrefixScan { filter },
            ..
        } => {
            let rows = server
                .scan_visible_rows(None, &schema, filter.as_ref())
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("expected PrefixScan"),
    }
}

#[tokio::test]
async fn schema_roundtrip() {
    let server = GatewayServer::new(new_store());

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.table_name, "test");
    assert_eq!(schema.primary_key, "id");
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.pk_data_type(), &DataType::Int64);
    assert_eq!(
        schema.find_column("name").unwrap().data_type,
        DataType::Text
    );
    assert!(!schema.find_column("name").unwrap().nullable);
    assert_eq!(
        schema.find_column("active").unwrap().data_type,
        DataType::Boolean
    );

    let catalog_table = server
        .catalog
        .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog_table.schema.table_id, schema.table_id);
}

#[tokio::test]
async fn select_nonexistent_table_errors() {
    let server = GatewayServer::new(new_store());
    let stmts = server.parse_sql("SELECT * FROM nope").unwrap();
    let result = server
        .plan_statement(&default_session(), stmts.into_iter().next().unwrap())
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn spoofed_responses() {
    let server = GatewayServer::new(new_store());
    let client = TestClient::default();
    assert!(server.spoofed_response(&client, "SELECT 1").is_some());
    assert!(
        server
            .spoofed_response(&client, "SELECT version()")
            .is_some()
    );
    assert!(
        server
            .spoofed_response(&client, "SET client_encoding = 'UTF8'")
            .is_some()
    );
    assert!(
        server
            .spoofed_response(&client, "SHOW search_path")
            .is_some()
    );
    assert!(
        server
            .spoofed_response(&client, "SELECT * FROM pg_catalog.pg_type")
            .is_none()
    );
    assert!(
        server
            .spoofed_response(&client, "VACUUM ANALYZE pgbench_branches")
            .is_none()
    );
    assert!(server.spoofed_response(&client, "ANALYZE users").is_none());
    assert!(
        server
            .spoofed_response(&client, "SELECT * FROM users")
            .is_none()
    );
}

#[test]
fn parameter_placeholder_replacement_skips_literals_and_comments() {
    let sql = "SELECT '$1', $1, \"col$2\" FROM t WHERE name = $2 -- keep $1\n";
    let replaced = GatewayServer::replace_pg_parameter_placeholders(
        sql,
        &["10".to_string(), "'alice'".to_string()],
    );
    assert_eq!(
        replaced,
        "SELECT '$1', 10, \"col$2\" FROM t WHERE name = 'alice' -- keep $1\n"
    );
}

#[tokio::test]
async fn describe_select_with_parameter_defaults_returns_fields() {
    let server = GatewayServer::new(new_store());
    let client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let defaults = GatewayServer::placeholder_defaults(&[Some(Type::INT4)]);
    let sql = GatewayServer::replace_pg_parameter_placeholders(
        "SELECT name FROM users WHERE id = $1",
        &defaults,
    );
    let fields = server
        .describe_sql_fields(&client, &sql, FieldFormat::Text)
        .await
        .unwrap();

    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "name");
    assert_eq!(fields[0].datatype(), &Type::TEXT);
}

#[tokio::test]
async fn describe_prepared_transaction_command_returns_no_fields() {
    let server = GatewayServer::new(new_store());
    let client = TestClient::default();

    let fields = server
        .describe_sql_fields(&client, "BEGIN TRANSACTION", FieldFormat::Text)
        .await
        .unwrap();

    assert!(fields.is_empty());
}

#[tokio::test]
async fn orm_metadata_introspection_smoke_queries() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "SELECT pg_catalog.version()",
        "SELECT current_schema()",
        "SELECT current_database()",
        "SHOW transaction isolation level",
        "SHOW standard_conforming_strings",
        "SHOW server_version",
        "SHOW client_encoding",
        "CREATE TABLE orm_meta (id INT PRIMARY KEY, name TEXT)",
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'orm_meta'",
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orm_meta' ORDER BY ordinal_position",
        "SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid WHERE c.relname = 'orm_meta' ORDER BY a.attnum",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn parameterized_insert_sql_runs_through_existing_execution_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let sql = GatewayServer::replace_pg_parameter_placeholders(
        "INSERT INTO users (id, name) VALUES ($1, $2)",
        &["1".to_string(), "'alice'".to_string()],
    );
    exec_sql_for_client(&server, &mut client, &sql)
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("alice".into())));
}

#[tokio::test]
async fn integer_key_ordering_in_scan() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE nums (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO nums (id, val) VALUES (10, 'ten')",
        "INSERT INTO nums (id, val) VALUES (2, 'two')",
        "INSERT INTO nums (id, val) VALUES (1, 'one')",
        "INSERT INTO nums (id, val) VALUES (20, 'twenty')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "nums")
        .await
        .unwrap()
        .unwrap();
    let rows = server.scan_visible_rows(None, &schema, None).await.unwrap();

    let ids: Vec<i32> = rows
        .iter()
        .map(|r| match r.get("id").unwrap() {
            ColumnValue::Int32(v) => *v,
            _ => panic!("expected Int32"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 10, 20]);
}

#[tokio::test]
async fn create_table_with_schema_qualified_name() {
    let server = GatewayServer::new(new_store());
    exec_sql(&server, &default_session(), "CREATE SCHEMA analytics").await;
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE analytics.events (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "analytics.events",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.table_name, "events");

    let catalog_table = server
        .catalog
        .load_table(DEFAULT_DATABASE_NAME, "analytics", "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog_table.schema_name, "analytics");
    assert_eq!(catalog_table.table_name, "events");
}

#[tokio::test]
async fn set_search_path_updates_current_schema_and_unqualified_resolution() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(&server, &default_session(), "CREATE SCHEMA analytics").await;
    server
        .handle_session_command(&mut client, "SET search_path TO analytics, public")
        .await
        .unwrap()
        .unwrap();

    let session = server.session_catalog(&client);
    assert_eq!(session.schema_name, "analytics");
    assert_eq!(
        client.metadata().get(METADATA_SEARCH_PATH).unwrap(),
        "analytics, public"
    );

    exec_sql(
        &server,
        &session,
        "CREATE TABLE events (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, "analytics", "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.table_name, "events");
}

#[tokio::test]
async fn datafusion_slow_path_supports_non_public_schema_and_search_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(&server, &default_session(), "CREATE SCHEMA analytics").await;
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE analytics.events (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &analytics_session(),
        "INSERT INTO events (id, name) VALUES (1, 'a'), (2, 'b')",
    )
    .await;

    let qualified = server
        .execute_via_datafusion(
            "SELECT count(*) AS total FROM analytics.events",
            &default_session(),
        )
        .await;
    assert!(qualified.is_ok());

    server
        .handle_session_command(&mut client, "SET search_path TO analytics, public")
        .await
        .unwrap();
    let session = server.session_catalog(&client);
    let unqualified = server
        .execute_via_datafusion("SELECT count(*) AS total FROM events", &session)
        .await;
    assert!(unqualified.is_ok());
}

#[tokio::test]
async fn search_path_falls_back_to_later_schema_in_fast_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(&server, &default_session(), "CREATE SCHEMA analytics").await;
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE analytics.events (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &analytics_session(),
        "INSERT INTO events (id, name) VALUES (1, 'later')",
    )
    .await;

    server
        .handle_session_command(&mut client, "SET search_path TO missing, analytics, public")
        .await
        .unwrap_err();

    server
        .handle_session_command(&mut client, "SET search_path TO scratch, analytics, public")
        .await
        .unwrap_err();

    exec_sql(&server, &default_session(), "CREATE SCHEMA scratch").await;
    server
        .handle_session_command(&mut client, "SET search_path TO scratch, analytics, public")
        .await
        .unwrap();

    let session = server.session_catalog(&client);
    let plan = plan_sql(&server, &session, "SELECT name FROM events WHERE id = 1").await;
    match plan {
        QueryPlan::SelectRows { schema, .. } => {
            assert_eq!(schema.table_name, "events");
        }
        _ => panic!("expected SelectRows"),
    }
}

#[tokio::test]
async fn datafusion_uses_later_search_path_schema_for_unqualified_table() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(&server, &default_session(), "CREATE SCHEMA scratch").await;
    exec_sql(&server, &default_session(), "CREATE SCHEMA analytics").await;
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE analytics.events (id INT PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &analytics_session(),
        "INSERT INTO events (id, name) VALUES (1, 'later')",
    )
    .await;

    server
        .handle_session_command(&mut client, "SET search_path TO scratch, analytics, public")
        .await
        .unwrap();
    let session = server.session_catalog(&client);
    let result = server
        .execute_via_datafusion("SELECT count(*) AS total FROM events", &session)
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn datafusion_exposes_pg_database_catalog_table() {
    let server = GatewayServer::new(new_store());
    exec_sql(&server, &default_session(), "CREATE DATABASE appdb").await;

    let unqualified = server
        .execute_via_datafusion(
            "SELECT datname FROM pg_database ORDER BY datname",
            &default_session(),
        )
        .await;
    assert!(unqualified.is_ok());

    let qualified = server
        .execute_via_datafusion(
            "SELECT datname FROM pg_catalog.pg_database ORDER BY datname",
            &default_session(),
        )
        .await;
    assert!(qualified.is_ok());

    let extended = server
            .execute_via_datafusion(
                "SELECT datname, datdba, encoding, datcollate, datctype, datlocprovider, datistemplate, datallowconn FROM pg_catalog.pg_database ORDER BY datname",
                &default_session(),
            )
            .await;
    assert!(extended.is_ok());
}

use super::{
    ClientInfo, ColumnValue, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DataType, GatewayMode,
    GatewayServer, METADATA_DATABASE, METADATA_USER, PgWireError, QueryPlan, ReadAccess, Response,
    TestClient, default_session, exec_sql, exec_sql_for_client, is_unsupported_error, new_store,
    plan_sql, response_field_names, response_text_rows, storage_layout,
};

#[tokio::test]
async fn allocate_table_id_increments() {
    let server = GatewayServer::new(new_store());
    let id1 = server.catalog.allocate_table_id().await.unwrap();
    let id2 = server.catalog.allocate_table_id().await.unwrap();
    let id3 = server.catalog.allocate_table_id().await.unwrap();
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);
}

#[tokio::test]
async fn bootstrap_catalog_exposes_default_database_and_schema() {
    let server = GatewayServer::new(new_store());
    server.catalog.ensure_bootstrap().await.unwrap();

    let db = server
        .catalog
        .get_database(DEFAULT_DATABASE_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.database_id, 1);

    let schema = server
        .catalog
        .get_schema(db.database_id, DEFAULT_SCHEMA_NAME)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.schema_name, DEFAULT_SCHEMA_NAME);
}

#[tokio::test]
async fn startup_falls_back_to_default_database_when_requested_name_matches_user() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    client
        .metadata_mut()
        .insert(METADATA_DATABASE.to_string(), "coco".to_string());
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "coco".to_string());

    let database = server
        .resolve_startup_database(Some("coco".to_string()), Some("coco".to_string()))
        .await
        .unwrap();
    assert_eq!(database, DEFAULT_DATABASE_NAME);
}

#[tokio::test]
async fn startup_rejects_unknown_explicit_database() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    client
        .metadata_mut()
        .insert(METADATA_DATABASE.to_string(), "appdb".to_string());
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "coco".to_string());

    let err = server
        .resolve_startup_database(Some("appdb".to_string()), Some("coco".to_string()))
        .await
        .unwrap_err();
    match err {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "3D000");
            assert!(info.message.contains("appdb"));
        }
        other => panic!("expected user error, got {other:?}"),
    }
}

#[tokio::test]
async fn create_database_and_schema_via_sql() {
    let server = GatewayServer::new(new_store());

    for sql in ["CREATE DATABASE appdb", "CREATE SCHEMA analytics"] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let db = server.catalog.get_database("appdb").await.unwrap().unwrap();
    assert_eq!(db.database_name, "appdb");

    let default_db = server
        .catalog
        .get_database(DEFAULT_DATABASE_NAME)
        .await
        .unwrap()
        .unwrap();
    let schema = server
        .catalog
        .get_schema(default_db.database_id, "analytics")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.schema_name, "analytics");
}

#[tokio::test]
async fn create_table_and_insert_select() {
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
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')",
    )
    .await;

    let plan = plan_sql(&server, &default_session(), "SELECT * FROM users").await;
    match &plan {
        QueryPlan::SelectRows {
            schema, projection, ..
        } => {
            assert_eq!(schema.table_name, "users");
            assert_eq!(projection, &vec!["id".to_string(), "name".to_string()]);
        }
        _ => panic!("expected SelectRows"),
    }

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT name FROM users WHERE id = 1",
    )
    .await;
    match &plan {
        QueryPlan::SelectRows {
            access, projection, ..
        } => {
            assert!(matches!(
                access,
                ReadAccess::PointLookup {
                    key: ColumnValue::Int32(1)
                }
            ));
            assert_eq!(projection, &vec!["name".to_string()]);
        }
        _ => panic!("expected SelectRows"),
    }
}

#[tokio::test]
async fn select_accepts_offset_comma_limit_syntax() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT id FROM users ORDER BY id LIMIT 2, 3",
    )
    .await;
    match &plan {
        QueryPlan::SelectRows { limit, offset, .. } => {
            assert_eq!(*limit, Some(3));
            assert_eq!(*offset, 2);
        }
        _ => panic!("expected SelectRows"),
    }
}

#[tokio::test]
async fn filtered_ordered_limit_is_applied_during_pk_scan() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, status TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO users (id, status) VALUES \
             (1, 'pending'), (2, 'paid'), (3, 'paid'), (4, 'pending'), (5, 'paid')",
    )
    .await;

    let plan = plan_sql(
        &server,
        &default_session(),
        "SELECT id FROM users WHERE status = 'paid' ORDER BY id LIMIT 2 OFFSET 1",
    )
    .await;
    let QueryPlan::SelectRows {
        schema,
        access: ReadAccess::PrefixScan { filter },
        limit,
        offset,
        ..
    } = plan
    else {
        panic!("expected filtered prefix scan");
    };

    let rows = server
        .scan_visible_row_entries_by_pk_range_at(
            None,
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            &schema,
            None,
            None,
            filter.as_ref(),
            limit,
            offset,
        )
        .await
        .unwrap();
    let ids = rows
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<ColumnValue>>();
    assert_eq!(ids, vec![ColumnValue::Int32(3), ColumnValue::Int32(5)]);
}

#[tokio::test]
async fn row_writes_persist_mvcc_versions_olap_chunks_and_stats() {
    let store = new_store();
    let server = GatewayServer::new(store.clone());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE t (id INT PRIMARY KEY, amount INT, name TEXT)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO t (id, amount, name) VALUES (1, 10, 'east')",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "UPDATE t SET amount = 15 WHERE id = 1",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    let row_key =
        storage_layout::row_key(schema.table_id, schema.table_epoch, &ColumnValue::Int32(1));
    let row = store
        .get(&row_key)
        .await
        .unwrap()
        .expect("updated row should exist");
    let decoded = storage_layout::decode_row_record(&schema, &row).unwrap();
    assert_eq!(decoded.get("amount"), Some(&ColumnValue::Int32(15)));

    let legacy_version_range = storage_layout::row_version_range(
        schema.table_id,
        schema.table_epoch,
        &ColumnValue::Int32(1),
        None,
    );
    let legacy_versions = store
        .scan_range(
            &legacy_version_range.start,
            legacy_version_range.end.as_deref(),
            legacy_version_range.limit,
            legacy_version_range.reverse,
        )
        .await
        .unwrap();
    assert_eq!(legacy_versions.len(), 0);

    let chunk_range = storage_layout::olap_chunk_meta_range(schema.table_id, schema.table_epoch);
    let chunks = store
        .scan_range(
            &chunk_range.start,
            chunk_range.end.as_deref(),
            chunk_range.limit,
            chunk_range.reverse,
        )
        .await
        .unwrap();
    assert_eq!(chunks.len(), 1);
    let chunk = storage_layout::decode_olap_chunk_meta(&chunks[0].1).unwrap();
    assert_eq!(chunk.row_count, 1);

    let stats = store
        .get(&storage_layout::stats_key(
            schema.table_id,
            schema.table_epoch,
            None,
        ))
        .await
        .unwrap()
        .map(|bytes| storage_layout::decode_table_stats(&bytes).unwrap())
        .unwrap();
    assert_eq!(stats.row_count, 1);
    assert!(stats.zones.iter().any(|zone| {
        zone.min == Some(ColumnValue::Int32(15)) && zone.max == Some(ColumnValue::Int32(15))
    }));
}

#[tokio::test]
async fn aggregate_select_is_rejected_by_fast_path() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let stmts = server
        .parse_sql("SELECT count(*) AS total FROM users")
        .unwrap();
    let err = server
        .plan_statement(&default_session(), stmts.into_iter().next().unwrap())
        .await
        .unwrap_err();
    assert!(is_unsupported_error(&err));
}

#[tokio::test]
async fn join_query_routes_to_datafusion() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE b (id INT PRIMARY KEY, label TEXT)",
        "INSERT INTO a (id, name) VALUES (1, 'alice')",
        "INSERT INTO b (id, label) VALUES (1, 'x')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "SELECT a.name, b.label FROM a JOIN b ON a.id = b.id",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    match &responses[0] {
        Response::Query(_) => {}
        other => panic!("expected DataFusion query response, got {other:?}"),
    }
}

#[tokio::test]
async fn group_by_query_routes_to_datafusion() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE events (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO events (id, name) VALUES (1, 'a'), (2, 'a'), (3, 'b')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "SELECT name, count(*) AS total FROM events GROUP BY name ORDER BY name",
    )
    .await
    .unwrap();
    assert_eq!(responses.len(), 1);
    match &responses[0] {
        Response::Query(_) => {}
        other => panic!("expected DataFusion query response, got {other:?}"),
    }
}

#[tokio::test]
async fn insert_select_roundtrip_binary() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY, label TEXT, active BOOLEAN)",
        "INSERT INTO items (id, label, active) VALUES (1, 'foo', true)",
        "INSERT INTO items (id, label, active) VALUES (2, 'bar', false)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "items")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("label"), Some(&ColumnValue::Text("foo".into())));
    assert_eq!(row.get("active"), Some(&ColumnValue::Boolean(true)));

    let row2 = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row2.get("active"), Some(&ColumnValue::Boolean(false)));
}

#[tokio::test]
async fn update_row() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO t (id, val) VALUES (1, 'old')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    exec_sql(
        &server,
        &default_session(),
        "UPDATE t SET val = 'new' WHERE id = 1",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("val"), Some(&ColumnValue::Text("new".into())));
}

#[tokio::test]
async fn update_row_add_literal() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, score INT)",
        "INSERT INTO t (id, score) VALUES (1, 10)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    exec_sql(
        &server,
        &default_session(),
        "UPDATE t SET score = score + 5 WHERE id = 1",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("score"), Some(&ColumnValue::Int32(15)));
}

#[tokio::test]
async fn delete_row() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)",
        "INSERT INTO t (id, val) VALUES (1, 'a')",
        "INSERT INTO t (id, val) VALUES (2, 'b')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    exec_sql(&server, &default_session(), "DELETE FROM t WHERE id = 1").await;

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
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn insert_returning_fast_path() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let responses = exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO users (id, name) VALUES (1, 'alice') RETURNING id, name",
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
async fn insert_on_conflict_do_nothing_skips_duplicate() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

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
async fn insert_on_conflict_do_update_uses_excluded_values() {
    let server = GatewayServer::new(new_store());

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (id, name) VALUES (1, 'alice')",
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

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
async fn mysql_auto_increment_and_on_duplicate_key_update() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (name) VALUES ('alice')",
        "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.primary_key, "id");
    assert!(schema.find_column("id").unwrap().default.is_some());
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int64(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));

    let show_tables = exec_sql_for_client(&server, &mut client, "SHOW TABLES")
        .await
        .unwrap();
    assert!(matches!(show_tables.as_slice(), [Response::Query(_)]));
    let describe = exec_sql_for_client(&server, &mut client, "DESCRIBE users")
        .await
        .unwrap();
    assert!(matches!(describe.as_slice(), [Response::Query(_)]));
}

#[tokio::test]
async fn mysql_on_duplicate_key_update_uses_unique_targets() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
        "INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'alice')",
        "INSERT INTO users (id, email, name) VALUES (2, 'a@example.com', 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

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
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn mysql_on_duplicate_key_update_uses_unique_indexes() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT)",
        "CREATE UNIQUE INDEX users_email_idx ON users (email)",
        "INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'alice')",
        "INSERT INTO users (id, email, name) VALUES (2, 'a@example.com', 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

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
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("Alice".into())));
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int32(2))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn mysql_describe_table_reports_key_and_extra_metadata() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email TEXT UNIQUE, org_id INT, name TEXT, updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE INDEX users_org_idx ON users (org_id)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let rows = server
        .mysql_describe_table(DEFAULT_DATABASE_NAME, "users")
        .await
        .unwrap();
    let id = rows.iter().find(|row| row.field == "id").unwrap();
    assert_eq!(id.key, "PRI");
    assert_eq!(id.extra, "auto_increment");
    let email = rows.iter().find(|row| row.field == "email").unwrap();
    assert_eq!(email.key, "UNI");
    let org_id = rows.iter().find(|row| row.field == "org_id").unwrap();
    assert_eq!(org_id.key, "MUL");
    let updated_at = rows.iter().find(|row| row.field == "updated_at").unwrap();
    assert_eq!(updated_at.extra, "on update CURRENT_TIMESTAMP");
}

#[tokio::test]
async fn mysql_information_schema_uses_mysql_catalog_shape() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();

    for sql in [
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci UNIQUE, org_id INT, name TEXT, updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE INDEX users_org_idx ON users (org_id)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let columns = exec_sql_for_client(
            &server,
            &mut client,
            "SELECT table_schema, column_name, column_type, column_key, extra, character_set_name, collation_name FROM information_schema.columns WHERE table_schema = 'defaultdb' AND table_name = 'users' AND column_name = 'id'",
        )
        .await
        .unwrap();
    assert_eq!(
        response_field_names(&columns[0]),
        vec![
            "table_schema",
            "column_name",
            "column_type",
            "column_key",
            "extra",
            "character_set_name",
            "collation_name"
        ]
    );
    let rows = response_text_rows(columns.into_iter().next().unwrap()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some("defaultdb".to_string()));
    assert_eq!(rows[0][1], Some("id".to_string()));
    assert_eq!(rows[0][3], Some("PRI".to_string()));
    assert_eq!(rows[0][4], Some("auto_increment".to_string()));

    let columns = exec_sql_for_client(
            &server,
            &mut client,
            "SELECT column_name, extra, character_set_name, collation_name FROM information_schema.columns WHERE table_schema = 'defaultdb' AND table_name = 'users' AND column_name IN ('email', 'updated_at') ORDER BY column_name",
        )
        .await
        .unwrap();
    let rows = response_text_rows(columns.into_iter().next().unwrap()).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Some("email".to_string()));
    assert_eq!(rows[0][2], Some("latin1".to_string()));
    assert_eq!(rows[0][3], Some("latin1_swedish_ci".to_string()));
    assert_eq!(rows[1][0], Some("updated_at".to_string()));
    assert_eq!(rows[1][1], Some("on update CURRENT_TIMESTAMP".to_string()));

    let statistics = exec_sql_for_client(
            &server,
            &mut client,
            "SELECT index_name, non_unique, seq_in_index, column_name, index_type FROM information_schema.statistics WHERE table_schema = 'defaultdb' AND table_name = 'users' ORDER BY index_name, seq_in_index",
        )
        .await
        .unwrap();
    assert_eq!(
        response_field_names(&statistics[0]),
        vec![
            "index_name",
            "non_unique",
            "seq_in_index",
            "column_name",
            "index_type"
        ]
    );
    let rows = response_text_rows(statistics.into_iter().next().unwrap()).await;
    assert!(rows.iter().any(|row| row[0] == Some("PRIMARY".to_string())));
    assert!(
        rows.iter()
            .any(|row| row[0] == Some("users_org_idx".to_string())
                && row[1] == Some("1".to_string()))
    );

    for sql in [
        "SELECT table_name, engine, table_collation FROM information_schema.tables WHERE table_schema = 'defaultdb' AND table_name = 'users'",
        "SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata WHERE schema_name = 'defaultdb'",
        "SELECT engine, support, transactions FROM information_schema.engines",
        "SELECT character_set_name, default_collate_name FROM information_schema.character_sets",
        "SELECT collation_name, character_set_name FROM information_schema.collations",
    ] {
        let responses = exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
        assert!(
            matches!(responses.as_slice(), [Response::Query(_)]),
            "{sql}"
        );
        assert!(
            !response_text_rows(responses.into_iter().next().unwrap())
                .await
                .is_empty(),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn mysql_on_update_current_timestamp_and_text_limits() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    exec_sql(
            &server,
            &default_session(),
            "CREATE TABLE audit_log (id INT PRIMARY KEY, note TINYTEXT, name TEXT, updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP)",
        )
        .await;
    exec_sql(
            &server,
            &default_session(),
            "INSERT INTO audit_log (id, note, name, updated_at) VALUES (1, 'ok', 'alice', '2000-01-01 00:00:00')",
        )
        .await;
    exec_sql(
        &server,
        &default_session(),
        "UPDATE audit_log SET name = 'Alice' WHERE id = 1",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "audit_log")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        row.get("updated_at"),
        Some(&ColumnValue::Timestamp("2000-01-01 00:00:00".into()))
    );

    let too_long = "x".repeat(256);
    let sql = format!("INSERT INTO audit_log (id, note) VALUES (2, '{too_long}')");
    let stmts = server.parse_sql(&sql).unwrap();
    assert!(
        server
            .plan_statement(&default_session(), stmts.into_iter().next().unwrap())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn mysql_show_create_table_uses_catalog_metadata() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) UNIQUE, org_id INT, name TEXT)",
        "CREATE INDEX users_org_idx ON users (org_id)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let create = server
        .mysql_show_create_table(DEFAULT_DATABASE_NAME, "users")
        .await
        .unwrap();
    assert!(create.contains("CREATE TABLE `users`"));
    assert!(create.contains("`id` bigint NOT NULL AUTO_INCREMENT"));
    assert!(create.contains("PRIMARY KEY (`id`)"));
    assert!(create.contains("KEY `users_org_idx` (`org_id`)"));
    assert!(create.contains("DEFAULT CHARSET=utf8mb4"));
}

#[tokio::test]
async fn mysql_create_table_like_copies_schema_indexes_and_auto_increment_option() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE source_users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255), name TEXT, KEY source_email_idx (email)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE copied_users LIKE source_users AUTO_INCREMENT = 10 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
        "INSERT INTO copied_users (email, name) VALUES ('a@example.com', 'Alice')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "copied_users")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.find_column("email").unwrap().data_type,
        DataType::VarChar(Some(255))
    );
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int64(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.get("email"),
        Some(&ColumnValue::Text("a@example.com".into()))
    );
    let indexes = server
        .catalog
        .list_indexes_for_table(schema.table_id)
        .await
        .unwrap();
    assert!(
        indexes
            .iter()
            .any(|index| index.column_names == vec!["email".to_string()])
    );
}

#[tokio::test]
async fn mysql_alter_table_common_ddl_operations_update_catalog() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE parent_orgs (id INT PRIMARY KEY)",
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(32), parent_id INT, CONSTRAINT users_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_orgs(id), KEY email_idx (email))",
        "ALTER TABLE users MODIFY COLUMN email VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL, CHANGE COLUMN parent_id org_id INT, DROP FOREIGN KEY users_parent_fk, DROP INDEX email_idx, ADD INDEX org_idx (org_id), AUTO_INCREMENT = 42, ALGORITHM=INPLACE, LOCK=NONE",
        "INSERT INTO users (email, org_id) VALUES ('a@example.com', 7)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    let email = schema.find_column("email").unwrap();
    assert_eq!(email.data_type, DataType::VarChar(Some(64)));
    assert!(!email.nullable);
    assert_eq!(email.character_set.as_deref(), Some("latin1"));
    assert_eq!(email.collation.as_deref(), Some("latin1_swedish_ci"));
    assert!(schema.find_column("parent_id").is_none());
    assert!(schema.find_column("org_id").is_some());
    assert!(schema.foreign_keys.is_empty());
    let indexes = server
        .catalog
        .list_indexes_for_table(schema.table_id)
        .await
        .unwrap();
    assert!(indexes.iter().any(|index| index.index_name == "org_idx"));
    assert!(!indexes.iter().any(|index| index.index_name == "email_idx"));
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int64(42))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("org_id"), Some(&ColumnValue::Int32(7)));
}

#[tokio::test]
async fn mysql_insert_set_ignore_and_replace_into_use_unique_conflicts() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(255) UNIQUE, name TEXT)",
        "INSERT INTO users SET email = 'a@example.com', name = 'Alice'",
        "INSERT IGNORE INTO users (id, email, name) VALUES (2, 'a@example.com', 'Ignored')",
        "REPLACE INTO users SET id = 3, email = 'a@example.com', name = 'Replaced'",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "users")
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int64(1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        server
            .read_visible_row(None, &schema, &ColumnValue::Int64(2))
            .await
            .unwrap()
            .is_none()
    );
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int64(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.get("email"),
        Some(&ColumnValue::Text("a@example.com".into()))
    );
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("Replaced".into())));
}

#[tokio::test]
async fn mysql_update_delete_order_by_limit_apply_to_target_rows() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);

    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY, label TEXT)",
        "INSERT INTO items (id, label) VALUES (1, 'old'), (2, 'old'), (3, 'old'), (4, 'old')",
        "UPDATE items SET label = 'new' ORDER BY id LIMIT 2",
        "DELETE FROM items WHERE label = 'new' ORDER BY id LIMIT 1",
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
    let row2 = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(2))
        .await
        .unwrap()
        .unwrap();
    let row3 = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(3))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row2.get("label"), Some(&ColumnValue::Text("new".into())));
    assert_eq!(row3.get("label"), Some(&ColumnValue::Text("old".into())));
}

#[tokio::test]
async fn mysql_type_system_enforces_p0_type_limits() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();

    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE mysql_types (
                id INT PRIMARY KEY,
                tiny TINYINT,
                tinyu TINYINT UNSIGNED,
                med MEDIUMINT,
                intu INT UNSIGNED,
                bigu BIGINT UNSIGNED,
                bits BIT(4),
                y YEAR,
                amount DECIMAL(5,2),
                bin BINARY(4),
                vbin VARBINARY(4),
                status ENUM('new','paid'),
                flags SET('red','blue'),
                happened DATETIME(3)
            )",
    )
    .await;

    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO mysql_types
             (id, tiny, tinyu, med, intu, bigu, bits, y, amount, bin, vbin, status, flags, happened)
             VALUES
             (1, -128, 255, 8388607, 4294967295, 18446744073709551615,
              15, 2026, '123.45', 'ab', 'abcd', 'paid', 'red,blue',
              '2026-06-13 12:34:56.123')",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "mysql_types")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.find_column("tiny").unwrap().data_type,
        DataType::MySqlInt {
            kind: crate::types::MySqlIntKind::Tiny,
            unsigned: false,
        }
    );
    assert_eq!(
        schema.find_column("amount").unwrap().data_type,
        DataType::Numeric {
            precision: Some(5),
            scale: Some(2),
        }
    );
    assert_eq!(
        schema.find_column("happened").unwrap().data_type,
        DataType::MySqlDateTime { fsp: Some(3) }
    );
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("tiny"), Some(&ColumnValue::Int16(-128)));
    assert_eq!(row.get("tinyu"), Some(&ColumnValue::Int32(255)));
    assert_eq!(row.get("intu"), Some(&ColumnValue::Int64(4_294_967_295)));
    assert_eq!(
        row.get("bigu"),
        Some(&ColumnValue::Numeric("18446744073709551615".to_string()))
    );
    assert_eq!(
        row.get("bin"),
        Some(&ColumnValue::Bytea(vec![b'a', b'b', 0, 0]))
    );

    for sql in [
        "INSERT INTO mysql_types (id, tinyu) VALUES (2, -1)",
        "INSERT INTO mysql_types (id, amount) VALUES (3, '1234.56')",
        "INSERT INTO mysql_types (id, vbin) VALUES (4, 'abcde')",
        "INSERT INTO mysql_types (id, status) VALUES (5, 'bad')",
        "INSERT INTO mysql_types (id, happened) VALUES (6, '0000-00-00 00:00:00')",
        "INSERT INTO mysql_types (id, happened) VALUES (7, '2026-06-13 12:34:56.1234')",
    ] {
        assert!(
            exec_sql_for_client(&server, &mut client, sql)
                .await
                .is_err(),
            "{sql}"
        );
    }
}

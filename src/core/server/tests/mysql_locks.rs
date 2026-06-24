use super::super::shared::MySqlLockKind;
use super::{
    ClientInfo, ColumnValue, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, GatewayMode,
    GatewayServer, QueryPlan, ReadAccess, SecretKey, TestClient, arrow_array_value_to_string,
    create_small_pgbench_schema, default_session, exec_pgbench_transaction, exec_sql_for_client,
    new_store, plan_sql, response_text_rows,
};

#[tokio::test]
async fn pgbench_style_rollback_and_savepoint_visibility() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    create_small_pgbench_schema(&server, &mut client).await;

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE pgbench_accounts SET abalance = abalance + 10 WHERE aid = 1",
    )
    .await
    .unwrap();
    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "pgbench_accounts",
        )
        .await
        .unwrap()
        .unwrap();
    let in_tx = server
        .read_visible_row(
            Some(client.pid_and_secret_key().0),
            &schema,
            &ColumnValue::Int32(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(in_tx.get("abalance"), Some(&ColumnValue::Int32(10)));
    let outside_tx = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outside_tx.get("abalance"), Some(&ColumnValue::Int32(0)));
    exec_sql_for_client(&server, &mut client, "ROLLBACK")
        .await
        .unwrap();
    let rolled_back = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rolled_back.get("abalance"), Some(&ColumnValue::Int32(0)));

    exec_sql_for_client(&server, &mut client, "BEGIN")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE pgbench_accounts SET abalance = abalance + 5 WHERE aid = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "SAVEPOINT pgbench_sp")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE pgbench_accounts SET abalance = abalance + 7 WHERE aid = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(
            &server,
            &mut client,
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime, filler) VALUES (1, 1, 1, 7, '2026-05-19 00:00:00', '')",
        )
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut client, "ROLLBACK TO SAVEPOINT pgbench_sp")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();

    let committed = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.get("abalance"), Some(&ColumnValue::Int32(5)));
    let history = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "pgbench_history",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        server
            .scan_visible_rows(None, &history, None)
            .await
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn mysql_autocommit_zero_reopens_transaction_after_commit_and_rollback() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE mysql_tx (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "INSERT INTO mysql_tx VALUES (1, 1)")
        .await
        .unwrap();

    exec_sql_for_client(&server, &mut client, "SET autocommit = 0")
        .await
        .unwrap();
    assert!(server.has_active_transaction(&client).await);

    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE mysql_tx SET val = 2 WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();
    assert!(server.has_active_transaction(&client).await);

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "mysql_tx")
        .await
        .unwrap()
        .unwrap();
    let committed = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.get("val"), Some(&ColumnValue::Int32(2)));

    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE mysql_tx SET val = 3 WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "ROLLBACK")
        .await
        .unwrap();
    assert!(server.has_active_transaction(&client).await);

    let rolled_back = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rolled_back.get("val"), Some(&ColumnValue::Int32(2)));

    exec_sql_for_client(&server, &mut client, "SET autocommit = 1")
        .await
        .unwrap();
    assert!(!server.has_active_transaction(&client).await);
}

#[tokio::test]
async fn mysql_ddl_implicitly_commits_active_transaction_and_restarts_autocommit_zero() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE mysql_ddl_tx (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO mysql_ddl_tx VALUES (1, 10)",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut client, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE mysql_ddl_tx SET val = 11 WHERE id = 1",
    )
    .await
    .unwrap();

    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE mysql_ddl_marker (id INT PRIMARY KEY)",
    )
    .await
    .unwrap();
    assert!(server.has_active_transaction(&client).await);

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "mysql_ddl_tx")
        .await
        .unwrap()
        .unwrap();
    let committed_by_ddl = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed_by_ddl.get("val"), Some(&ColumnValue::Int32(11)));

    exec_sql_for_client(&server, &mut client, "ROLLBACK")
        .await
        .unwrap();
    let after_rollback = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_rollback.get("val"), Some(&ColumnValue::Int32(11)));
}

#[tokio::test]
async fn mysql_start_transaction_read_only_accepts_snapshot_clause_and_blocks_writes() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE mysql_read_only_tx (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO mysql_read_only_tx VALUES (1, 1)",
    )
    .await
    .unwrap();

    exec_sql_for_client(
        &server,
        &mut client,
        "START TRANSACTION READ ONLY WITH CONSISTENT SNAPSHOT",
    )
    .await
    .unwrap();
    assert!(server.has_active_transaction(&client).await);
    assert!(server.is_active_transaction_read_only(&client).await);

    let err = exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE mysql_read_only_tx SET val = 2 WHERE id = 1",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("read-only transaction"));

    exec_sql_for_client(&server, &mut client, "ROLLBACK")
        .await
        .unwrap();
    assert!(!server.has_active_transaction(&client).await);
}

#[tokio::test]
async fn mysql_locking_read_holds_table_lock_until_transaction_end() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut locker,
        "CREATE TABLE mysql_lock_tx (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "INSERT INTO mysql_lock_tx VALUES (1, 1)",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_lock_tx WHERE id = 1 FOR UPDATE",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    let waiter = async {
        exec_sql_for_client(
            &server,
            &mut writer,
            "UPDATE mysql_lock_tx SET val = 2 WHERE id = 1",
        )
        .await
    };
    let releaser = async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        exec_sql_for_client(&server, &mut locker, "COMMIT")
            .await
            .unwrap();
    };
    let (wait_result, _) = tokio::join!(waiter, releaser);
    wait_result.unwrap();
    exec_sql_for_client(&server, &mut writer, "COMMIT")
        .await
        .unwrap();
}

#[tokio::test]
async fn mysql_row_locks_allow_different_primary_keys() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut first = TestClient::default();
    let mut second = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut first,
        "CREATE TABLE mysql_row_locks (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut first,
        "INSERT INTO mysql_row_locks VALUES (1, 1), (2, 2)",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut first, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut second, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut first,
        "UPDATE mysql_row_locks SET val = 10 WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut second,
        "UPDATE mysql_row_locks SET val = 20 WHERE id = 2",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn mysql_range_lock_blocks_insert_inside_range_only() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut locker,
        "CREATE TABLE mysql_range_locks (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "INSERT INTO mysql_range_locks VALUES (1, 1), (5, 5)",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_range_locks WHERE id >= 1 AND id < 5 FOR UPDATE",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();

    let err = exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_range_locks VALUES (3, 3)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));

    exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_range_locks VALUES (9, 9)",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn mysql_secondary_index_locking_read_locks_matching_rows_not_whole_table() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_sec_locks (id INT PRIMARY KEY, status TEXT, val INT)",
        "CREATE INDEX mysql_sec_locks_status_idx ON mysql_sec_locks (status)",
        "INSERT INTO mysql_sec_locks VALUES (1, 'held', 1), (2, 'free', 2)",
    ] {
        exec_sql_for_client(&server, &mut locker, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_sec_locks WHERE status = 'held' FOR UPDATE",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut writer,
        "UPDATE mysql_sec_locks SET val = 20 WHERE id = 2",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    let err = exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_sec_locks VALUES (3, 'held', 3)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));
}

#[tokio::test]
async fn mysql_secondary_index_gap_lock_blocks_insert_for_missing_key() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_sec_gap_locks (id INT PRIMARY KEY, status TEXT)",
        "CREATE INDEX mysql_sec_gap_locks_status_idx ON mysql_sec_gap_locks (status)",
        "INSERT INTO mysql_sec_gap_locks VALUES (1, 'present')",
    ] {
        exec_sql_for_client(&server, &mut locker, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_sec_gap_locks WHERE status = 'missing' FOR UPDATE",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    let err = exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_sec_gap_locks VALUES (2, 'missing')",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));
}

#[tokio::test]
async fn mysql_primary_key_missing_lock_uses_gap_and_blocks_insert() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_pk_gap_locks (id INT PRIMARY KEY, val INT)",
        "INSERT INTO mysql_pk_gap_locks VALUES (1, 1)",
    ] {
        exec_sql_for_client(&server, &mut locker, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_pk_gap_locks WHERE id = 2 FOR UPDATE",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    let err = exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_pk_gap_locks VALUES (2, 2)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));
}

#[tokio::test]
async fn mysql_secondary_index_range_next_key_lock_blocks_inside_range_only() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_sec_range_locks (id INT PRIMARY KEY, score INT)",
        "CREATE INDEX mysql_sec_range_locks_score_idx ON mysql_sec_range_locks (score)",
        "INSERT INTO mysql_sec_range_locks VALUES (1, 10), (2, 20), (3, 40)",
    ] {
        exec_sql_for_client(&server, &mut locker, sql)
            .await
            .unwrap();
    }

    let plan = plan_sql(
        &server,
        &server.session_catalog(&locker),
        "SELECT * FROM mysql_sec_range_locks WHERE score >= 10 AND score < 30",
    )
    .await;
    let QueryPlan::SelectRows { access, .. } = plan else {
        panic!("expected select rows");
    };
    assert!(matches!(access, ReadAccess::SecondaryIndexRangeScan { .. }));

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_sec_range_locks WHERE score >= 10 AND score < 30 FOR UPDATE",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut writer,
        "UPDATE mysql_sec_range_locks SET score = 41 WHERE id = 3",
    )
    .await
    .unwrap();
    let err = exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_sec_range_locks VALUES (4, 20)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));
    exec_sql_for_client(
        &server,
        &mut writer,
        "INSERT INTO mysql_sec_range_locks VALUES (5, 35)",
    )
    .await
    .unwrap();
}

#[test]
fn mysql_metadata_lock_scope_hierarchy_conflicts() {
    let table_read =
        GatewayServer::mysql_lock(1, "appdb", "public", "orders", MySqlLockKind::MetadataRead);
    let same_db_write =
        GatewayServer::mysql_database_lock(2, "appdb", MySqlLockKind::MetadataWrite);
    let other_db_write =
        GatewayServer::mysql_database_lock(2, "otherdb", MySqlLockKind::MetadataWrite);
    assert!(GatewayServer::mysql_locks_conflict(
        &same_db_write,
        &table_read
    ));
    assert!(!GatewayServer::mysql_locks_conflict(
        &other_db_write,
        &table_read
    ));

    let same_schema_write =
        GatewayServer::mysql_schema_lock(2, "appdb", "public", MySqlLockKind::MetadataWrite);
    let other_schema_write =
        GatewayServer::mysql_schema_lock(2, "appdb", "archive", MySqlLockKind::MetadataWrite);
    assert!(GatewayServer::mysql_locks_conflict(
        &same_schema_write,
        &table_read
    ));
    assert!(!GatewayServer::mysql_locks_conflict(
        &other_schema_write,
        &table_read
    ));

    let other_table_read = GatewayServer::mysql_lock(
        2,
        "appdb",
        "public",
        "customers",
        MySqlLockKind::MetadataRead,
    );
    let table_write =
        GatewayServer::mysql_lock(3, "appdb", "public", "orders", MySqlLockKind::MetadataWrite);
    assert!(GatewayServer::mysql_locks_conflict(
        &table_write,
        &table_read
    ));
    assert!(!GatewayServer::mysql_locks_conflict(
        &table_write,
        &other_table_read
    ));
    assert!(!GatewayServer::mysql_locks_conflict(
        &table_read,
        &other_table_read
    ));
}

#[test]
fn mysql_insert_intention_locks_do_not_conflict_with_each_other() {
    let first = GatewayServer::mysql_lock(
        1,
        "appdb",
        "public",
        "orders",
        MySqlLockKind::InsertIntention("Int32(7)".to_string()),
    );
    let second = GatewayServer::mysql_lock(
        2,
        "appdb",
        "public",
        "orders",
        MySqlLockKind::InsertIntention("Int32(7)".to_string()),
    );
    let gap = GatewayServer::mysql_lock(
        3,
        "appdb",
        "public",
        "orders",
        MySqlLockKind::Gap {
            lower: Some("Int32(7)".to_string()),
            upper: Some("Int32(8)".to_string()),
        },
    );
    assert!(!GatewayServer::mysql_locks_conflict(&first, &second));
    assert!(GatewayServer::mysql_locks_conflict(&gap, &first));
}

#[tokio::test]
async fn mysql_metadata_lock_conflicts_with_active_row_lock() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut ddl = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut locker,
        "CREATE TABLE mysql_metadata_locks (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "INSERT INTO mysql_metadata_locks VALUES (1, 1)",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut ddl, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "UPDATE mysql_metadata_locks SET val = 2 WHERE id = 1",
    )
    .await
    .unwrap();

    let err = exec_sql_for_client(
        &server,
        &mut ddl,
        "CREATE INDEX mysql_metadata_locks_val_idx ON mysql_metadata_locks (val)",
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("Lock wait timeout exceeded"));
}

#[tokio::test]
async fn mysql_metadata_write_waits_for_metadata_read_and_wakes() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut reader = TestClient::default();
    let mut ddl = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut reader,
        "CREATE TABLE mysql_metadata_wake (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut reader,
        "INSERT INTO mysql_metadata_wake VALUES (1, 1)",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut reader, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut reader,
        "UPDATE mysql_metadata_wake SET val = 2 WHERE id = 1",
    )
    .await
    .unwrap();

    let waiter = async {
        exec_sql_for_client(
            &server,
            &mut ddl,
            "CREATE INDEX mysql_metadata_wake_val_idx ON mysql_metadata_wake (val)",
        )
        .await
    };
    let releaser = async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        exec_sql_for_client(&server, &mut reader, "COMMIT")
            .await
            .unwrap();
    };
    let (ddl_result, _) = tokio::join!(waiter, releaser);
    ddl_result.unwrap();
}

#[tokio::test]
async fn mysql_metadata_write_wait_queue_blocks_later_metadata_reads() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut holder = TestClient::default();
    let mut ddl = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    let mut later_reader = TestClient {
        pid_secret_key: (3, SecretKey::I32(126)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_mdl_fairness (id INT PRIMARY KEY, val INT)",
        "INSERT INTO mysql_mdl_fairness VALUES (1, 1), (2, 2)",
    ] {
        exec_sql_for_client(&server, &mut holder, sql)
            .await
            .unwrap();
    }
    exec_sql_for_client(&server, &mut holder, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut holder,
        "UPDATE mysql_mdl_fairness SET val = 10 WHERE id = 1",
    )
    .await
    .unwrap();

    let ddl_waiter = async {
        exec_sql_for_client(
            &server,
            &mut ddl,
            "CREATE INDEX mysql_mdl_fairness_val_idx ON mysql_mdl_fairness (val)",
        )
        .await
    };
    let later_read = async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        exec_sql_for_client(&server, &mut later_reader, "SET autocommit = 0")
            .await
            .unwrap();
        exec_sql_for_client(
            &server,
            &mut later_reader,
            "SET innodb_lock_wait_timeout = 1",
        )
        .await
        .unwrap();
        let err = exec_sql_for_client(
            &server,
            &mut later_reader,
            "SELECT * FROM mysql_mdl_fairness WHERE id = 2 FOR UPDATE",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("Lock wait timeout exceeded"));
        exec_sql_for_client(&server, &mut holder, "COMMIT")
            .await
            .unwrap();
    };
    let (ddl_result, _) = tokio::join!(ddl_waiter, later_read);
    ddl_result.unwrap();
}

#[tokio::test]
async fn mysql_kill_connection_releases_locks_and_wakes_waiters() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut locker = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    let mut killer = TestClient {
        pid_secret_key: (3, SecretKey::I32(126)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_kill_locks (id INT PRIMARY KEY, val INT)",
        "INSERT INTO mysql_kill_locks VALUES (1, 1)",
    ] {
        exec_sql_for_client(&server, &mut locker, sql)
            .await
            .unwrap();
    }

    exec_sql_for_client(&server, &mut locker, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut locker,
        "SELECT * FROM mysql_kill_locks WHERE id = 1 FOR UPDATE",
    )
    .await
    .unwrap();
    exec_sql_for_client(&server, &mut writer, "SET autocommit = 0")
        .await
        .unwrap();
    let target = locker.pid_and_secret_key().0;
    let waiter = async {
        exec_sql_for_client(
            &server,
            &mut writer,
            "UPDATE mysql_kill_locks SET val = 2 WHERE id = 1",
        )
        .await
    };
    let releaser = async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        exec_sql_for_client(&server, &mut killer, &format!("KILL CONNECTION {target}"))
            .await
            .unwrap();
    };
    let (wait_result, _) = tokio::join!(waiter, releaser);
    wait_result.unwrap();
    exec_sql_for_client(&server, &mut writer, "COMMIT")
        .await
        .unwrap();

    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "mysql_kill_locks",
        )
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("val"), Some(&ColumnValue::Int32(2)));
}

#[tokio::test]
async fn mysql_repeatable_read_select_uses_snapshot_but_locking_read_uses_current_read() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut reader = TestClient::default();
    let mut writer = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    for sql in [
        "CREATE TABLE mysql_consistent_reads (id INT PRIMARY KEY, val INT)",
        "INSERT INTO mysql_consistent_reads VALUES (1, 10)",
    ] {
        exec_sql_for_client(&server, &mut reader, sql)
            .await
            .unwrap();
    }
    exec_sql_for_client(&server, &mut reader, "SET autocommit = 0")
        .await
        .unwrap();
    let first = exec_sql_for_client(
        &server,
        &mut reader,
        "SELECT val FROM mysql_consistent_reads WHERE id = 1",
    )
    .await
    .unwrap()
    .remove(0);
    assert_eq!(
        response_text_rows(first).await,
        vec![vec![Some("10".to_string())]]
    );

    exec_sql_for_client(
        &server,
        &mut writer,
        "UPDATE mysql_consistent_reads SET val = 20 WHERE id = 1",
    )
    .await
    .unwrap();
    let consistent = exec_sql_for_client(
        &server,
        &mut reader,
        "SELECT val FROM mysql_consistent_reads WHERE id = 1",
    )
    .await
    .unwrap()
    .remove(0);
    assert_eq!(
        response_text_rows(consistent).await,
        vec![vec![Some("10".to_string())]]
    );

    let current = exec_sql_for_client(
        &server,
        &mut reader,
        "SELECT val FROM mysql_consistent_reads WHERE id = 1 FOR UPDATE",
    )
    .await
    .unwrap()
    .remove(0);
    assert_eq!(
        response_text_rows(current).await,
        vec![vec![Some("20".to_string())]]
    );
}

#[tokio::test]
async fn mysql_lock_manager_detects_simple_deadlock_and_uses_timeout_setting() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut first = TestClient::default();
    let mut second = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut first,
        "CREATE TABLE mysql_deadlocks (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut first,
        "INSERT INTO mysql_deadlocks VALUES (1, 1), (2, 2)",
    )
    .await
    .unwrap();

    exec_sql_for_client(&server, &mut first, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut second, "SET autocommit = 0")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut first, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    exec_sql_for_client(&server, &mut second, "SET innodb_lock_wait_timeout = 1")
        .await
        .unwrap();
    exec_sql_for_client(
        &server,
        &mut first,
        "UPDATE mysql_deadlocks SET val = 10 WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut second,
        "UPDATE mysql_deadlocks SET val = 20 WHERE id = 2",
    )
    .await
    .unwrap();

    let first_wait = async {
        exec_sql_for_client(
            &server,
            &mut first,
            "UPDATE mysql_deadlocks SET val = 11 WHERE id = 2",
        )
        .await
    };
    let second_wait = async {
        exec_sql_for_client(
            &server,
            &mut second,
            "UPDATE mysql_deadlocks SET val = 21 WHERE id = 1",
        )
        .await
    };
    let (first_result, second_result) = tokio::join!(first_wait, second_wait);
    let errors = [
        first_result.unwrap_err().to_string(),
        second_result.unwrap_err().to_string(),
    ];
    assert!(errors.iter().any(|error| error.contains("Deadlock found")));
}

#[tokio::test]
async fn mysql_lock_manager_detects_three_session_deadlock_cycle() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    let mut first = TestClient::default();
    let mut second = TestClient {
        pid_secret_key: (2, SecretKey::I32(84)),
        ..TestClient::default()
    };
    let mut third = TestClient {
        pid_secret_key: (3, SecretKey::I32(126)),
        ..TestClient::default()
    };
    exec_sql_for_client(
        &server,
        &mut first,
        "CREATE TABLE mysql_three_deadlocks (id INT PRIMARY KEY, val INT)",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut first,
        "INSERT INTO mysql_three_deadlocks VALUES (1, 1), (2, 2), (3, 3)",
    )
    .await
    .unwrap();
    for client in [&mut first, &mut second, &mut third] {
        exec_sql_for_client(&server, client, "SET autocommit = 0")
            .await
            .unwrap();
        exec_sql_for_client(&server, client, "SET innodb_lock_wait_timeout = 1")
            .await
            .unwrap();
    }
    exec_sql_for_client(
        &server,
        &mut first,
        "UPDATE mysql_three_deadlocks SET val = 10 WHERE id = 1",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut second,
        "UPDATE mysql_three_deadlocks SET val = 20 WHERE id = 2",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut third,
        "UPDATE mysql_three_deadlocks SET val = 30 WHERE id = 3",
    )
    .await
    .unwrap();

    let first_wait = async {
        exec_sql_for_client(
            &server,
            &mut first,
            "UPDATE mysql_three_deadlocks SET val = 11 WHERE id = 2",
        )
        .await
    };
    let second_wait = async {
        exec_sql_for_client(
            &server,
            &mut second,
            "UPDATE mysql_three_deadlocks SET val = 21 WHERE id = 3",
        )
        .await
    };
    let third_wait = async {
        exec_sql_for_client(
            &server,
            &mut third,
            "UPDATE mysql_three_deadlocks SET val = 31 WHERE id = 1",
        )
        .await
    };
    let (first_result, second_result, third_result) =
        tokio::join!(first_wait, second_wait, third_wait);
    let errors = [
        first_result.unwrap_err().to_string(),
        second_result.unwrap_err().to_string(),
        third_result.unwrap_err().to_string(),
    ];
    assert!(errors.iter().any(|error| error.contains("Deadlock found")));
}

#[tokio::test]
async fn pgbench_consistency_aggregate_query_matches_history_delta() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    create_small_pgbench_schema(&server, &mut client).await;

    exec_pgbench_transaction(&server, &mut client, 10).await;
    exec_pgbench_transaction(&server, &mut client, -3).await;

    let batches = server
        .collect_datafusion_batches(
            "SELECT \
                    (SELECT count(*) FROM pgbench_history) AS history_count, \
                    (SELECT sum(abalance) FROM pgbench_accounts) AS account_sum, \
                    (SELECT sum(tbalance) FROM pgbench_tellers) AS teller_sum, \
                    (SELECT sum(bbalance) FROM pgbench_branches) AS branch_sum, \
                    (SELECT sum(delta) FROM pgbench_history) AS history_sum",
            &default_session(),
        )
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("consistency query should return one row");
    assert_eq!(arrow_array_value_to_string(batch.column(0), 0), "2");
    assert_eq!(arrow_array_value_to_string(batch.column(1), 0), "7");
    assert_eq!(arrow_array_value_to_string(batch.column(2), 0), "7");
    assert_eq!(arrow_array_value_to_string(batch.column(3), 0), "7");
    assert_eq!(arrow_array_value_to_string(batch.column(4), 0), "7");
}

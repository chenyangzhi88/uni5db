use super::{
    ClientInfo, ColumnValue, CommandComplete, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DataType,
    FieldFormat, GatewayMode, GatewayServer, METADATA_USER, Response, Statement, TestClient,
    arrow_array_value_to_string, default_session, exec_sql, exec_sql_for_client, new_store,
    plan_sql, response_field_names, response_text_rows,
};
use crate::types::INTERNAL_ROWID_COLUMN;

#[tokio::test]
async fn psql_list_databases_query_returns_catalog_rows() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "coco".to_string());
    exec_sql(&server, &default_session(), "CREATE DATABASE appdb").await;

    let sql = "SELECT d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", \
pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", d.datcollate as \"Collate\", \
d.datctype as \"Ctype\", d.daticulocale as \"ICU Locale\", CASE d.datlocprovider WHEN 'c' THEN 'libc' \
WHEN 'i' THEN 'icu' END AS \"Locale Provider\", pg_catalog.array_to_string(d.datacl, E'\\n') AS \"Access privileges\" \
FROM pg_catalog.pg_database d ORDER BY 1;";
    let response = server.catalog_query_response(&client, sql).await;
    assert!(response.is_some());
    assert!(response.unwrap().is_ok());
}

#[tokio::test]
async fn datafusion_exposes_pg_tables_view() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let result = server
        .execute_via_datafusion(
            "SELECT schemaname, tablename FROM pg_catalog.pg_tables ORDER BY schemaname, tablename",
            &default_session(),
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn datafusion_exposes_postgres_catalog_introspection_tables() {
    let server = GatewayServer::new(new_store());
    let session = default_session();
    exec_sql(
        &server,
        &session,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL, active BOOLEAN)",
    )
    .await;
    exec_sql(
        &server,
        &session,
        "CREATE UNIQUE INDEX users_email_idx ON users(email)",
    )
    .await;

    for sql in [
        "SELECT relname, relkind FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT reltype, relam, relhasindex, relpersistence, relnatts, relchecks, relhasrules, relhastriggers, relrowsecurity, reltuples FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT attrelid, attname, atttypid, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum",
        "SELECT attlen, attbyval, attalign, attstorage, atthasdef, attidentity, attgenerated, attcollation FROM pg_catalog.pg_attribute ORDER BY attrelid, attnum",
        "SELECT typname, typtype, typrelid FROM pg_catalog.pg_type WHERE typname IN ('bool', 'int4', 'int8', 'text', 'unknown', 'record', 'users') ORDER BY typname",
        "SELECT indexrelid, indrelid, indisunique, indkey FROM pg_catalog.pg_index",
        "SELECT indisexclusion, indimmediate, indisclustered, indcheckxmin, indislive, indisreplident, indcollation, indclass, indoption, indexprs, indpred FROM pg_catalog.pg_index",
        "SELECT conname, contype, conrelid, conindid, conkey FROM pg_catalog.pg_constraint ORDER BY conname",
        "SELECT contypid, conparentid, confupdtype, confdeltype, confmatchtype, conislocal, coninhcount, connoinherit, conpfeqop, conppeqop, conffeqop, conexclop, conbin FROM pg_catalog.pg_constraint",
        "SELECT tablename, hasindexes FROM pg_catalog.pg_tables WHERE tablename = 'users'",
        "SELECT rolname, rolsuper FROM pg_catalog.pg_roles",
        "SELECT usename, usesuper FROM pg_catalog.pg_user",
        "SELECT amname, amtype FROM pg_catalog.pg_am ORDER BY amname",
        "SELECT opcname, opcintype FROM pg_catalog.pg_opclass ORDER BY opcname",
        "SELECT collname, collcollate FROM pg_catalog.pg_collation",
        "SELECT proname, prorettype, pronargs FROM pg_catalog.pg_proc WHERE proname IN ('format_type', 'current_setting', 'has_table_privilege') ORDER BY proname",
        "SELECT castsource, casttarget, castcontext FROM pg_catalog.pg_cast ORDER BY castsource, casttarget",
        "SELECT name, setting, vartype FROM pg_catalog.pg_settings WHERE name IN ('server_version_num', 'standard_conforming_strings') ORDER BY name",
        "SELECT current_setting('server_version_num')",
        "SELECT pg_table_is_visible(oid) FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT has_table_privilege(oid, 'SELECT') FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT * FROM pg_catalog.pg_attrdef",
        "SELECT * FROM pg_catalog.pg_depend",
        "SELECT * FROM pg_catalog.pg_description",
        "SELECT * FROM pg_catalog.pg_shdepend",
        "SELECT * FROM pg_catalog.pg_shdescription",
        "SELECT * FROM pg_catalog.pg_sequence",
        "SELECT * FROM pg_catalog.pg_sequences",
        "SELECT * FROM pg_catalog.pg_rewrite",
        "SELECT * FROM pg_catalog.pg_views",
        "SELECT * FROM pg_catalog.pg_trigger",
        "SELECT * FROM pg_catalog.pg_policy",
        "SELECT * FROM pg_catalog.pg_statistic",
        "SELECT * FROM pg_catalog.pg_stats",
        "SELECT * FROM pg_catalog.pg_statistic_ext",
        "SELECT * FROM pg_catalog.pg_stats_ext",
        "SELECT pg_get_userbyid(relowner) FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_schema, table_name",
        "SELECT table_name, column_name, ordinal_position, is_nullable, data_type, udt_name FROM information_schema.columns ORDER BY table_name, ordinal_position",
        "SELECT is_identity, is_generated, is_updatable FROM information_schema.columns ORDER BY table_name, ordinal_position",
        "SELECT schema_name, schema_owner FROM information_schema.schemata ORDER BY schema_name",
        "SELECT constraint_name, constraint_type FROM information_schema.table_constraints ORDER BY constraint_name",
        "SELECT constraint_name, column_name FROM information_schema.key_column_usage ORDER BY constraint_name",
        "SELECT constraint_name, column_name FROM information_schema.constraint_column_usage ORDER BY constraint_name",
        "SELECT constraint_name, table_name FROM information_schema.constraint_table_usage ORDER BY constraint_name",
        "SELECT table_name, privilege_type FROM information_schema.table_privileges ORDER BY table_name, privilege_type",
        "SELECT table_name, column_name, privilege_type FROM information_schema.column_privileges ORDER BY table_name, column_name, privilege_type",
        "SELECT * FROM information_schema.views",
        "SELECT * FROM information_schema.sequences",
        "SELECT * FROM information_schema.routines",
        "SELECT * FROM information_schema.parameters",
    ] {
        let result = server.execute_via_datafusion(sql, &session).await;
        if let Err(err) = result {
            panic!("{sql}: {err:?}");
        }
    }

    let mut client = TestClient::default();
    for sql in [
        "SELECT relname FROM pg_catalog.pg_class ORDER BY relname",
        "SELECT table_name FROM information_schema.tables ORDER BY table_name",
    ] {
        let result = exec_sql_for_client(&server, &mut client, sql).await;
        if let Err(err) = result {
            panic!("{sql}: {err:?}");
        }
    }
}

#[tokio::test]
async fn psql_list_tables_query_returns_catalog_rows() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "coco".to_string());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    let sql = "SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' \
WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' \
WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\", \
pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
WHERE c.relkind IN ('r','p','') AND n.nspname <> 'pg_catalog' AND n.nspname !~ '^pg_toast' AND n.nspname <> 'information_schema' \
ORDER BY 1,2;";
    let response = server.catalog_query_response(&client, sql).await;
    assert!(response.is_some());
    assert!(response.unwrap().is_ok());
}

#[tokio::test]
async fn drop_table_if_exists_supports_multiple_tables() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE TABLE b (id INT PRIMARY KEY)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let stmts = server.parse_sql("DROP TABLE IF EXISTS a, b, c").unwrap();
    let plan = server
        .plan_statement(&default_session(), stmts.into_iter().next().unwrap())
        .await
        .unwrap();
    server
        .execute_plan(plan, None, FieldFormat::Text)
        .await
        .unwrap();

    assert!(
        server
            .catalog
            .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "a")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        server
            .catalog
            .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "b")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn parse_multi_table_truncate_into_statements() {
    let server = GatewayServer::new(new_store());
    let statements = server.parse_sql("TRUNCATE TABLE a, b, public.c;").unwrap();
    assert_eq!(statements.len(), 3);
    assert!(matches!(statements[0], Statement::Truncate { .. }));
    assert!(matches!(statements[1], Statement::Truncate { .. }));
    assert!(matches!(statements[2], Statement::Truncate { .. }));
}

#[tokio::test]
async fn truncate_table_removes_rows_but_keeps_catalog() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE b (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO a (id, name) VALUES (1, 'x'), (2, 'y')",
        "INSERT INTO b (id, name) VALUES (1, 'z')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    for statement in server.parse_sql("TRUNCATE TABLE a, b").unwrap() {
        let plan = server
            .plan_statement(&default_session(), statement)
            .await
            .unwrap();
        server
            .execute_plan(plan, None, FieldFormat::Text)
            .await
            .unwrap();
    }

    let a_schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "a")
        .await
        .unwrap()
        .unwrap();
    let b_schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "b")
        .await
        .unwrap()
        .unwrap();

    assert!(
        server
            .scan_visible_rows(None, &a_schema, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        server
            .scan_visible_rows(None, &b_schema, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        server
            .catalog
            .load_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "a")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn mysql_truncate_resets_auto_increment_and_returns_ok_rows_zero() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    for sql in [
        "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (name) VALUES ('a'), ('b')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let plan = plan_sql(&server, &default_session(), "TRUNCATE TABLE users").await;
    let response = server
        .execute_plan(plan, None, FieldFormat::Text)
        .await
        .unwrap();
    let Response::Execution(tag) = response else {
        panic!("expected command response");
    };
    let complete: CommandComplete = tag.into();
    assert_eq!(complete.tag, "TRUNCATE TABLE 0");

    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO users (name) VALUES ('after')",
    )
    .await;
    let response = server
        .execute_plan(
            plan_sql(&server, &default_session(), "SELECT id, name FROM users").await,
            None,
            FieldFormat::Text,
        )
        .await
        .unwrap();
    let rows = response_text_rows(response).await;
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string()), Some("after".to_string())]]
    );
}

#[tokio::test]
async fn mysql_explain_returns_basic_mysql_table_shape() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT)",
        "CREATE INDEX users_email_idx ON users(email)",
        "INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'a')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let response = server
        .execute_plan(
            plan_sql(
                &server,
                &default_session(),
                "EXPLAIN SELECT * FROM users WHERE id = 1",
            )
            .await,
            None,
            FieldFormat::Text,
        )
        .await
        .unwrap();
    assert_eq!(
        response_field_names(&response),
        vec![
            "id",
            "select_type",
            "table",
            "partitions",
            "type",
            "possible_keys",
            "key",
            "key_len",
            "ref",
            "rows",
            "filtered",
            "Extra"
        ]
    );
    let rows = response_text_rows(response).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Some("SIMPLE".to_string()));
    assert_eq!(rows[0][2], Some("users".to_string()));
    assert_eq!(rows[0][4], Some("const".to_string()));
    assert_eq!(rows[0][6], Some("PRIMARY".to_string()));
}

#[tokio::test]
async fn postgres_explain_returns_query_plan_rows() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT)",
        "CREATE INDEX users_email_idx ON users(email)",
        "INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'a')",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let response = server
        .execute_plan(
            plan_sql(
                &server,
                &default_session(),
                "EXPLAIN SELECT * FROM users WHERE email = 'a@example.com'",
            )
            .await,
            None,
            FieldFormat::Text,
        )
        .await
        .unwrap();
    assert_eq!(response_field_names(&response), vec!["QUERY PLAN"]);
    let rows = response_text_rows(response).await;
    assert!(
        rows.iter()
            .flatten()
            .flatten()
            .any(|line| line.contains("Index Scan using users_email_idx on users"))
    );
    assert!(
        rows.iter()
            .flatten()
            .flatten()
            .any(|line| line.contains("Index Cond: (email = 'a@example.com')"))
    );
}

#[tokio::test]
async fn mysql_analyze_and_optimize_table_return_mysql_status_rows() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
    )
    .await;

    for (sql, op) in [
        ("ANALYZE TABLE users", "analyze"),
        ("OPTIMIZE TABLE users", "optimize"),
    ] {
        let response = server
            .execute_plan(
                plan_sql(&server, &default_session(), sql).await,
                None,
                FieldFormat::Text,
            )
            .await
            .unwrap();
        assert_eq!(
            response_field_names(&response),
            vec!["Table", "Op", "Msg_type", "Msg_text"]
        );
        let rows = response_text_rows(response).await;
        assert_eq!(
            rows,
            vec![vec![
                Some("defaultdb.users".to_string()),
                Some(op.to_string()),
                Some("status".to_string()),
                Some("OK".to_string())
            ]]
        );
    }
}

#[tokio::test]
async fn postgres_analyze_refreshes_stats_and_vacuum_analyze_reuses_it() {
    let server = GatewayServer::new(new_store());
    let client = TestClient::default();
    for sql in [
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, NULL)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let response = server
        .execute_plan(
            plan_sql(&server, &default_session(), "ANALYZE users").await,
            None,
            FieldFormat::Text,
        )
        .await
        .unwrap();
    let Response::Execution(tag) = response else {
        panic!("expected ANALYZE command response");
    };
    let complete: CommandComplete = tag.into();
    assert_eq!(complete.tag, "ANALYZE");

    let mut stats = server
        .execute_via_datafusion(
            "SELECT attname, null_frac FROM pg_catalog.pg_stats \
                 WHERE tablename = 'users' ORDER BY attname",
            &default_session(),
        )
        .await
        .unwrap();
    let rows = response_text_rows(stats.remove(0)).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Some("id".to_string()));
    assert_eq!(rows[1][0], Some("name".to_string()));
    assert_eq!(rows[1][1], Some("0.5".to_string()));

    let vacuum = server
        .handle_postgres_vacuum_command(&client, "VACUUM")
        .await
        .unwrap()
        .unwrap();
    let Response::Execution(tag) = &vacuum[0] else {
        panic!("expected VACUUM command response");
    };
    let complete: CommandComplete = tag.clone().into();
    assert_eq!(complete.tag, "VACUUM");

    let vacuum_analyze = server
        .handle_postgres_vacuum_command(&client, "VACUUM ANALYZE users")
        .await
        .unwrap()
        .unwrap();
    let Response::Execution(tag) = &vacuum_analyze[0] else {
        panic!("expected ANALYZE command response");
    };
    let complete: CommandComplete = tag.clone().into();
    assert_eq!(complete.tag, "ANALYZE");

    let mixed_case_vacuum_analyze = server
        .handle_postgres_vacuum_command(&client, "VaCuUm (AnAlYzE) users;")
        .await
        .unwrap()
        .unwrap();
    let Response::Execution(tag) = &mixed_case_vacuum_analyze[0] else {
        panic!("expected ANALYZE command response");
    };
    let complete: CommandComplete = tag.clone().into();
    assert_eq!(complete.tag, "ANALYZE");

    let response = server
        .execute_plan(
            plan_sql(&server, &default_session(), "ANALYZE").await,
            None,
            FieldFormat::Text,
        )
        .await
        .unwrap();
    let Response::Execution(tag) = response else {
        panic!("expected bare ANALYZE command response");
    };
    let complete: CommandComplete = tag.into();
    assert_eq!(complete.tag, "ANALYZE");
}

#[tokio::test]
async fn mysql_fast_path_functions_and_operators_smoke() {
    let server = GatewayServer::with_mode(new_store(), GatewayMode::MySql);
    for sql in [
        "CREATE TABLE mysql_expr_stage (id INT PRIMARY KEY, name TEXT, doc TEXT, ts DATETIME, score INT, out TEXT)",
        "INSERT INTO mysql_expr_stage (id, name, doc, ts, score, out) VALUES (1, 'Alice Smith', '{\"name\":\"alice\",\"tags\":[\"x\",\"y\"]}', '2026-05-31 13:45:27', 9, NULL)",
        "UPDATE mysql_expr_stage
             SET
                out = CONCAT_WS('|',
                    SUBSTRING_INDEX(name, ' ', 1),
                    JSON_UNQUOTE(JSON_EXTRACT(doc, '$.name')),
                    DATE_FORMAT(DATE_ADD(ts, INTERVAL 1 DAY), '%Y-%m-%d'),
                    REGEXP_REPLACE('abc', 'b', 'X'),
                    CHARSET(name),
                    COLLATION(name)
                ),
                score = (score DIV 2) | (5 & 3)
             WHERE id = 1",
        "UPDATE mysql_expr_stage SET score = score + CASE WHEN REGEXP_LIKE(name, '^Alice') THEN 1 ELSE 0 END WHERE id = 1",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "mysql_expr_stage",
        )
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.get("out"),
        Some(&ColumnValue::Text(
            "Alice|alice|2026-06-01|aXc|utf8mb4|utf8mb4_0900_ai_ci".into()
        ))
    );
    assert_eq!(row.get("score"), Some(&ColumnValue::Int32(6)));
}

#[tokio::test]
async fn create_table_without_primary_key_uses_internal_row_ids_for_inserts() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE history (tid INT, bid INT, aid INT, delta INT)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO history (tid, bid, aid, delta) VALUES (1, 1, 1, 10), (2, 1, 2, 20)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "history")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.primary_key, INTERNAL_ROWID_COLUMN);

    let rows = server.scan_visible_rows(None, &schema, None).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn insert_current_timestamp_is_accepted() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE history (id INT PRIMARY KEY, mtime TIMESTAMP)",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        "INSERT INTO history (id, mtime) VALUES (1, CURRENT_TIMESTAMP)",
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "history")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(row.get("mtime"), Some(ColumnValue::Text(value)) if !value.is_empty())
            || matches!(row.get("mtime"), Some(ColumnValue::Timestamp(value)) if !value.is_empty())
    );
}

#[tokio::test]
async fn insert_phase2_types_and_arrays_roundtrip() {
    let server = GatewayServer::new(new_store());
    exec_sql(
        &server,
        &default_session(),
        "CREATE TABLE typed_values (
                id INT PRIMARY KEY,
                f4 REAL,
                f8 DOUBLE PRECISION,
                amount NUMERIC,
                d DATE,
                ts TIMESTAMP,
                tstz TIMESTAMPTZ,
                uid UUID,
                payload BYTEA,
                doc JSON,
                docb JSONB,
                tags INT[]
            )",
    )
    .await;
    exec_sql(
        &server,
        &default_session(),
        r#"INSERT INTO typed_values
               (id, f4, f8, amount, d, ts, tstz, uid, payload, doc, docb, tags)
               VALUES
               (1, 1.5, 2.25, '12.34', '2026-05-18',
                '2026-05-18 12:34:56', '2026-05-18T12:34:56Z',
                '123e4567-e89b-12d3-a456-426614174000',
                '\xdeadbeef', '{"a":1}', '{"b":true}', ARRAY[1,2,3])"#,
    )
    .await;

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "typed_values")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.find_column("f4").unwrap().data_type,
        DataType::Float32
    );
    assert_eq!(
        schema.find_column("f8").unwrap().data_type,
        DataType::Float64
    );
    assert_eq!(
        schema.find_column("amount").unwrap().data_type,
        DataType::Numeric {
            precision: None,
            scale: None
        }
    );
    assert_eq!(
        schema.find_column("ts").unwrap().data_type,
        DataType::Timestamp
    );
    assert_eq!(
        schema.find_column("tstz").unwrap().data_type,
        DataType::TimestampTz
    );
    assert_eq!(
        schema.find_column("tags").unwrap().data_type,
        DataType::Array(Box::new(DataType::Int32))
    );

    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("f4"), Some(&ColumnValue::Float32(1.5)));
    assert_eq!(row.get("f8"), Some(&ColumnValue::Float64(2.25)));
    assert_eq!(
        row.get("amount"),
        Some(&ColumnValue::Numeric("12.34".into()))
    );
    assert_eq!(row.get("d"), Some(&ColumnValue::Date("2026-05-18".into())));
    assert_eq!(
        row.get("ts"),
        Some(&ColumnValue::Timestamp("2026-05-18 12:34:56".into()))
    );
    assert_eq!(
        row.get("tstz"),
        Some(&ColumnValue::TimestampTz("2026-05-18T12:34:56Z".into()))
    );
    assert_eq!(
        row.get("uid"),
        Some(&ColumnValue::Uuid(
            "123e4567-e89b-12d3-a456-426614174000".into()
        ))
    );
    assert_eq!(
        row.get("payload"),
        Some(&ColumnValue::Bytea(vec![0xde, 0xad, 0xbe, 0xef]))
    );
    assert_eq!(
        row.get("doc"),
        Some(&ColumnValue::Json(r#"{"a":1}"#.into()))
    );
    assert_eq!(
        row.get("docb"),
        Some(&ColumnValue::Jsonb(r#"{"b":true}"#.into()))
    );
    assert_eq!(
        row.get("tags"),
        Some(&ColumnValue::Array(vec![
            ColumnValue::Int32(1),
            ColumnValue::Int32(2),
            ColumnValue::Int32(3),
        ]))
    );
}

#[tokio::test]
async fn compatibility_sprint_type_system_smoke() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE type_stage6 (
                id INT PRIMARY KEY,
                s SMALLINT,
                c CHAR(4),
                vc VARCHAR(5),
                doc JSONB,
                ints INT[],
                texts TEXT[],
                amount NUMERIC
            )",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        r#"INSERT INTO type_stage6
               (id, s, c, vc, doc, ints, texts, amount)
               VALUES
               (1, 12, 'xy', 'abc', '{"a":{"b":2},"tags":["red","blue"],"k":true}',
                ARRAY[1,2,3], ARRAY['a','b'], '123.456')"#,
    )
    .await
    .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "type_stage6")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.find_column("s").unwrap().data_type, DataType::Int16);
    assert_eq!(
        schema.find_column("c").unwrap().data_type,
        DataType::Char(Some(4))
    );
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("s"), Some(&ColumnValue::Int16(12)));
    assert_eq!(row.get("c"), Some(&ColumnValue::Text("xy  ".into())));

    for sql in [
        "SELECT id FROM type_stage6 WHERE s = 12",
        "SELECT id FROM type_stage6 WHERE doc @> '{\"a\":{\"b\":2}}'",
        "SELECT id FROM type_stage6 WHERE doc ? 'k'",
        "SELECT id FROM type_stage6 WHERE s = ANY(ARRAY[7,12,99])",
        "SELECT id FROM type_stage6 WHERE s <> ALL(ARRAY[7,8,9])",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    exec_sql_for_client(
        &server,
        &mut client,
        "UPDATE type_stage6 SET vc = doc ->> 'k' WHERE doc @> '{\"a\":{\"b\":2}}'",
    )
    .await
    .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("vc"), Some(&ColumnValue::Text("true".into())));

    let too_long = exec_sql_for_client(
            &server,
            &mut client,
            "INSERT INTO type_stage6 (id, s, c, vc, doc, ints, texts, amount) VALUES (2, 1, 'toolong', 'abc', '{}', ARRAY[1], ARRAY['x'], '1')",
        )
        .await
        .unwrap_err();
    assert!(format!("{too_long:?}").contains("22001"));
}

#[tokio::test]
async fn compatibility_sprint_function_expression_smoke() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    exec_sql_for_client(
        &server,
        &mut client,
        "CREATE TABLE expr_stage (
                id INT PRIMARY KEY,
                name TEXT,
                amount NUMERIC,
                score INT,
                ts TIMESTAMP,
                out TEXT
            )",
    )
    .await
    .unwrap();
    exec_sql_for_client(
        &server,
        &mut client,
        "INSERT INTO expr_stage (id, name, amount, score, ts, out)
             VALUES (1, ' Alice ', '12.40', -7, '2026-05-31 13:45:27', NULL)",
    )
    .await
    .unwrap();

    for sql in [
        "SELECT lower(name), upper(name), length(name), substring(name from 2 for 3), trim(name), replace(name, 'A', 'a'), concat(name, ':x') FROM expr_stage",
        "SELECT abs(score), round(score), ceil(score), floor(score) FROM expr_stage",
        "SELECT CASE WHEN score < 0 THEN 'neg' ELSE 'pos' END, coalesce(out, 'missing'), nullif(name, 'bob'), greatest(score, 3), least(score, 3) FROM expr_stage",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    exec_sql_for_client(
            &server,
            &mut client,
            "UPDATE expr_stage
             SET
                name = upper(trim(name)),
                score = CAST(ceil(abs(score) + 1.2) AS INT),
                ts = date_trunc('day', ts),
                out = CASE WHEN nullif(out, '') IS NULL THEN concat(lower(trim(name)), ':', extract(year from ts)) ELSE out END
             WHERE id = 1",
        )
        .await
        .unwrap();

    let schema = server
        .load_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "expr_stage")
        .await
        .unwrap()
        .unwrap();
    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("name"), Some(&ColumnValue::Text("ALICE".into())));
    assert_eq!(row.get("score"), Some(&ColumnValue::Int32(9)));
    assert_eq!(
        row.get("ts"),
        Some(&ColumnValue::Timestamp("2026-05-31 00:00:00".into()))
    );
    assert_eq!(
        row.get("out"),
        Some(&ColumnValue::Text("alice:2026".into()))
    );
}

#[tokio::test]
async fn alter_table_add_primary_key_rewrites_existing_rows() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE pgbench_branches (bid INT, bbalance INT, filler TEXT)",
        "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (1, 10, 'a'), (2, 20, 'b')",
        "ALTER TABLE pgbench_branches ADD PRIMARY KEY (bid)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "pgbench_branches",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(schema.primary_key, "bid");
    assert!(schema.find_column("bid").unwrap().primary_key);

    let row = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get("bbalance"), Some(&ColumnValue::Int32(20)));
    assert_eq!(
        server
            .scan_visible_rows(None, &schema, None)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn datafusion_count_star_preserves_empty_projection_row_count() {
    let server = GatewayServer::new(new_store());
    for sql in [
        "CREATE TABLE pgbench_branches (bid INT, bbalance INT, filler TEXT)",
        "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (1, 10, 'a')",
        "ALTER TABLE pgbench_branches ADD PRIMARY KEY (bid)",
    ] {
        exec_sql(&server, &default_session(), sql).await;
    }

    let batches = server
        .collect_datafusion_batches("SELECT count(*) FROM pgbench_branches", &default_session())
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(arrow_array_value_to_string(batches[0].column(0), 0), "1");
}

#[tokio::test]
async fn transaction_update_is_visible_to_session_before_commit() {
    let server = GatewayServer::new(new_store());
    let mut client = TestClient::default();
    for sql in [
        "CREATE TABLE pgbench_accounts (aid INT, abalance INT, filler TEXT)",
        "INSERT INTO pgbench_accounts (aid, abalance, filler) VALUES (1, 10, '')",
        "ALTER TABLE pgbench_accounts ADD PRIMARY KEY (aid)",
    ] {
        exec_sql_for_client(&server, &mut client, sql)
            .await
            .unwrap();
    }

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

    let schema = server
        .load_schema(
            DEFAULT_DATABASE_NAME,
            DEFAULT_SCHEMA_NAME,
            "pgbench_accounts",
        )
        .await
        .unwrap()
        .unwrap();
    let session_id = Some(client.pid_and_secret_key().0);
    let in_tx = server
        .read_visible_row(session_id, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(in_tx.get("abalance"), Some(&ColumnValue::Int32(15)));
    let outside_tx = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outside_tx.get("abalance"), Some(&ColumnValue::Int32(10)));

    exec_sql_for_client(&server, &mut client, "COMMIT")
        .await
        .unwrap();
    let committed = server
        .read_visible_row(None, &schema, &ColumnValue::Int32(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.get("abalance"), Some(&ColumnValue::Int32(15)));
}

pub(super) async fn create_small_pgbench_schema(server: &GatewayServer, client: &mut TestClient) {
    for sql in [
        "CREATE TABLE pgbench_branches (bid INT PRIMARY KEY, bbalance INT, filler TEXT)",
        "CREATE TABLE pgbench_tellers (tid INT PRIMARY KEY, bid INT, tbalance INT, filler TEXT)",
        "CREATE TABLE pgbench_accounts (aid INT PRIMARY KEY, bid INT, abalance INT, filler TEXT)",
        "CREATE TABLE pgbench_history (tid INT, bid INT, aid INT, delta INT, mtime TIMESTAMP, filler TEXT)",
        "INSERT INTO pgbench_branches (bid, bbalance, filler) VALUES (1, 0, '')",
        "INSERT INTO pgbench_tellers (tid, bid, tbalance, filler) VALUES (1, 1, 0, '')",
        "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES (1, 1, 0, '')",
    ] {
        exec_sql_for_client(server, client, sql).await.unwrap();
    }
}

pub(super) async fn exec_pgbench_transaction(
    server: &GatewayServer,
    client: &mut TestClient,
    delta: i32,
) {
    for sql in [
        "BEGIN".to_string(),
        format!("UPDATE pgbench_accounts SET abalance = abalance + {delta} WHERE aid = 1"),
        "SELECT abalance FROM pgbench_accounts WHERE aid = 1".to_string(),
        format!("UPDATE pgbench_tellers SET tbalance = tbalance + {delta} WHERE tid = 1"),
        format!("UPDATE pgbench_branches SET bbalance = bbalance + {delta} WHERE bid = 1"),
        format!(
            "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime, filler) VALUES (1, 1, 1, {delta}, '2026-05-19 00:00:00', '')"
        ),
        "COMMIT".to_string(),
    ] {
        exec_sql_for_client(server, client, &sql).await.unwrap();
    }
}

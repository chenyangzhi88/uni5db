# pg_gateway MySQL Compatibility Plan

Goal: make `pg_gateway` usable by the MySQL CLI, common MySQL drivers, ORMs,
and ordinary CRUD applications. This is a compatibility layer over the existing
gateway catalog, KV OLTP path, and DataFusion OLAP path; it is not a full MySQL
server clone.

## Phase A Scope

Phase A targets the most important 80% of MySQL behavior:

- Client connection and session commands used by MySQL clients and drivers.
- Common MySQL DDL/DML syntax that can map cleanly to existing gateway plans.
- Basic MySQL metadata and introspection commands.
- Prepared statement lifecycle and parameter execution.
- MySQL-flavored type names for ordinary application schemas.

Out of scope for Phase A:

- Stored procedures, triggers, events, replication, binlog, and XA.
- Full privilege system and account management.
- Full charset/collation semantics.
- Optimizer hints, partitions, generated columns, and full SQL mode parity.
- Full InnoDB lock behavior beyond existing gateway transaction semantics.

## P1 Protocol And Session Baseline

Required behavior:

- Start in MySQL mode through `PG_GATEWAY_MODE=mysql`.
- Accept MySQL client startup and `COM_INIT_DB`.
- Route `BEGIN`, `START TRANSACTION`, `COMMIT`, and `ROLLBACK` into gateway
  transaction state instead of replying with a synthetic OK.
- Accept common driver setup statements:
  - `SET NAMES ...`
  - `SET CHARACTER SET ...`
  - `SET autocommit = ...`
  - `SET sql_mode = ...`
  - `SET time_zone = ...`
  - `SET transaction isolation level ...`
- Return MySQL-style rows for common environment probes:
  - `SELECT 1`
  - `SELECT VERSION()`
  - `SELECT DATABASE()`
  - `SELECT USER()`
  - `SELECT @@version`, `@@version_comment`, `@@autocommit`,
    `@@transaction_isolation`, `@@tx_isolation`, `@@sql_mode`,
    `@@character_set_client`, `@@character_set_connection`,
    `@@character_set_results`, `@@time_zone`
- Return useful results for:
  - `SHOW DATABASES`
  - `SHOW TABLES`
  - `SHOW WARNINGS`

Validation:

- `cargo test -p pg_gateway mysql`
- `cargo test -p pg_gateway --lib`
- Manual smoke with `mysql -h 127.0.0.1 -P <port> -u root` once a server is
  running.

## P2 MySQL SQL Dialect Rewrite

The MySQL parser should normalize common MySQL SQL into forms the gateway core
already supports.

Required rewrites:

- Backtick identifiers are accepted through `sqlparser::MySqlDialect`.
- `USE db` becomes a session database switch handled by the protocol/session
  layer.
- `SHOW TABLES`, `SHOW COLUMNS FROM table`, and `DESCRIBE table` become
  metadata queries or synthetic protocol responses. `SHOW COLUMNS`/`DESCRIBE`
  report MySQL-style `Key` and `Extra` values for primary keys, single-column
  unique constraints/indexes, ordinary indexes, and `AUTO_INCREMENT`.
- `LIMIT offset, count` becomes standard `LIMIT count OFFSET offset`.
- `AUTO_INCREMENT` columns become gateway sequence-backed defaults.
- `INSERT ... ON DUPLICATE KEY UPDATE ...` uses MySQL-style conflict inference:
  the insert probes the primary key, table unique constraints, and unique
  indexes before applying the update assignment.
- MySQL type aliases normalize to gateway types:
  - `TINYINT`/`BOOL`/`BOOLEAN`
  - `SMALLINT`, `INT`/`INTEGER`, `BIGINT`
  - `FLOAT`, `DOUBLE`, `DECIMAL`
  - `CHAR`, `VARCHAR`, `TEXT`
  - `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`
  - `BLOB`, `JSON`

Validation SQL:

```sql
CREATE DATABASE app;
USE app;
CREATE TABLE users (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) UNIQUE,
  name TEXT,
  active TINYINT DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (email, name) VALUES ('a@example.com', 'alice');
INSERT INTO users (id, email, name) VALUES (1, 'a@example.com', 'Alice')
  ON DUPLICATE KEY UPDATE name = VALUES(name);
SELECT id, email, name FROM users ORDER BY id LIMIT 0, 10;
SHOW TABLES;
DESCRIBE users;
```

## Completed Phase A Hardening

- P3: Broader DDL and CRUD parity for the 80% target is covered by existing
  gateway DDL/DML plus MySQL aliases for common table definitions, ordinary
  `CREATE INDEX`/`CREATE UNIQUE INDEX`, `ALTER TABLE` forms already supported
  by the core planner, and MySQL `ON DUPLICATE KEY UPDATE`.
- P4: MySQL binary prepared execution binds `COM_STMT_EXECUTE` parameters from
  `ParamParser` into SQL literals before routing through the gateway planner.
  Strings, numbers, NULL, date/time, and datetime values are rendered with SQL
  escaping.
- P5: ORM bootstrap metadata probes are handled directly in the MySQL protocol
  layer, including session variables, database/table listing, warnings, and
  column description metadata.
- P6: Common scalar/type compatibility for the 80% target is implemented through
  MySQL dialect parsing, MySQL type-name aliases, `VALUES(col)` in duplicate-key
  updates, and existing gateway scalar expression evaluation.

## Completed Catalog/Metadata Hardening

- MySQL mode now registers a MySQL-shaped `information_schema` separately from
  the PostgreSQL catalog view set. PostgreSQL mode keeps the SQL-standard /
  PostgreSQL-compatible `information_schema` fields, while MySQL mode exposes
  MySQL-style fields for driver and ORM metadata probes.
- MySQL `information_schema.tables` includes MySQL columns such as `engine`,
  `version`, `row_format`, `table_rows`, `data_length`, `index_length`,
  `auto_increment`, `table_collation`, `create_options`, and `table_comment`.
  Fields that need storage-engine statistics but are not tracked yet return
  `NULL` or conservative constants.
- MySQL `information_schema.columns` includes `column_type`, `column_key`,
  `extra`, `character_set_name`, `collation_name`, precision/scale, datetime
  precision, privileges, comments, generated expression, and SRS placeholders.
- MySQL `information_schema.statistics`, `table_constraints`,
  `key_column_usage`, `referential_constraints`, `views`, `schemata`,
  `engines`, `character_sets`, `collations`, `processlist`,
  `global_variables`, and `session_variables` are registered for common
  metadata queries. Unsupported subsystems such as routines remain empty
  compatibility tables.
- MySQL protocol metadata commands now cover `SHOW FULL TABLES`,
  `SHOW TABLE STATUS`, `SHOW INDEX` / `SHOW KEYS`, `SHOW VARIABLES`,
  `SHOW ENGINES`, `SHOW CHARACTER SET`, `SHOW COLLATION`,
  `SHOW CREATE DATABASE`, `SHOW CREATE TABLE`, and `SHOW FULL COLUMNS`.
  `SHOW CREATE TABLE` is reconstructed from the gateway catalog with columns,
  defaults, `AUTO_INCREMENT`, primary keys, unique keys, secondary indexes, and
  engine/charset/collation clauses.

Remaining outside Phase A:

- Full MySQL privilege/account management, stored programs, replication/binlog,
  optimizer hints, partitioning, generated columns, full charset/collation
  semantics, and exact InnoDB locking behavior.
- Exhaustive MySQL `information_schema` parity for every ORM edge case.
- Full MySQL SQL mode matrix and every MySQL scalar/date/string function.

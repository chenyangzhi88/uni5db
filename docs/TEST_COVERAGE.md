# pg_gateway Test Coverage

This file tracks the current local test surface for `pg_gateway`.

## Unit Coverage Baseline

Command:

```bash
cargo llvm-cov -p pg_gateway --lib --summary-only
```

Current baseline:

| Metric | Coverage |
| --- | ---: |
| Regions | 66.38% |
| Functions | 58.72% |
| Lines | 68.60% |

Lowest line-coverage areas from the current baseline:

| File | Line Coverage | Notes |
| --- | ---: | --- |
| `protocol/mysql.rs` | 39.86% | Protocol integration is covered by MySQL CLI, Go, JDBC, and mysqlclient smoke; packet-level unit tests now cover malformed pgwire row decoding and prepared-parameter rendering. |
| `kv_engine_store.rs` | 36.42% | Transactional range scan tests now cover pending puts/deletes, reverse limit scans, commit visibility, and rollback visibility. |
| `codec.rs` | 46.07% | Needs more row/cell/null/version encoding cases. |
| `sql.rs` | 54.30% | Type conversion and expression-rule tests now cover unsigned integer bounds, `BIT`, `YEAR`, range-bound merging, integer division, XOR, and division-by-zero errors. |
| `protocol/postgres.rs` | 57.72% | Network serve loop is covered by real client smoke; pure PEM/base64 helpers have unit tests. |

Coverage is optional in local pre-submit because it is slower than normal tests:

```bash
RUN_PG_GATEWAY_COVERAGE=1 scripts/local_presubmit.sh
```

## PostgreSQL End-to-End Smoke

Entrypoint:

```bash
pg_gateway/tests/postgres_smoke/local_ci.sh
```

Covered clients and paths:

| Area | Coverage |
| --- | --- |
| `psql` | connect, `SELECT 1`, DDL, DML, result checking |
| `COPY` | `\copy ... FROM` text data |
| SQL prepared statements | `PREPARE` / `EXECUTE` via `psql` |
| Metadata | `information_schema.columns`, `pg_catalog.pg_class` |
| Maintenance | `ANALYZE`, `VACUUM`, `VACUUM ANALYZE`, `EXPLAIN`, `TRUNCATE TABLE` |
| `pgbench` | init, simple mode, prepared mode, consistency query |
| Python | `psycopg`, SQLAlchemy parameter binding and metadata |
| Rust | SQLx prepared query and metadata |
| Java | PostgreSQL JDBC prepared query and metadata |

## MySQL End-to-End Smoke

Entrypoint:

```bash
pg_gateway/tests/mysql_smoke/local_ci.sh
```

Covered clients and paths:

| Area | Coverage |
| --- | --- |
| MySQL CLI | connect, DDL, DML, result checking |
| Bulk load | `LOAD DATA LOCAL INFILE` |
| Metadata | `DESCRIBE`, `SHOW COLUMNS` |
| Maintenance | `EXPLAIN`, `ANALYZE TABLE`, `OPTIMIZE TABLE`, `TRUNCATE TABLE` |
| Go | `database/sql` with `go-sql-driver/mysql`, prepared query, result metadata |
| Java | MySQL Connector/J prepared query and metadata |
| Python | `mysqlclient` prepared-style parameters and metadata |

## Remaining Gaps

- PostgreSQL `EXPLAIN` has a basic fast-path text table implementation and smoke coverage; it does not yet implement PostgreSQL cost estimates, `EXPLAIN ANALYZE`, `FORMAT`, `BUFFERS`, `VERBOSE`, or slow-path/DataFusion explain output.
- PostgreSQL autovacuum/version-GC behavior is not implemented; `VACUUM` is currently a compatibility no-op and `VACUUM ANALYZE` refreshes stats.
- PostgreSQL protocol unit coverage is still mostly represented by real-client smoke, not packet-level unit tests.
- MySQL protocol packet-level unit tests should be expanded around multi-result, capability negotiation, auth variants, and error mapping.
- Coverage does not currently include external smoke subprocess execution; it measures Rust unit tests only.

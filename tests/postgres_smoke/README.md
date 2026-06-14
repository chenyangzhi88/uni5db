# pg_gateway PostgreSQL Smoke

This directory contains local end-to-end PostgreSQL protocol smoke tests for
`pg_gateway`. The runner starts `pg_gateway` in PostgreSQL mode with the memory
backend, then verifies real clients and common driver paths:

- `psql`
- `COPY FROM STDIN`
- SQL prepared statements and extended-query parameters
- catalog and metadata queries
- `ANALYZE`, `VACUUM`, `VACUUM ANALYZE`, `EXPLAIN`, and `TRUNCATE TABLE`
- `pgbench`
- Python `psycopg` and SQLAlchemy
- Rust SQLx
- PostgreSQL JDBC

Run the full local CI entrypoint:

```bash
pg_gateway/tests/postgres_smoke/local_ci.sh
```

Useful knobs:

- `PG_GATEWAY_POSTGRES_HOST`, default `127.0.0.1`
- `PG_GATEWAY_POSTGRES_PORT`, default `55433`
- `PG_GATEWAY_POSTGRES_USER`, default `postgres`
- `PG_GATEWAY_POSTGRES_DATABASE`, default `defaultdb`
- `PG_GATEWAY_POSTGRES_SMOKE_REQUIRE_ALL`, default `0` for `run.sh`, `1` for `local_ci.sh`
- `PG_GATEWAY_POSTGRES_SMOKE_WITH_PYTHON`, default `auto`
- `PG_GATEWAY_POSTGRES_SMOKE_WITH_SQLX`, default `auto`
- `PG_GATEWAY_POSTGRES_SMOKE_WITH_JDBC`, default `auto`
- `PG_GATEWAY_POSTGRES_SMOKE_WITH_PGBENCH`, default `auto`
- `PGBENCH_SCALE`, default `1`
- `PGBENCH_DURATION`, default `2`
- `PYTHON_BIN`, default `python3`
- `MVN_BIN`, default `mvn`
- `CARGO_BIN`, default `cargo`

If Python dependencies are not installed globally, point `PYTHON_BIN` at a venv
containing `psycopg` and `sqlalchemy`.

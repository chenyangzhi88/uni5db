# MySQL Driver Smoke

This smoke test starts `pg_gateway` in MySQL mode with the in-memory backend, then verifies:

- MySQL CLI connectivity and basic DDL/DML.
- `LOAD DATA LOCAL INFILE` through the MySQL CLI.
- CLI metadata and maintenance commands: `DESCRIBE`, `SHOW COLUMNS`,
  `EXPLAIN`, `ANALYZE TABLE`, `OPTIMIZE TABLE`, and `TRUNCATE TABLE`.
- Go `database/sql` with `github.com/go-sql-driver/mysql`, including prepared statements and result metadata.
- JDBC with MySQL Connector/J when Maven is available.
- Python `mysqlclient` / `MySQLdb` when the module is installed.

Run from the repository root:

```bash
pg_gateway/tests/mysql_smoke/run.sh
```

Optional environment variables:

- `PG_GATEWAY_MYSQL_HOST`, default `127.0.0.1`
- `PG_GATEWAY_MYSQL_PORT`, default `33307`
- `PG_GATEWAY_MYSQL_USER`, default `root`
- `PG_GATEWAY_MYSQL_PASSWORD`, default empty
- `PG_GATEWAY_MYSQL_DATABASE`, default `defaultdb`
- `MYSQL_BIN`, default `/usr/bin/mysql`
- `GO_BIN`, default `go`
- `MVN_BIN`, default `mvn`
- `PYTHON_BIN`, default `python3`
- `PG_GATEWAY_MYSQL_SMOKE_WITH_JDBC`, default `auto`
- `PG_GATEWAY_MYSQL_SMOKE_WITH_MYSQLCLIENT`, default `auto`
- `PG_GATEWAY_MYSQL_SMOKE_REQUIRE_ALL`, default `0`

Run the local CI wrapper to require every smoke path:

```bash
pg_gateway/tests/mysql_smoke/local_ci.sh
```

`local_ci.sh` runs `cargo test -p pg_gateway --lib`, then runs this smoke with
`PG_GATEWAY_MYSQL_SMOKE_REQUIRE_ALL=1`.

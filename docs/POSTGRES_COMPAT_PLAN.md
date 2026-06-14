# pg_gateway PostgreSQL Compatibility Plan

Goal: build a PostgreSQL-compatible OLTP + OLAP database on top of the existing
KV engine. Replication is explicitly out of scope. OLAP execution is delegated
to DataFusion; OLTP execution should use KV-native access paths.

## Architecture

1. Wire protocol
   - Startup, SSL negotiation, cancel requests, simple query, extended query,
     COPY, and precise transaction status.
   - Extended query must support Parse/Bind/Describe/Execute with typed
     parameters and prepared statement lifecycle.

2. Catalog
   - Store databases, schemas, tables, columns, primary keys, secondary indexes,
     constraints, functions, types, and privileges in KV catalog records.
   - Expose PostgreSQL-compatible `pg_catalog` and `information_schema` rows for
     psql and common drivers.

3. Storage layout
   - Table rows use KV row markers and per-column cells.
   - Primary key is encoded in row keys for ordered scans.
   - Secondary indexes use KV keys shaped as `index prefix + indexed value + pk`.
   - Unique indexes validate conflicts before row mutation.

4. OLTP executor
   - Fast path: point lookup, range scan, index lookup, insert, update, delete,
     upsert, returning, transactions, constraints.
   - Planner chooses primary key, secondary index, or table scan.
   - Mutations update table data and all affected indexes atomically in the
     session transaction.

5. OLAP executor
   - DataFusion handles joins, aggregates, grouping, ordering, subqueries,
     expressions, and analytical scans.
   - KV tables are registered as DataFusion table providers.
   - Later phases should push projection/filter/index access into DataFusion
     table providers where useful.

6. Type system
   - Phase 1: int4, int8, text, bool.
   - Phase 2: float4/float8, numeric, date, time/timetz, interval,
     timestamp/timestamptz, uuid, bytea, json/jsonb, varchar(n).
   - Phase 3: arrays, domains, enums, collations.

7. PostgreSQL semantics
   - SQLSTATE-compatible errors.
   - MVCC-style transaction visibility as far as KV transaction APIs allow.
   - Search path, schemas, prepared statements, savepoints, constraints,
     default values, nullability, sequences.

## Implementation Phases

1. Protocol baseline
   - Simple query, extended query, COPY FROM STDIN, transaction state.
   - Driver smoke tests for psql, tokio-postgres, psycopg, JDBC.

2. Catalog baseline
   - `pg_database`, `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
     `pg_index`, `pg_constraint`, `pg_tables`, `information_schema.tables`,
     `information_schema.columns`.

3. OLTP indexes
   - `CREATE INDEX` and `CREATE UNIQUE INDEX`.
   - Backfill existing rows.
   - Maintain indexes on INSERT/UPDATE/DELETE/COPY.
   - Planner uses single-column equality predicates through indexes.

4. Constraints
   - NOT NULL, primary key, unique, check, foreign key metadata first.
   - Enforce NOT NULL, unique, primary key immediately.
   - Defer foreign key enforcement until catalog and transaction semantics are
     stable.

5. SQL planner/executor
   - Normalize SQL AST into logical plans.
   - Choose KV point lookup, KV index scan, KV table scan, or DataFusion.
   - Support parameterized plans without string substitution.

6. OLAP integration
   - Keep DataFusion as the analytical engine.
   - Improve table provider with projection/filter pushdown.
   - Route writes and transactional reads through OLTP path.

7. Compatibility hardening
   - psql meta-command coverage.
   - pgbench initialization and simple transaction workload.
   - sqllogictest subset.
   - Error message and SQLSTATE parity for common cases.

## Phase A Status

Implemented in the current compatibility pass:

- `sqllogictest` smoke baseline at `tests/sqllogictest/phase_a.slt`.
- pgbench init smoke script at `tests/pgbench/phase_a_init.sh`.
- ORM smoke skeletons for psycopg, SQLAlchemy, and SQLx under
  `tests/orm_smoke/`.
- Real `CREATE VIEW` catalog metadata, `pg_catalog.pg_views`, and
  `information_schema.views`.
- Basic view SELECT expansion through the DataFusion slow path.
- Common catalog functions: `format_type`, `pg_get_expr`, `pg_get_viewdef`,
  `pg_get_userbyid`, `pg_encoding_to_char`, `array_to_string`,
  `obj_description`, `col_description`.
- Real type descriptors for `time`, `timetz`, `interval`, and `varchar(n)`;
  values are still stored text-backed but validated/coerced at SQL boundaries.

Validation:

- `cargo test -p pg_gateway --lib`: green, including the Phase A smoke,
  sqllogictest baseline, transaction visibility, and kv-engine transaction
  coverage.
- `pgbench -i -s 1`: passes against a running pg_gateway.
- `pgbench -c 1 -T 10`: passes functionally in simple query mode.
- `pgbench -M prepared -c 1 -T 10`: passes functionally in prepared query mode.
- `tests/pgbench/phase_a3_runtime.sh` reruns init, simple runtime, prepared
  runtime, and the pgbench consistency aggregate query.

Current PostgreSQL compatibility gaps:

- FK is enforced immediately for basic insert/update/delete cases; deferred
  constraints, cascades, and full PostgreSQL match/action semantics remain out
  of scope for Phase A.
- View expansion is query-text based and supports common SELECT cases, not full
  PostgreSQL rewrite-rule semantics.
- SQL PREPARE and extended-query Bind remain session-scoped SQL rendering paths,
  not a real typed plan cache.
- Transactional DataFusion aggregate SELECT is used for committed consistency
  checks; uncommitted OLAP overlays remain limited.
- pgbench runtime is correctness-first and still intentionally unoptimized.



 下一步建议：把 catalog/table metadata 做 server 级缓存。现在普通 mysql 虽然不再无限卡住，但 CLI 补全仍会花几秒；另外我顺手测到 SELECT COUNT(*) FROM orders 还有独立查询层问题，会触发连接异常，这个建议
  下一个收。
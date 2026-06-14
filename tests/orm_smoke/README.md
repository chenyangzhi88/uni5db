# ORM Smoke Tests

These smoke tests target a running pg_gateway on `127.0.0.1:55432`.

Run the gateway:

```bash
PG_GATEWAY_BACKEND=memory cargo run
```

Run client smoke tests:

```bash
PG_GATEWAY_DSN=postgresql://postgres@127.0.0.1:55432/defaultdb python tests/orm_smoke/psycopg_smoke.py
PG_GATEWAY_SQLALCHEMY_DSN=postgresql+psycopg://postgres@127.0.0.1:55432/defaultdb python tests/orm_smoke/sqlalchemy_smoke.py
```

The cargo unit test `orm_metadata_introspection_smoke_queries` keeps the
dependency-free metadata surface pinned for CI. The Python scripts are the
driver-level smoke checks and require local `psycopg` / `sqlalchemy`
installations.

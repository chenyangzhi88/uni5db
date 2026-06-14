import os

import psycopg
from sqlalchemy import create_engine, text


dsn = os.environ.get(
    "PG_GATEWAY_POSTGRES_DSN",
    "postgresql://postgres@127.0.0.1:55433/defaultdb",
)
sqlalchemy_dsn = os.environ.get(
    "PG_GATEWAY_SQLALCHEMY_DSN",
    "postgresql+psycopg://postgres@127.0.0.1:55433/defaultdb",
)

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS py_psycopg_smoke")
        cur.execute("CREATE TABLE py_psycopg_smoke (id INT PRIMARY KEY, name TEXT NOT NULL)")
        cur.execute(
            "INSERT INTO py_psycopg_smoke (id, name) VALUES (%s, %s), (%s, %s)",
            (1, "alice", 2, "bob"),
        )
        cur.execute("SELECT name FROM py_psycopg_smoke WHERE id = %s", (2,))
        assert cur.fetchone() == ("bob",)
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s ORDER BY ordinal_position",
            ("py_psycopg_smoke",),
        )
        assert cur.fetchall() == [("id",), ("name",)]

engine = create_engine(sqlalchemy_dsn)
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS py_sqlalchemy_smoke"))
    conn.execute(text("CREATE TABLE py_sqlalchemy_smoke (id INT PRIMARY KEY, name TEXT NOT NULL)"))
    conn.execute(
        text("INSERT INTO py_sqlalchemy_smoke (id, name) VALUES (:id, :name)"),
        {"id": 1, "name": "carol"},
    )
    row = conn.execute(
        text("SELECT name FROM py_sqlalchemy_smoke WHERE id = :id"),
        {"id": 1},
    ).one()
    assert row[0] == "carol"
    tables = conn.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'py_sqlalchemy_smoke'"
        )
    ).all()
    assert tables

print("python postgres smoke ok")

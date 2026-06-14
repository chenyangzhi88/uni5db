import os

from sqlalchemy import create_engine, text


dsn = os.environ.get(
    "PG_GATEWAY_SQLALCHEMY_DSN",
    "postgresql+psycopg://postgres@127.0.0.1:55432/defaultdb",
)

engine = create_engine(dsn)

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS orm_sqlalchemy"))
    conn.execute(text("CREATE TABLE orm_sqlalchemy (id INT PRIMARY KEY, name TEXT)"))
    conn.execute(
        text("INSERT INTO orm_sqlalchemy (id, name) VALUES (:id, :name)"),
        {"id": 1, "name": "alice"},
    )
    row = conn.execute(
        text("SELECT name FROM orm_sqlalchemy WHERE id = :id"),
        {"id": 1},
    ).one()
    assert row[0] == "alice"
    tables = conn.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'orm_sqlalchemy'"
        )
    ).all()
    assert tables

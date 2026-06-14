import os

import psycopg


dsn = os.environ.get(
    "PG_GATEWAY_DSN",
    "postgresql://postgres@127.0.0.1:55432/defaultdb",
)

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orm_psycopg")
        cur.execute("CREATE TABLE orm_psycopg (id INT PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO orm_psycopg (id, name) VALUES (%s, %s)", (1, "alice"))
        cur.execute("SELECT name FROM orm_psycopg WHERE id = %s", (1,))
        assert cur.fetchone() == ("alice",)
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = %s",
            ("orm_psycopg",),
        )
        assert cur.fetchone() == ("orm_psycopg",)

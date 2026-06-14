import os

import MySQLdb


def env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def main() -> None:
    conn = MySQLdb.connect(
        host=env("PG_GATEWAY_MYSQL_HOST", "127.0.0.1"),
        port=int(env("PG_GATEWAY_MYSQL_PORT", "33307")),
        user=env("PG_GATEWAY_MYSQL_USER", "root"),
        passwd=env("PG_GATEWAY_MYSQL_PASSWORD", ""),
        db=env("PG_GATEWAY_MYSQL_DATABASE", "defaultdb"),
        charset="utf8mb4",
        local_infile=1,
    )
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS mysqlclient_smoke")
        cur.execute(
            "CREATE TABLE mysqlclient_smoke (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)"
        )
        cur.executemany(
            "INSERT INTO mysqlclient_smoke VALUES (%s, %s)",
            [(1, "alice"), (2, "bob")],
        )
        conn.commit()

        cur.execute("SELECT name FROM mysqlclient_smoke WHERE id = %s", (2,))
        row = cur.fetchone()
        if row != ("bob",):
            raise AssertionError(f"prepared-style query returned {row!r}, want ('bob',)")

        cur.execute("SELECT id, name FROM mysqlclient_smoke ORDER BY id")
        rows = cur.fetchall()
        if rows != ((1, "alice"), (2, "bob")):
            raise AssertionError(f"unexpected selected rows: {rows!r}")
        if [col[0] for col in cur.description] != ["id", "name"]:
            raise AssertionError(f"unexpected select metadata: {cur.description!r}")

        cur.execute("DESCRIBE mysqlclient_smoke")
        describe_rows = cur.fetchall()
        if len(describe_rows) != 2:
            raise AssertionError(f"DESCRIBE returned {len(describe_rows)} rows, want 2")
        if len(cur.description or ()) != 6:
            raise AssertionError(
                f"DESCRIBE metadata returned {len(cur.description or ())} columns, want 6"
            )
    finally:
        conn.close()

    print("python mysqlclient smoke ok")


if __name__ == "__main__":
    main()


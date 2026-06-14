// Manual smoke target for a running pg_gateway:
//
//   PG_GATEWAY_DSN=postgres://postgres@127.0.0.1:55432/defaultdb \
//   cargo run --example sqlx_smoke
//
// Keep this file dependency-free in pg_gateway. It documents the SQLx
// compatibility surface we expect once a workspace-level smoke crate is added.

const SQLX_SMOKE_SQL: &[&str] = &[
    "DROP TABLE IF EXISTS orm_sqlx",
    "CREATE TABLE orm_sqlx (id INT PRIMARY KEY, name TEXT)",
    "INSERT INTO orm_sqlx (id, name) VALUES ($1, $2)",
    "SELECT name FROM orm_sqlx WHERE id = $1",
    "SELECT table_name FROM information_schema.tables WHERE table_name = 'orm_sqlx'",
];

fn main() {
    for sql in SQLX_SMOKE_SQL {
        println!("{sql}");
    }
}

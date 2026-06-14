use std::time::Duration;

use sqlx::{Column, PgPool, Row};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:55433/defaultdb".to_string());
    let pool = PgPool::connect(&dsn).await?;

    sqlx::query("DROP TABLE IF EXISTS sqlx_smoke")
        .execute(&pool)
        .await?;
    sqlx::query("CREATE TABLE sqlx_smoke (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO sqlx_smoke (id, name) VALUES ($1, $2), ($3, $4)")
        .bind(1_i32)
        .bind("alice")
        .bind(2_i32)
        .bind("bob")
        .execute(&pool)
        .await?;

    let row = sqlx::query("SELECT name FROM sqlx_smoke WHERE id = $1")
        .bind(2_i32)
        .fetch_one(&pool)
        .await?;
    let name: String = row.try_get("name")?;
    if name != "bob" {
        return Err(format!("prepared SQLx query returned {name}, want bob").into());
    }

    let rows = sqlx::query("SELECT id, name FROM sqlx_smoke ORDER BY id")
        .fetch_all(&pool)
        .await?;
    if rows.len() != 2 {
        return Err(format!("selected {} rows, want 2", rows.len()).into());
    }
    let columns = rows[0].columns();
    if columns.len() != 2 || columns[0].name() != "id" || columns[1].name() != "name" {
        return Err("unexpected SQLx result metadata".into());
    }

    let metadata_rows = sqlx::query(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'sqlx_smoke' ORDER BY ordinal_position",
    )
    .fetch_all(&pool)
    .await?;
    let names: Vec<String> = metadata_rows
        .into_iter()
        .map(|row| row.try_get::<String, _>("column_name"))
        .collect::<Result<_, _>>()?;
    if names != ["id".to_string(), "name".to_string()] {
        return Err(format!("unexpected information_schema columns: {names:?}").into());
    }

    pool.close().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    println!("SQLx postgres smoke ok");
    Ok(())
}

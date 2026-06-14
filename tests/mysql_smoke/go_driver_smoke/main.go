package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func mustExec(ctx context.Context, db *sql.DB, query string, args ...any) {
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		panic(fmt.Errorf("exec %q: %w", query, err))
	}
}

func main() {
	dsn := os.Getenv("PG_GATEWAY_MYSQL_DSN")
	if dsn == "" {
		panic("PG_GATEWAY_MYSQL_DSN is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	if err := db.PingContext(ctx); err != nil {
		panic(fmt.Errorf("ping: %w", err))
	}

	mustExec(ctx, db, "DROP TABLE IF EXISTS go_driver_smoke")
	mustExec(ctx, db, "CREATE TABLE go_driver_smoke (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)")
	mustExec(ctx, db, "INSERT INTO go_driver_smoke VALUES (?, ?), (?, ?)", 1, "alice", 2, "bob")

	stmt, err := db.PrepareContext(ctx, "SELECT name FROM go_driver_smoke WHERE id = ?")
	if err != nil {
		panic(fmt.Errorf("prepare: %w", err))
	}
	defer stmt.Close()

	var name string
	if err := stmt.QueryRowContext(ctx, 2).Scan(&name); err != nil {
		panic(fmt.Errorf("prepared query: %w", err))
	}
	if name != "bob" {
		panic(fmt.Errorf("prepared query returned %q, want bob", name))
	}

	rows, err := db.QueryContext(ctx, "SELECT id, name FROM go_driver_smoke ORDER BY id")
	if err != nil {
		panic(fmt.Errorf("select metadata: %w", err))
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		panic(fmt.Errorf("column types: %w", err))
	}
	if len(columnTypes) != 2 || columnTypes[0].Name() != "id" || columnTypes[1].Name() != "name" {
		rows.Close()
		panic(fmt.Errorf("unexpected column metadata: %#v", columnTypes))
	}

	seen := 0
	for rows.Next() {
		var id int
		var rowName string
		if err := rows.Scan(&id, &rowName); err != nil {
			rows.Close()
			panic(fmt.Errorf("scan row: %w", err))
		}
		seen++
	}
	if err := rows.Close(); err != nil {
		panic(fmt.Errorf("close rows: %w", err))
	}
	if seen != 2 {
		panic(fmt.Errorf("selected %d rows, want 2", seen))
	}

	describeRows, err := db.QueryContext(ctx, "DESCRIBE go_driver_smoke")
	if err != nil {
		panic(fmt.Errorf("describe: %w", err))
	}
	describeCols, err := describeRows.Columns()
	if err != nil {
		describeRows.Close()
		panic(fmt.Errorf("describe columns: %w", err))
	}
	if len(describeCols) != 6 {
		describeRows.Close()
		panic(fmt.Errorf("DESCRIBE returned %d columns, want 6", len(describeCols)))
	}
	describeCount := 0
	for describeRows.Next() {
		describeCount++
	}
	if err := describeRows.Close(); err != nil {
		panic(fmt.Errorf("close describe rows: %w", err))
	}
	if describeCount != 2 {
		panic(fmt.Errorf("DESCRIBE returned %d rows, want 2", describeCount))
	}

	fmt.Println("go-sql-driver mysql smoke ok")
}

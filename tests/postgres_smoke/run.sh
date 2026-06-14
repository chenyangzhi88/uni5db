#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$ROOT_DIR"

PSQL_BIN=${PSQL_BIN:-psql}
PGBENCH_BIN=${PGBENCH_BIN:-pgbench}
CARGO_BIN=${CARGO_BIN:-cargo}
MVN_BIN=${MVN_BIN:-mvn}
MVN_ARGS=${MVN_ARGS:--q}
PYTHON_BIN=${PYTHON_BIN:-python3}
HOST=${PG_GATEWAY_POSTGRES_HOST:-127.0.0.1}
PORT=${PG_GATEWAY_POSTGRES_PORT:-55433}
POSTGRES_USER=${PG_GATEWAY_POSTGRES_USER:-postgres}
DATABASE=${PG_GATEWAY_POSTGRES_DATABASE:-defaultdb}
REQUIRE_ALL=${PG_GATEWAY_POSTGRES_SMOKE_REQUIRE_ALL:-0}
WITH_PYTHON=${PG_GATEWAY_POSTGRES_SMOKE_WITH_PYTHON:-auto}
WITH_SQLX=${PG_GATEWAY_POSTGRES_SMOKE_WITH_SQLX:-auto}
WITH_JDBC=${PG_GATEWAY_POSTGRES_SMOKE_WITH_JDBC:-auto}
WITH_PGBENCH=${PG_GATEWAY_POSTGRES_SMOKE_WITH_PGBENCH:-auto}
PGBENCH_SCALE=${PGBENCH_SCALE:-1}
PGBENCH_DURATION=${PGBENCH_DURATION:-2}

TMPDIR=$(mktemp -d)
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

falsy() {
  case "${1:-}" in
    0|false|FALSE|no|NO|off|OFF) return 0 ;;
    *) return 1 ;;
  esac
}

require_or_skip() {
  local label=$1
  if truthy "$REQUIRE_ALL"; then
    echo "$label is required for this smoke run" >&2
    return 1
  fi
  echo "skipping $label smoke; dependency is not available" >&2
  return 0
}

"$PSQL_BIN" --version >/dev/null
"$CARGO_BIN" --version >/dev/null

completed_smokes=()

"$CARGO_BIN" build -p pg_gateway --bin pg_gateway

TARGET_DIR=${CARGO_TARGET_DIR:-target}
if [[ "$TARGET_DIR" != /* ]]; then
  TARGET_DIR="$ROOT_DIR/$TARGET_DIR"
fi
GATEWAY_BIN="$TARGET_DIR/debug/pg_gateway"
SERVER_LOG="$TMPDIR/pg_gateway.log"

PG_GATEWAY_MODE=postgres \
PG_GATEWAY_BACKEND=memory \
PG_GATEWAY_LISTEN_ADDR="$HOST:$PORT" \
"$GATEWAY_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

PSQL_ARGS=(
  -X
  -v
  ON_ERROR_STOP=1
  -h
  "$HOST"
  -p
  "$PORT"
  -U
  "$POSTGRES_USER"
  -d
  "$DATABASE"
)

ready=0
for _ in {1..60}; do
  if "$PSQL_BIN" "${PSQL_ARGS[@]}" -c "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "pg_gateway exited before accepting PostgreSQL connections" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != "1" ]]; then
  echo "timed out waiting for pg_gateway PostgreSQL listener at $HOST:$PORT" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

COPY_FILE="$TMPDIR/copy.tsv"
printf '3\tcarol\n4\tdave\n' >"$COPY_FILE"

"$PSQL_BIN" "${PSQL_ARGS[@]}" <<SQL
DROP TABLE IF EXISTS pg_smoke_cli;
CREATE TABLE pg_smoke_cli (id INT PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO pg_smoke_cli VALUES (1, 'alice'), (2, 'bob');
\copy pg_smoke_cli FROM '$COPY_FILE' WITH (FORMAT text, DELIMITER E'\t')
SQL

row_count=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -c "SELECT COUNT(*) FROM pg_smoke_cli")
if [[ "$row_count" != "4" ]]; then
  echo "COPY row count mismatch: got $row_count, want 4" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

prepared_output=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At <<SQL
PREPARE pg_smoke_lookup(INT) AS SELECT name FROM pg_smoke_cli WHERE id = \$1;
EXECUTE pg_smoke_lookup(2);
SQL
)
prepared_name=$(printf '%s\n' "$prepared_output" | tail -n 1)
if [[ "$prepared_name" != "bob" ]]; then
  echo "SQL PREPARE lookup returned $prepared_name, want bob" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

metadata=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -F $'\t' -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'pg_smoke_cli' ORDER BY ordinal_position")
expected_metadata=$'id\tinteger\nname\ttext'
if [[ "$metadata" != "$expected_metadata" ]]; then
  echo "unexpected information_schema metadata:" >&2
  printf '%s\n' "$metadata" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

catalog=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -c "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'pg_smoke_cli'")
if [[ "$catalog" != "pg_smoke_cli" ]]; then
  echo "pg_catalog.pg_class did not expose pg_smoke_cli" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

"$PSQL_BIN" "${PSQL_ARGS[@]}" -c "ANALYZE pg_smoke_cli" >/dev/null
stats_count=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -c "SELECT COUNT(*) FROM pg_catalog.pg_stats WHERE tablename = 'pg_smoke_cli'")
if [[ "$stats_count" != "2" ]]; then
  echo "pg_stats row count mismatch after ANALYZE: got $stats_count, want 2" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

"$PSQL_BIN" "${PSQL_ARGS[@]}" -c "VACUUM" >/dev/null
"$PSQL_BIN" "${PSQL_ARGS[@]}" -c "VACUUM ANALYZE pg_smoke_cli" >/dev/null
explain_output=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -c "EXPLAIN SELECT name FROM pg_smoke_cli WHERE id = 1")
if [[ "$explain_output" != *"Index Scan"* || "$explain_output" != *"Index Cond"* ]]; then
  echo "EXPLAIN output was unexpected:" >&2
  printf '%s\n' "$explain_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

"$PSQL_BIN" "${PSQL_ARGS[@]}" <<SQL
DROP TABLE IF EXISTS pg_smoke_truncate;
CREATE TABLE pg_smoke_truncate (id INT PRIMARY KEY, name TEXT);
INSERT INTO pg_smoke_truncate VALUES (1, 'alice'), (2, 'bob');
TRUNCATE TABLE pg_smoke_truncate;
SQL
truncate_count=$("$PSQL_BIN" "${PSQL_ARGS[@]}" -At -c "SELECT COUNT(*) FROM pg_smoke_truncate")
if [[ "$truncate_count" != "0" ]]; then
  echo "TRUNCATE row count mismatch: got $truncate_count, want 0" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

completed_smokes+=("psql" "COPY" "prepared" "metadata" "ANALYZE" "VACUUM" "EXPLAIN" "TRUNCATE")

run_pgbench=0
require_pgbench=1
if truthy "$WITH_PGBENCH"; then
  run_pgbench=1
elif falsy "$WITH_PGBENCH"; then
  run_pgbench=0
  require_pgbench=0
elif command -v "$PGBENCH_BIN" >/dev/null 2>&1; then
  run_pgbench=1
fi

if [[ "$run_pgbench" == "1" ]]; then
  PGHOST="$HOST" \
  PGPORT="$PORT" \
  PGUSER="$POSTGRES_USER" \
  PGDATABASE="$DATABASE" \
  PGBENCH_SCALE="$PGBENCH_SCALE" \
  PGBENCH_DURATION="$PGBENCH_DURATION" \
  PGBENCH_BIN="$PGBENCH_BIN" \
  pg_gateway/tests/postgres_smoke/pgbench_smoke.sh
  completed_smokes+=("pgbench")
else
  if [[ "$require_pgbench" == "1" ]]; then
    require_or_skip "pgbench"
  fi
fi

POSTGRES_DSN="postgresql://${POSTGRES_USER}@${HOST}:${PORT}/${DATABASE}"

run_python=0
require_python=1
if truthy "$WITH_PYTHON"; then
  run_python=1
elif falsy "$WITH_PYTHON"; then
  run_python=0
  require_python=0
elif "$PYTHON_BIN" -c 'import psycopg, sqlalchemy' >/dev/null 2>&1; then
  run_python=1
fi

if [[ "$run_python" == "1" ]]; then
  PG_GATEWAY_POSTGRES_DSN="$POSTGRES_DSN" \
  PG_GATEWAY_SQLALCHEMY_DSN="postgresql+psycopg://${POSTGRES_USER}@${HOST}:${PORT}/${DATABASE}" \
  "$PYTHON_BIN" "$ROOT_DIR/pg_gateway/tests/postgres_smoke/python_smoke/postgres_python_smoke.py"
  completed_smokes+=("psycopg" "SQLAlchemy")
else
  if [[ "$require_python" == "1" ]]; then
    require_or_skip "Python psycopg/SQLAlchemy"
  fi
fi

run_sqlx=0
require_sqlx=1
if truthy "$WITH_SQLX"; then
  run_sqlx=1
elif falsy "$WITH_SQLX"; then
  run_sqlx=0
  require_sqlx=0
elif "$CARGO_BIN" metadata --manifest-path "$ROOT_DIR/pg_gateway/tests/postgres_smoke/sqlx_smoke/Cargo.toml" --no-deps >/dev/null 2>&1; then
  run_sqlx=1
fi

if [[ "$run_sqlx" == "1" ]]; then
  (
    cd "$ROOT_DIR/pg_gateway/tests/postgres_smoke/sqlx_smoke"
    CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target/postgres-smoke-sqlx}" \
    DATABASE_URL="$POSTGRES_DSN" \
    "$CARGO_BIN" run --quiet
  )
  completed_smokes+=("SQLx")
else
  if [[ "$require_sqlx" == "1" ]]; then
    require_or_skip "SQLx"
  fi
fi

run_jdbc=0
require_jdbc=1
if truthy "$WITH_JDBC"; then
  run_jdbc=1
elif falsy "$WITH_JDBC"; then
  run_jdbc=0
  require_jdbc=0
elif command -v "$MVN_BIN" >/dev/null 2>&1; then
  run_jdbc=1
fi

if [[ "$run_jdbc" == "1" ]]; then
  (
    cd "$ROOT_DIR/pg_gateway/tests/postgres_smoke/jdbc_smoke"
    PG_GATEWAY_POSTGRES_JDBC_URL="jdbc:postgresql://${HOST}:${PORT}/${DATABASE}" \
    PG_GATEWAY_POSTGRES_USER="$POSTGRES_USER" \
    "$MVN_BIN" $MVN_ARGS compile exec:java
  )
  completed_smokes+=("JDBC")
else
  if [[ "$require_jdbc" == "1" ]]; then
    require_or_skip "JDBC"
  fi
fi

printf 'postgres smoke ok: %s\n' "${completed_smokes[*]}"

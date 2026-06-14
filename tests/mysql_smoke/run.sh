#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$ROOT_DIR"

MYSQL_BIN=${MYSQL_BIN:-/usr/bin/mysql}
GO_BIN=${GO_BIN:-go}
MVN_BIN=${MVN_BIN:-mvn}
MVN_ARGS=${MVN_ARGS:--q}
PYTHON_BIN=${PYTHON_BIN:-python3}
HOST=${PG_GATEWAY_MYSQL_HOST:-127.0.0.1}
PORT=${PG_GATEWAY_MYSQL_PORT:-33307}
MYSQL_USER_NAME=${PG_GATEWAY_MYSQL_USER:-root}
MYSQL_PASSWORD_VALUE=${PG_GATEWAY_MYSQL_PASSWORD:-}
DATABASE=${PG_GATEWAY_MYSQL_DATABASE:-defaultdb}
REQUIRE_ALL=${PG_GATEWAY_MYSQL_SMOKE_REQUIRE_ALL:-0}
WITH_JDBC=${PG_GATEWAY_MYSQL_SMOKE_WITH_JDBC:-auto}
WITH_MYSQLCLIENT=${PG_GATEWAY_MYSQL_SMOKE_WITH_MYSQLCLIENT:-auto}

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

"$MYSQL_BIN" --version >/dev/null
"$GO_BIN" version >/dev/null

truthy() {
  case "$1" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

falsy() {
  case "$1" in
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

completed_smokes=()

cargo build -p pg_gateway --bin pg_gateway

TARGET_DIR=${CARGO_TARGET_DIR:-target}
if [[ "$TARGET_DIR" != /* ]]; then
  TARGET_DIR="$ROOT_DIR/$TARGET_DIR"
fi
GATEWAY_BIN="$TARGET_DIR/debug/pg_gateway"
SERVER_LOG="$TMPDIR/pg_gateway.log"

PG_GATEWAY_MODE=mysql \
PG_GATEWAY_BACKEND=memory \
PG_GATEWAY_LISTEN_ADDR="$HOST:$PORT" \
"$GATEWAY_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

MYSQL_ARGS=(
  --protocol=tcp
  --host="$HOST"
  --port="$PORT"
  --user="$MYSQL_USER_NAME"
  --database="$DATABASE"
  --local-infile=1
  --batch
  --raw
  --skip-column-names
)
if [[ -n "$MYSQL_PASSWORD_VALUE" ]]; then
  MYSQL_ARGS+=(--password="$MYSQL_PASSWORD_VALUE")
fi

ready=0
for _ in {1..60}; do
  if "$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "pg_gateway exited before accepting MySQL connections" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != "1" ]]; then
  echo "timed out waiting for pg_gateway MySQL listener at $HOST:$PORT" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

LOAD_FILE="$TMPDIR/load.tsv"
printf '3\tcarol\n4\tdave\n' >"$LOAD_FILE"

"$MYSQL_BIN" "${MYSQL_ARGS[@]}" <<SQL
DROP TABLE IF EXISTS cli_load_smoke;
CREATE TABLE cli_load_smoke (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL);
INSERT INTO cli_load_smoke VALUES (1, 'alice'), (2, 'bob');
LOAD DATA LOCAL INFILE '$LOAD_FILE' INTO TABLE cli_load_smoke FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
SQL

row_count=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "SELECT COUNT(*) FROM cli_load_smoke")
if [[ "$row_count" != "4" ]]; then
  echo "LOAD DATA LOCAL row count mismatch: got $row_count, want 4" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

rows=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "SELECT id, name FROM cli_load_smoke ORDER BY id")
expected_rows=$'1\talice\n2\tbob\n3\tcarol\n4\tdave'
if [[ "$rows" != "$expected_rows" ]]; then
  echo "unexpected CLI rows:" >&2
  printf '%s\n' "$rows" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

describe_output=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "DESCRIBE cli_load_smoke")
if [[ "$describe_output" != *$'id\t'* || "$describe_output" != *$'name\t'* ]]; then
  echo "DESCRIBE output did not include expected columns:" >&2
  printf '%s\n' "$describe_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

show_columns_output=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "SHOW COLUMNS FROM cli_load_smoke")
if [[ "$show_columns_output" != *$'id\t'* || "$show_columns_output" != *$'name\t'* ]]; then
  echo "SHOW COLUMNS output did not include expected columns:" >&2
  printf '%s\n' "$show_columns_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

explain_output=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "EXPLAIN SELECT name FROM cli_load_smoke WHERE id = 1")
if [[ "$explain_output" != *$'cli_load_smoke\t'* || "$explain_output" != *$'PRIMARY\t'* ]]; then
  echo "EXPLAIN output did not include expected table/key:" >&2
  printf '%s\n' "$explain_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

analyze_output=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "ANALYZE TABLE cli_load_smoke")
if [[ "$analyze_output" != *$'\tanalyze\tstatus\tOK' ]]; then
  echo "ANALYZE TABLE output was unexpected:" >&2
  printf '%s\n' "$analyze_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

optimize_output=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "OPTIMIZE TABLE cli_load_smoke")
if [[ "$optimize_output" != *$'\toptimize\tstatus\tOK' ]]; then
  echo "OPTIMIZE TABLE output was unexpected:" >&2
  printf '%s\n' "$optimize_output" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

"$MYSQL_BIN" "${MYSQL_ARGS[@]}" <<SQL
DROP TABLE IF EXISTS cli_truncate_smoke;
CREATE TABLE cli_truncate_smoke (id INT PRIMARY KEY, name VARCHAR(32));
INSERT INTO cli_truncate_smoke VALUES (1, 'alice'), (2, 'bob');
TRUNCATE TABLE cli_truncate_smoke;
SQL
truncate_count=$("$MYSQL_BIN" "${MYSQL_ARGS[@]}" -e "SELECT COUNT(*) FROM cli_truncate_smoke")
if [[ "$truncate_count" != "0" ]]; then
  echo "TRUNCATE row count mismatch: got $truncate_count, want 0" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

completed_smokes+=("mysql CLI" "LOAD DATA LOCAL" "metadata" "EXPLAIN" "ANALYZE" "OPTIMIZE" "TRUNCATE")

if [[ -n "$MYSQL_PASSWORD_VALUE" ]]; then
  GO_DSN="${MYSQL_USER_NAME}:${MYSQL_PASSWORD_VALUE}@tcp(${HOST}:${PORT})/${DATABASE}?allowNativePasswords=true&multiStatements=true&parseTime=true"
else
  GO_DSN="${MYSQL_USER_NAME}@tcp(${HOST}:${PORT})/${DATABASE}?allowNativePasswords=true&multiStatements=true&parseTime=true"
fi

(
  cd "$ROOT_DIR/pg_gateway/tests/mysql_smoke/go_driver_smoke"
  GOCACHE="${GOCACHE:-$TMPDIR/go-cache}" \
  PG_GATEWAY_MYSQL_DSN="${PG_GATEWAY_MYSQL_DSN:-$GO_DSN}" \
  "$GO_BIN" run .
)
completed_smokes+=("Go driver")

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
    cd "$ROOT_DIR/pg_gateway/tests/mysql_smoke/jdbc_smoke"
    "$MVN_BIN" $MVN_ARGS compile exec:java
  )
  completed_smokes+=("JDBC")
else
  if [[ "$require_jdbc" == "1" ]]; then
    require_or_skip "JDBC"
  fi
fi

run_mysqlclient=0
require_mysqlclient=1
if truthy "$WITH_MYSQLCLIENT"; then
  run_mysqlclient=1
elif falsy "$WITH_MYSQLCLIENT"; then
  run_mysqlclient=0
  require_mysqlclient=0
elif "$PYTHON_BIN" -c 'import MySQLdb' >/dev/null 2>&1; then
  run_mysqlclient=1
fi

if [[ "$run_mysqlclient" == "1" ]]; then
  "$PYTHON_BIN" "$ROOT_DIR/pg_gateway/tests/mysql_smoke/python_mysqlclient_smoke/mysqlclient_smoke.py"
  completed_smokes+=("python mysqlclient")
else
  if [[ "$require_mysqlclient" == "1" ]]; then
    require_or_skip "python mysqlclient"
  fi
fi

printf 'mysql smoke ok: %s\n' "${completed_smokes[*]}"

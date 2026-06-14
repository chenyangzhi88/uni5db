#!/usr/bin/env bash
set -euo pipefail

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=55433}"
: "${PGUSER:=postgres}"
: "${PGDATABASE:=defaultdb}"
: "${PGBENCH_SCALE:=1}"
: "${PGBENCH_DURATION:=2}"
: "${PGBENCH_BIN:=pgbench}"
: "${PSQL_BIN:=psql}"

"$PGBENCH_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -i -n -s "$PGBENCH_SCALE"
"$PGBENCH_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -M simple -c 1 -T "$PGBENCH_DURATION"
"$PGBENCH_BIN" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -M prepared -c 1 -T "$PGBENCH_DURATION"
"$PSQL_BIN" -X -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f pg_gateway/tests/pgbench/phase_a3_consistency.sql >/dev/null

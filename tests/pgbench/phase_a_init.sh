#!/usr/bin/env bash
set -euo pipefail

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=55432}"
: "${PGUSER:=postgres}"
: "${PGDATABASE:=defaultdb}"
: "${PGBENCH_SCALE:=1}"

pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -i -s "$PGBENCH_SCALE"

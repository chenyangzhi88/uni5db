#!/usr/bin/env bash
set -euo pipefail

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=55432}"
: "${PGUSER:=postgres}"
: "${PGDATABASE:=defaultdb}"
: "${PGBENCH_SCALE:=1}"
: "${PGBENCH_DURATION:=10}"

pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -i -s "$PGBENCH_SCALE"
pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -M simple -c 1 -T "$PGBENCH_DURATION"
pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -M prepared -c 1 -T "$PGBENCH_DURATION"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f tests/pgbench/phase_a3_consistency.sql

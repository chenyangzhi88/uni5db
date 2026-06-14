#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$ROOT_DIR"

cargo test -p pg_gateway --lib

PG_GATEWAY_MYSQL_SMOKE_REQUIRE_ALL="${PG_GATEWAY_MYSQL_SMOKE_REQUIRE_ALL:-1}" \
PG_GATEWAY_MYSQL_SMOKE_WITH_JDBC="${PG_GATEWAY_MYSQL_SMOKE_WITH_JDBC:-1}" \
PG_GATEWAY_MYSQL_SMOKE_WITH_MYSQLCLIENT="${PG_GATEWAY_MYSQL_SMOKE_WITH_MYSQLCLIENT:-1}" \
pg_gateway/tests/mysql_smoke/run.sh


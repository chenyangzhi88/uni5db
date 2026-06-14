#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

run_step() {
  local name=$1
  shift
  printf '\n==> %s\n' "$name"
  "$@"
}

RUN_CARGO_CHECK=${RUN_CARGO_CHECK:-1}
RUN_CARGO_TEST=${RUN_CARGO_TEST:-1}

if truthy "$RUN_CARGO_CHECK"; then
  run_step "workspace cargo check" cargo check --workspace
fi

if truthy "$RUN_CARGO_TEST"; then
  run_step "workspace cargo test" cargo test --workspace
fi

printf '\nlocal pre-submit ok\n'

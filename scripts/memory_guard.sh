#!/usr/bin/env bash
set -euo pipefail

# Kill known UniDB processes if their RSS exceeds the configured limit.
# Defaults: 16 GiB limit, 1 second polling interval.

LIMIT_GB="${MEM_GUARD_LIMIT_GB:-16}"
INTERVAL_SECONDS="${MEM_GUARD_INTERVAL_SECONDS:-1}"
LOG_FILE="${MEM_GUARD_LOG_FILE:-.run/memory_guard.log}"
GRACE_SECONDS="${MEM_GUARD_GRACE_SECONDS:-2}"

WATCH_NAMES=(
  "db_bench"
  "kv_engine"
  "onedis-server"
  "pg_gateway"
)

LIMIT_KB=$((LIMIT_GB * 1024 * 1024))
LOCK_DIR="${MEM_GUARD_LOCK_DIR:-.run/memory_guard.lock}"

mkdir -p "$(dirname "$LOG_FILE")"

while ! mkdir "$LOCK_DIR" 2>/dev/null; do
  existing_pid=""
  if [[ -r "$LOCK_DIR/pid" ]]; then
    existing_pid="$(<"$LOCK_DIR/pid")"
  fi

  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "$(date '+%F %T') memory_guard already running pid=$existing_pid; lock exists at $LOCK_DIR" >>"$LOG_FILE"
    exit 0
  fi

  echo "$(date '+%F %T') removing stale memory_guard lock at $LOCK_DIR" >>"$LOG_FILE"
  rm -f "$LOCK_DIR/pid"
  rmdir "$LOCK_DIR" 2>/dev/null || sleep 1
done

echo "$$" >"$LOCK_DIR/pid"

cleanup() {
  rm -f "$LOCK_DIR/pid"
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

log() {
  echo "$(date '+%F %T') $*" >>"$LOG_FILE"
}

is_watched_name() {
  local name="$1"
  local watched
  for watched in "${WATCH_NAMES[@]}"; do
    if [[ "$name" == "$watched" ]]; then
      return 0
    fi
  done
  return 1
}

terminate_process() {
  local pid="$1"
  local rss_kb="$2"
  local comm="$3"
  local args="$4"

  log "killing pid=$pid comm=$comm rss_kb=$rss_kb limit_kb=$LIMIT_KB args=$args"
  kill "$pid" 2>/dev/null || return 0

  sleep "$GRACE_SECONDS"
  if kill -0 "$pid" 2>/dev/null; then
    log "pid=$pid still alive after ${GRACE_SECONDS}s; sending SIGKILL"
    kill -9 "$pid" 2>/dev/null || true
  fi
}

log "memory_guard started limit_gb=$LIMIT_GB interval_seconds=$INTERVAL_SECONDS watched=${WATCH_NAMES[*]}"

while true; do
  while read -r pid rss_kb comm args; do
    [[ -n "${pid:-}" && -n "${rss_kb:-}" && -n "${comm:-}" ]] || continue
    [[ "$pid" != "$$" ]] || continue

    if is_watched_name "$comm" && (( rss_kb >= LIMIT_KB )); then
      terminate_process "$pid" "$rss_kb" "$comm" "$args"
    fi
  done < <(ps -eo pid=,rss=,comm=,args=)

  sleep "$INTERVAL_SECONDS"
done

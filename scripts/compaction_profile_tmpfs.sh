#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
TMPFS_DIR=${TMPFS_DIR:-/mnt/unidb_compaction_tmpfs}
TMPFS_SIZE=${TMPFS_SIZE:-64G}
WORK_ROOT=${WORK_ROOT:-$TMPFS_DIR/unidb_compaction_profile}
SEED_DB=${SEED_DB:-$WORK_ROOT/seed_db}
RUN_DB=${RUN_DB:-$WORK_ROOT/run_db}
BACKUP_ROOT=${BACKUP_ROOT:-/nvme_data_0/unidb_compaction_profile_backups}
LOG_ROOT=${LOG_ROOT:-/nvme_data_0/unidb_compaction_profile_logs}
RUN_ID=${RUN_ID:-$(date +%Y%m%d_%H%M%S)}

BASE_NUM=${BASE_NUM:-100000000}
KEY_SIZE=${KEY_SIZE:-20}
VALUE_SIZE=${VALUE_SIZE:-16}
BATCH_SIZE=${BATCH_SIZE:-1024}
BASE_BENCH=${BASE_BENCH:-fillrandom}
THREADS=${THREADS:-16}
PIPELINE_DEPTH=${PIPELINE_DEPTH:-4096}
DB_BENCH=${DB_BENCH:-$REPO_DIR/target/release/db_bench}

BUILD=${BUILD:-1}
RECREATE_BASELINE=${RECREATE_BASELINE:-0}
PERF=${PERF:-0}
PERF_FREQ=${PERF_FREQ:-99}
PERF_CALLGRAPH=${PERF_CALLGRAPH:-dwarf}
SUDO_PERF=${SUDO_PERF:-0}
RUN_SECOND_STAGE=${RUN_SECOND_STAGE:-1}
RESTORE_RUN_DB_FROM=${RESTORE_RUN_DB_FROM:-}
DISABLE_VALUE_SHED=${DISABLE_VALUE_SHED:-0}
MEMORY_GUARD=${MEMORY_GUARD:-1}
MEM_GUARD_LIMIT_GB=${MEM_GUARD_LIMIT_GB:-16}
MEM_GUARD_INTERVAL_SECONDS=${MEM_GUARD_INTERVAL_SECONDS:-1}

LOG_DIR="$LOG_ROOT/$RUN_ID"
BASELINE_MARKER="$SEED_DB/.baseline_100m_compacted"
BASELINE_BACKUP="$BACKUP_ROOT/baseline_100m_compacted_$RUN_ID"
RUN_BACKUP="$BACKUP_ROOT/pre_append_run_db_$RUN_ID"

mkdir -p "$LOG_DIR" "$BACKUP_ROOT"

MEMORY_GUARD_PID=""
cleanup() {
  if [[ -n "$MEMORY_GUARD_PID" ]] && kill -0 "$MEMORY_GUARD_PID" 2>/dev/null; then
    kill "$MEMORY_GUARD_PID" 2>/dev/null || true
    wait "$MEMORY_GUARD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT TERM

if [[ "$MEMORY_GUARD" == "1" ]]; then
  echo "starting memory guard: limit=${MEM_GUARD_LIMIT_GB}GiB interval=${MEM_GUARD_INTERVAL_SECONDS}s"
  MEM_GUARD_LIMIT_GB="$MEM_GUARD_LIMIT_GB" \
    MEM_GUARD_INTERVAL_SECONDS="$MEM_GUARD_INTERVAL_SECONDS" \
    MEM_GUARD_LOG_FILE="$LOG_DIR/memory_guard.log" \
    MEM_GUARD_LOCK_DIR="$LOG_DIR/memory_guard.lock" \
    "$REPO_DIR/scripts/memory_guard.sh" &
  MEMORY_GUARD_PID=$!
fi

if [[ "$BUILD" == "1" ]]; then
  cargo build -p kv_engine_benchmarks --release --bin db_bench
fi

if [[ ! -x "$DB_BENCH" ]]; then
  echo "db_bench binary not found or not executable: $DB_BENCH" >&2
  exit 1
fi

if ! mountpoint -q "$TMPFS_DIR"; then
  sudo mkdir -p "$TMPFS_DIR"
  sudo mount -t tmpfs -o "size=$TMPFS_SIZE,mode=0777" tmpfs "$TMPFS_DIR"
fi

mkdir -p "$WORK_ROOT"

if [[ "$RECREATE_BASELINE" == "1" || ! -f "$BASELINE_MARKER" ]]; then
  rm -rf "$SEED_DB"
  echo "creating compacted baseline: bench=$BASE_BENCH num=$BASE_NUM db=$SEED_DB"
  env RUST_LOG="${RUST_LOG:-info}" \
    "$DB_BENCH" \
    --benchmarks "$BASE_BENCH" \
    --db "$SEED_DB" \
    --num "$BASE_NUM" \
    --key_size "$KEY_SIZE" \
    --value_size "$VALUE_SIZE" \
    --batch_size "$BATCH_SIZE" \
    --threads "$THREADS" \
    --pipeline_depth "$PIPELINE_DEPTH" \
    --final_manual_compaction \
    2>&1 | tee "$LOG_DIR/01_seed_${BASE_BENCH}_100m.log"
  touch "$BASELINE_MARKER"
fi

echo "backing up compacted baseline before append compaction: $BASELINE_BACKUP"
rsync -a --delete "$SEED_DB/" "$BASELINE_BACKUP/"

if [[ "$RUN_SECOND_STAGE" != "1" ]]; then
  echo "first stage completed; RUN_SECOND_STAGE=$RUN_SECOND_STAGE so append/compaction stage is skipped"
  echo "logs: $LOG_DIR"
  exit 0
fi

rm -rf "$RUN_DB"
mkdir -p "$RUN_DB"
if [[ -n "$RESTORE_RUN_DB_FROM" ]]; then
  echo "restoring run db from existing backup: $RESTORE_RUN_DB_FROM"
  rsync -a --delete "$RESTORE_RUN_DB_FROM/" "$RUN_DB/"
else
  rsync -a --delete "$SEED_DB/" "$RUN_DB/"
fi

echo "backing up run db before 2GiB append: $RUN_BACKUP"
rsync -a --delete "$RUN_DB/" "$RUN_BACKUP/"

COMPACT_CMD=(
  "$DB_BENCH"
  --benchmarks compact_2g
  --db "$RUN_DB"
  --use_existing_db
  --key_offset "$BASE_NUM"
  --key_size "$KEY_SIZE"
  --value_size "$VALUE_SIZE"
  --batch_size "$BATCH_SIZE"
  --disable_auto_compaction
)

if [[ "$DISABLE_VALUE_SHED" == "1" ]]; then
  COMPACT_CMD+=(--disable_value_shed)
fi

echo "appending 2GiB without mid-way compaction, then running one manual compaction"
if [[ "$PERF" == "1" ]]; then
  PERF_CMD=(perf record -F "$PERF_FREQ" -g --call-graph "$PERF_CALLGRAPH" -o "$LOG_DIR/perf.data" --)
  if [[ "$SUDO_PERF" == "1" ]]; then
    env RUST_LOG="${RUST_LOG:-info}" \
      UNIDB_COMPACTION_TRACE="${UNIDB_COMPACTION_TRACE:-1}" \
      UNIDB_COMPACTION_LEVEL_STATS="${UNIDB_COMPACTION_LEVEL_STATS:-1}" \
      sudo "${PERF_CMD[@]}" "${COMPACT_CMD[@]}" \
      2>&1 | tee "$LOG_DIR/02_append_2g_manual_compaction.log"
  else
    env RUST_LOG="${RUST_LOG:-info}" \
      UNIDB_COMPACTION_TRACE="${UNIDB_COMPACTION_TRACE:-1}" \
      UNIDB_COMPACTION_LEVEL_STATS="${UNIDB_COMPACTION_LEVEL_STATS:-1}" \
      "${PERF_CMD[@]}" "${COMPACT_CMD[@]}" \
      2>&1 | tee "$LOG_DIR/02_append_2g_manual_compaction.log"
  fi
else
  env RUST_LOG="${RUST_LOG:-info}" \
    UNIDB_COMPACTION_TRACE="${UNIDB_COMPACTION_TRACE:-1}" \
    UNIDB_COMPACTION_LEVEL_STATS="${UNIDB_COMPACTION_LEVEL_STATS:-1}" \
    "${COMPACT_CMD[@]}" \
    2>&1 | tee "$LOG_DIR/02_append_2g_manual_compaction.log"
fi

{
  echo "run_id=$RUN_ID"
  echo "tmpfs_dir=$TMPFS_DIR"
  echo "seed_db=$SEED_DB"
  echo "run_db=$RUN_DB"
  echo "baseline_backup=$BASELINE_BACKUP"
  echo "run_backup=$RUN_BACKUP"
  echo
  grep -E "compact_2g:|compaction_profile\\[compact_2g|memtables drained|manual_compaction_done" \
    "$LOG_DIR/02_append_2g_manual_compaction.log" || true
} | tee "$LOG_DIR/summary.txt"

echo "logs: $LOG_DIR"

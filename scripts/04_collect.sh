#!/usr/bin/env bash
# Collect status + logs for the last cpu/gpu runs (or given instance ids),
# then extract the [bench] summary tables from the driver logs.
#   ./scripts/04_collect.sh [cpu-instance-id] [gpu-instance-id]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

CPU_ID="${1:-$(cat "$RESULTS_DIR/last_cpu.id" 2>/dev/null || true)}"
GPU_ID="${2:-$(cat "$RESULTS_DIR/last_gpu.id" 2>/dev/null || true)}"

collect() {
  local label="$1" id="$2" dir="$RESULTS_DIR/logs-$label-$id"
  [[ -z "$id" ]] && { echo "no instance id for $label"; return; }
  echo "== $label  $id"
  dpe instance status --format=table "$id" 2>/dev/null | grep -v jline || true
  dpe instance logs "$id" --zip-file "$RESULTS_DIR/$label-logs.zip" 2>/dev/null \
    | grep -v 'jline\|WARNING' || true
  rm -rf "$dir" && mkdir -p "$dir"
  unzip -oq "$RESULTS_DIR/$label-logs.zip" -d "$dir" 2>/dev/null || true
  echo "-- driver summary ([bench] lines) --"
  grep -h '\[bench\]' "$dir"/*-j-*.log 2>/dev/null || \
    echo "(driver log not found in zip)"
  echo
}

collect cpu "$CPU_ID"
collect gpu "$GPU_ID"

#!/usr/bin/env bash
# Full ramp: warmup 20u/2m (discarded), steps 60 -> 120 -> 240 -> 360 -> 480,
# 3 min each except a 10-min hold at 360. Monitor samples Trino every 5s.
#
#   ./concurrency/run_ramp.sh [--headless]
#
# With --headless, runs locust --headless -u N -r N -t Ts per step and keeps
# CSV stats per step in concurrency/results/. Without it, locust starts with
# the web UI and YOU drive the user count from the browser while the script
# prints each step's timing — useful for the live demo.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/../scripts/lib.sh"
load_env
require_vars DDAE_USER DDAE_PASSWORD

export LOCUST_HOST="dummy"   # trino target comes from env, not --host

LOG="$HERE/results/ramp.log"
mkdir -p "$HERE/results"
: > "$LOG"

step() {
  local users="$1" secs="$2" tag="$3"
  echo "== STEP $tag: users=$users hold=${secs}s ($(date +%H:%M:%S))" | tee -a "$LOG"
  if [[ "${HEADLESS:-0}" == "1" ]]; then
    python3 -m locust -f "$HERE/locustfile.py" --headless \
      -u "$users" -r "$users" -t "${secs}s" \
      --csv "$HERE/results/locust-${tag}" --only-summary 2>&1 | tee -a "$LOG" || true
  else
    echo "   -> drive locust UI to $users users now; hold until $(date -d "+${secs} sec" +%H:%M:%S 2>/dev/null || echo "+${secs}s")" | tee -a "$LOG"
    sleep "$secs"
  fi
}

if [[ "${1:-}" == "--headless" ]]; then HEADLESS=1; else HEADLESS=0; fi

echo "start monitor -> results/monitor.csv" | tee -a "$LOG"
python3 "$HERE/monitor.py" 5 0 &
MON_PID=$!
trap 'kill $MON_PID 2>/dev/null || true' EXIT

if [[ "$HEADLESS" == "0" ]]; then
  echo "launching locust web UI on :8089" | tee -a "$LOG"
  python3 -m locust -f "$HERE/locustfile.py" &
  LOCU_PID=$!
  trap 'kill $MON_PID $LOCU_PID 2>/dev/null || true' EXIT
fi

step 20 120 warmup
step 60 180 step060
step 120 180 step120
step 240 180 step240
step 360 600 step360
step 480 180 step480

echo "ramp done $(date +%H:%M:%S)" | tee -a "$LOG"
kill $MON_PID 2>/dev/null || true

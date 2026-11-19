#!/usr/bin/env bash
# Submit the benchmark on the GPU pool. RUN_ID labels the results rows.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
require_vars OBS_ACCESS_KEY OBS_SECRET_KEY

RUN_ID="${RUN_ID:-gpu-$(date -u +%Y%m%dT%H%M%SZ)}"

conf_to_args "$REPO_ROOT/conf/common.conf" "$REPO_ROOT/conf/gpu.conf"

out=$(dpe submit --ttl-seconds-after-finished=86400 --pool "$GPU_POOL" --name "aidp-etl-gpu" \
  "${CONF_ARGS[@]}" \
  -c "spark.demo.run_id=$RUN_ID" \
  "$REPO_ROOT/jobs/etl_benchmark.py" 2>&1 | grep -v 'jline\|WARNING')
echo "$out"
id=$(echo "$out" | extract_instance_id)
echo "instanceId: $id  run_id: $RUN_ID"
echo "$id" > "$RESULTS_DIR/last_gpu.id"
echo "$RUN_ID" > "$RESULTS_DIR/last_gpu.run_id"
echo "watch : dell-data-processing-engine instance status --insecure $id"
echo "logs  : dell-data-processing-engine instance logs --insecure $id   (live)"
echo "        dell-data-processing-engine instance logs --insecure $id --zip-file gpu-logs.zip"
echo "ui    : https://aidp-etl-gpu-ui.ddpe.lab9bgp.com (while RUNNING)"

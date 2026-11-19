#!/usr/bin/env bash
# Submit the datagen job. ROWS controls scale (10M dry run, 100M-500M demo).
#   ROWS=10000000 ./scripts/01_generate_data.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
require_vars OBS_ACCESS_KEY OBS_SECRET_KEY

ROWS="${ROWS:-100000000}"
TXN_FILES="${TXN_FILES:-200}"

conf_to_args "$REPO_ROOT/conf/common.conf"
# datagen is I/O bound; it does not need the benchmark's fixed executor shape
# but common.conf keeps it simple and identical for the demo.

out=$(dpe submit --ttl-seconds-after-finished=86400 --pool "$CPU_POOL" --name "aidp-datagen" \
  "${CONF_ARGS[@]}" \
  -c "spark.demo.rows=$ROWS" \
  -c "spark.demo.txn.files=$TXN_FILES" \
  "$REPO_ROOT/jobs/datagen.py" 2>&1 | grep -v 'jline\|WARNING')
echo "$out"
id=$(echo "$out" | extract_instance_id)
echo "instanceId: $id"
echo "$id" > "$RESULTS_DIR/last_datagen.id"
echo "watch : dell-data-processing-engine instance status --insecure $id"
echo "ui    : https://aidp-datagen-ui.ddpe.lab9bgp.com (while RUNNING)"

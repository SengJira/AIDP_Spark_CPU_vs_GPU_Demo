#!/usr/bin/env bash
# Shared helpers for the AIDP CPU-vs-GPU demo scripts.
# shellcheck disable=SC1091

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/results"
mkdir -p "$RESULTS_DIR"

# ---------------------------------------------------------------------------
# Environment loading + name mapping
# ---------------------------------------------------------------------------
# Priority: repo .env (gitignored) -> ~/.cnc-demo-credentials -> process env.
# Both the new names (.env.example) and the lab's legacy names are accepted.
load_env() {
  if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a; source "$REPO_ROOT/.env"; set +a
  elif [[ -f "$HOME/.cnc-demo-credentials" ]]; then
    source "$HOME/.cnc-demo-credentials"
  fi

  # legacy lab names -> demo names
  : "${OBS_ACCESS_KEY:=${S3_ACCESS_KEY:-}}"
  : "${OBS_SECRET_KEY:=${S3_SECRET_KEY:-}}"
  : "${OBS_ENDPOINT:=${S3_ENDPOINT:-http://172.18.11.31:9020}}"
  : "${DPE_USER:=${TRINO_USER:-}}"
  : "${DPE_PASSWORD:=${TRINO_PASS:-}}"
  : "${DDAE_USER:=${TRINO_USER:-}}"
  : "${DDAE_PASSWORD:=${TRINO_PASS:-}}"

  : "${DEMO_BUCKET:=js-demo}"
  : "${CPU_POOL:=default}"
  : "${GPU_POOL:=NVIDIA-L4}"
  : "${DPE_ROLE:=jirawut_demo}"
  : "${DPE_API_ENDPOINT:=https://ddpe-http.ddpe.lab9bgp.com}"
  : "${DPE_CLI:=$HOME/jirawut/dell-data-processing-engine/bin/dell-data-processing-engine}"
  : "${DPE_JAVA_HOME:=${JAVA_HOME:-/home/betalab/workspace/tk/openjdk/jdk-27}}"

  export OBS_ACCESS_KEY OBS_SECRET_KEY OBS_ENDPOINT DPE_USER DPE_PASSWORD
  export DDAE_USER DDAE_PASSWORD DEMO_BUCKET CPU_POOL GPU_POOL DPE_ROLE
}

require_vars() {
  local missing=()
  for v in "$@"; do
    [[ -z "${!v:-}" ]] && missing+=("$v")
  done
  if ((${#missing[@]})); then
    echo "Missing env vars: ${missing[*]}" >&2
    echo "Copy .env.example to .env and fill it in." >&2
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# DPE CLI wrapper (the bundled CLI needs a Java >= 22 runtime)
# ---------------------------------------------------------------------------
dpe() {
  JAVA_HOME="$DPE_JAVA_HOME" "$DPE_CLI" --insecure "$@"
}

dpe_login() {
  require_vars DPE_USER DPE_PASSWORD DPE_CLI
  [[ -x "$DPE_CLI" ]] || { echo "DPE CLI not found: $DPE_CLI" >&2; exit 1; }
  dpe config set --api-endpoint="$DPE_API_ENDPOINT" >/dev/null
  dpe login-with-credentials -u "$DPE_USER" -P "$DPE_PASSWORD" --role "$DPE_ROLE" \
    | grep -v 'jline\|WARNING' || true
}

# ---------------------------------------------------------------------------
# conf files -> -c args. Reads one or more key=value conf files, ignoring
# comments/blanks, and fills the CONF_ARGS array. Secrets come from env.
# ---------------------------------------------------------------------------
declare -a CONF_ARGS=()
conf_to_args() {
  CONF_ARGS=()
  local f line
  for f in "$@"; do
    while IFS= read -r line; do
      line="${line%%#*}"; line="$(echo "$line" | xargs)"
      [[ -z "$line" ]] && continue
      CONF_ARGS+=(-c "$line")
    done < "$f"
  done
  # s3a credentials + demo paths from env — never from files
  CONF_ARGS+=(
    -c "spark.hadoop.fs.s3a.access.key=$OBS_ACCESS_KEY"
    -c "spark.hadoop.fs.s3a.secret.key=$OBS_SECRET_KEY"
    -c "spark.demo.bucket=$DEMO_BUCKET"
  )
}

# Extract the b-<hex> instance id from submit output.
extract_instance_id() {
  grep -oE 'b-[0-9a-f]{32}' | head -1
}

# Wait for an instance to reach a terminal state; echoes status each poll.
wait_instance() {
  local id="$1" s
  while true; do
    s="$(dpe instance status --format=json "$id" 2>/dev/null | grep -v jline \
         | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' 2>/dev/null)"
    echo "[$(date +%H:%M:%S)] $id -> ${s:-?}"
    case "$s" in COMPLETED|FAILED|ERROR|CANCELLED|KILLED) return 0 ;; esac
    sleep 20
  done
}

# ---------------------------------------------------------------------------
# S3 helpers: prefer aws CLI, fall back to python3 + boto3
# ---------------------------------------------------------------------------
s3_upload_dir() {
  local src_dir="$1" prefix="$2"
  if command -v aws >/dev/null 2>&1; then
    AWS_ACCESS_KEY_ID="$OBS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$OBS_SECRET_KEY" \
      aws s3 cp --endpoint-url "$OBS_ENDPOINT" --recursive "$src_dir" "s3://$DEMO_BUCKET/$prefix"
  else
    OBS_ACCESS_KEY="$OBS_ACCESS_KEY" OBS_SECRET_KEY="$OBS_SECRET_KEY" \
    OBS_ENDPOINT="$OBS_ENDPOINT" DEMO_BUCKET="$DEMO_BUCKET" \
    python3 - "$src_dir" "$prefix" <<'PY'
import boto3, os, sys
src, prefix = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url=os.environ["OBS_ENDPOINT"],
                  aws_access_key_id=os.environ["OBS_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["OBS_SECRET_KEY"])
for name in sorted(os.listdir(src)):
    path = os.path.join(src, name)
    if os.path.isfile(path):
        key = f"{prefix}/{name}"
        s3.upload_file(path, os.environ["DEMO_BUCKET"], key)
        print(f"uploaded s3://{os.environ['DEMO_BUCKET']}/{key}")
PY
  fi
}

s3_ensure_bucket() {
  OBS_ACCESS_KEY="$OBS_ACCESS_KEY" OBS_SECRET_KEY="$OBS_SECRET_KEY" \
  OBS_ENDPOINT="$OBS_ENDPOINT" DEMO_BUCKET="$DEMO_BUCKET" \
  python3 - <<'PY'
import boto3, os, sys
from botocore.exceptions import ClientError
s3 = boto3.client("s3", endpoint_url=os.environ["OBS_ENDPOINT"],
                  aws_access_key_id=os.environ["OBS_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["OBS_SECRET_KEY"])
bucket = os.environ["DEMO_BUCKET"]
try:
    s3.head_bucket(Bucket=bucket)
    print(f"bucket {bucket} exists")
except ClientError:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"bucket {bucket} created")
    except ClientError as e:
        sys.exit(f"cannot use bucket {bucket}: {e}")
PY
}

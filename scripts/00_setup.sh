#!/usr/bin/env bash
# One-time setup: verify CLI + Java, configure endpoint, login, ensure bucket,
# upload jobs/*.py to s3://$DEMO_BUCKET/aidp-demo/code/ for reference.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
require_vars OBS_ACCESS_KEY OBS_SECRET_KEY DPE_USER DPE_PASSWORD

echo "==> DPE CLI: $DPE_CLI (JAVA_HOME=$DPE_JAVA_HOME)"
dpe version | grep -v 'jline\|WARNING' || true

echo "==> config set + login (role $DPE_ROLE)"
dpe_login

echo "==> bucket $DEMO_BUCKET"
s3_ensure_bucket

echo "==> upload jobs/*.py -> s3://$DEMO_BUCKET/aidp-demo/code/"
s3_upload_dir "$REPO_ROOT/jobs" "aidp-demo/code"

echo
echo "NOTE: DPE 'submit' uploads the local main file automatically, so scripts"
echo "      pass jobs/*.py as local paths. The s3 copy is for reference and for"
echo "      environments where 'submit s3a://...' is supported."
echo "Done."

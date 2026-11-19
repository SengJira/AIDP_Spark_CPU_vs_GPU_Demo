#!/usr/bin/env bash
# Remove everything the demo created:
#   - s3://$DEMO_BUCKET/aidp-demo/  (raw data, features, results, code)
#   - js_manuf_ice.aidp_demo schema (Iceberg tables registered in Starburst)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

echo "==> deleting s3://$DEMO_BUCKET/aidp-demo/"
OBS_ACCESS_KEY="$OBS_ACCESS_KEY" OBS_SECRET_KEY="$OBS_SECRET_KEY" \
OBS_ENDPOINT="$OBS_ENDPOINT" DEMO_BUCKET="$DEMO_BUCKET" \
python3 - <<'PY'
import boto3, os
s3 = boto3.client("s3", endpoint_url=os.environ["OBS_ENDPOINT"],
                  aws_access_key_id=os.environ["OBS_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["OBS_SECRET_KEY"])
bucket, prefix = os.environ["DEMO_BUCKET"], "aidp-demo/"
paginator = s3.get_paginator("list_objects_v2")
n = 0
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objs:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
        n += len(objs)
print(f"deleted {n} objects under {bucket}/{prefix}")
PY

echo "==> dropping Starburst schema js_manuf_ice.aidp_demo"
DDAE_USER="$DDAE_USER" DDAE_PASSWORD="$DDAE_PASSWORD" \
python3 - <<'PY'
import os, trino.dbapi, trino.auth, urllib3
urllib3.disable_warnings()
conn = trino.dbapi.connect(
    host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
    http_scheme="https", user=os.environ["DDAE_USER"],
    auth=trino.auth.BasicAuthentication(os.environ["DDAE_USER"], os.environ["DDAE_PASSWORD"]),
    verify=False)
cur = conn.cursor()
cur.execute("DROP SCHEMA IF EXISTS js_manuf_ice.aidp_demo CASCADE")
print("schema dropped")
PY

echo "cleanup done"

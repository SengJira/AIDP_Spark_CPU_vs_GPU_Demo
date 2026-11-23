#!/usr/bin/env python3
"""Prepare the serving tables for the 360-concurrency test.

1. Registers raw transactions/customers/merchants Parquet as Iceberg tables
   in js_manuf_ice.aidp_demo (pyiceberg metadata + CALL register_table —
   the cluster disables add_files; register_table needs sysadmin).
2. Creates customer_features_serving (mode='gpu' copy, partitioned by
   province) for BI-style access, then ANALYZEs everything.
3. Prints row counts + file counts as evidence.

Usage: ./concurrency/prepare.py
Env:   OBS_* / S3_* (ObjectScale), DDAE_* / TRINO_* (Starburst), DEMO_BUCKET.
"""
import functools
import os
import env  # noqa: F401  (loads credentials like scripts/lib.sh)
import sys

import boto3
import trino.auth
import trino.dbapi
import urllib3

import pyiceberg.io.pyarrow as _paio
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# pyiceberg 0.12 add_files doesn't wire the ns->us downcast into the
# add_files schema check; default it on (harmless for us files).
_orig_check = _paio._check_pyarrow_schema_compatible
_paio._check_pyarrow_schema_compatible = functools.partial(
    _orig_check, downcast_ns_timestamp_to_us=True
)

RAW_TABLES = {
    "transactions": {
        "prefix": "aidp-demo/raw/transactions/",
        "schema": Schema(
            NestedField(1, "txn_id", LongType()),
            NestedField(2, "customer_id", LongType()),
            NestedField(3, "merchant_id", LongType()),
            NestedField(4, "amount", DoubleType()),
            NestedField(5, "currency", StringType()),
            NestedField(6, "channel", StringType()),
            NestedField(7, "status", StringType()),
            NestedField(8, "txn_ts", TimestamptzType()),
        ),
    },
    "customers": {
        "prefix": "aidp-demo/raw/customers/",
        "schema": Schema(
            NestedField(1, "customer_id", LongType()),
            NestedField(2, "segment", StringType()),
            NestedField(3, "province", StringType()),
            NestedField(4, "age", IntegerType()),
            NestedField(5, "signup_date", DateType()),
            NestedField(6, "risk_tier", StringType()),
        ),
    },
    "merchants": {
        "prefix": "aidp-demo/raw/merchants/",
        "schema": Schema(
            NestedField(1, "merchant_id", LongType()),
            NestedField(2, "category", StringType()),
            NestedField(3, "mcc", StringType()),
            NestedField(4, "country", StringType()),
        ),
    },
}
BUILD_NS = "aidp_demo_build"
TRINO_CATALOG = "js_manuf_ice"
TRINO_SCHEMA = "aidp_demo"
WAREHOUSE_PREFIX = "aidp-demo/warehouse-meta"


def s3_client(ak, sk, endpoint):
    return boto3.client("s3", endpoint_url=endpoint,
                        aws_access_key_id=ak, aws_secret_access_key=sk)


def list_parquet(s3, bucket, prefix):
    out, tok = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        page = s3.list_objects_v2(**kw)
        out += [o["Key"] for o in page.get("Contents", [])
                if o["Key"].endswith(".parquet")]
        if not page.get("IsTruncated"):
            return sorted(out)
        tok = page["NextContinuationToken"]


def trino_conn():
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    password = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    if not user:
        sys.exit("set DDAE_USER/DDAE_PASSWORD (or TRINO_USER/TRINO_PASS)")
    urllib3.disable_warnings()
    conn = trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
        http_scheme="https", user=user,
        auth=trino.auth.BasicAuthentication(user, password),
        verify=False, request_timeout=600)
    cur = conn.cursor()
    cur.execute("SET ROLE sysadmin")
    return cur


def run(cur, sql, fetch=False):
    head = " ".join(sql.split())[:90]
    print(f"--> {head}")
    cur.execute(sql)
    return cur.fetchall() if fetch else None


def main():
    endpoint = os.environ.get("OBS_ENDPOINT") or os.environ.get("S3_ENDPOINT")
    ak = os.environ.get("OBS_ACCESS_KEY") or os.environ.get("S3_ACCESS_KEY")
    sk = os.environ.get("OBS_SECRET_KEY") or os.environ.get("S3_SECRET_KEY")
    bucket = os.environ.get("DEMO_BUCKET", "js-demo")
    for v in (endpoint, ak, sk):
        if not v:
            sys.exit("set OBS_ENDPOINT/OBS_ACCESS_KEY/OBS_SECRET_KEY (or S3_*)")

    cat = load_catalog(
        "build",
        type="sql",
        uri=f"sqlite:///{os.path.dirname(os.path.abspath(__file__))}/results/iceberg-build.db",
        warehouse=f"s3://{bucket}/{WAREHOUSE_PREFIX}",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": endpoint,
            "s3.access-key-id": ak,
            "s3.secret-access-key": sk,
            "s3.path-style-access": "true",
        },
    )
    s3 = s3_client(ak, sk, endpoint)
    try:
        cat.create_namespace(BUILD_NS)
    except Exception:
        pass

    # ---------- 1. register raw tables ----------
    registered = {}
    skip_raw = os.environ.get("PREPARE_RAW", "1") == "0"
    for name, spec in ({} if skip_raw else RAW_TABLES).items():
        keys = list_parquet(s3, bucket, spec["prefix"])
        if not keys:
            sys.exit(f"no parquet under {spec['prefix']} — run datagen first")
        ident = (BUILD_NS, name)
        try:
            cat.drop_table(ident)
        except Exception:
            pass
        tbl = cat.create_table(
            ident, schema=spec["schema"],
            location=f"s3://{bucket}/{WAREHOUSE_PREFIX}/{name}")
        print(f"{name}: adding {len(keys)} files")
        tbl.add_files(file_paths=[f"s3://{bucket}/{k}" for k in keys])
        registered[name] = tbl.location()
        print(f"{name}: {tbl.scan().to_arrow().num_rows} rows")

    cur = trino_conn()
    run(cur, f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA} "
             f"WITH (location='s3://{bucket}/aidp-demo/warehouse/{TRINO_SCHEMA}')")
    for name, loc in registered.items():
        run(cur, f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{name}")
        run(cur, f"CALL {TRINO_CATALOG}.system.register_table('{TRINO_SCHEMA}', '{name}', '{loc}')")

    # ---------- 2. serving copy ----------
    # Full CTAS can blow the user's per-node memory cap on this cluster, so we
    # create a province-partitioned table and INSERT one province at a time.
    fq = f"{TRINO_CATALOG}.{TRINO_SCHEMA}"
    run(cur, f"DROP TABLE IF EXISTS {fq}.customer_features_serving")
    cols = ("customer_id, txn_count, total_amount, avg_amount, max_amount, "
            "distinct_merchants, night_txn_ratio, flagged_count, segment, "
            "province, age, signup_date, risk_tier, mode")
    run(cur, f"""CREATE TABLE {fq}.customer_features_serving (
                    customer_id bigint, txn_count bigint, total_amount double,
                    avg_amount double, max_amount double,
                    distinct_merchants bigint, night_txn_ratio double,
                    flagged_count bigint, segment varchar, province varchar,
                    age integer, signup_date date, risk_tier varchar,
                    mode varchar)
                 WITH (partitioning = ARRAY['province'])""")
    provs = [r[0] for r in run(
        cur, f"SELECT DISTINCT province FROM {fq}.customer_features "
             f"WHERE mode='gpu' ORDER BY 1", fetch=True)]
    for i, p in enumerate(provs):
        run(cur, f"INSERT INTO {fq}.customer_features_serving "
                 f"SELECT {cols} FROM {fq}.customer_features "
                 f"WHERE mode='gpu' AND province='{p}'")
        if (i + 1) % 10 == 0:
            print(f"  serving: {i+1}/{len(provs)} provinces inserted")

    # ---------- 3. analyze + evidence ----------
    for t in ["customer_features_serving", "transactions", "customers", "merchants"]:
        try:
            run(cur, f"ANALYZE {fq}.{t}")
        except Exception as e:
            print(f"  ANALYZE {t} failed (non-fatal): {str(e)[:80]}")
        cnt = run(cur, f"SELECT count(*) FROM {fq}.{t}", fetch=True)
        files = run(cur, f"SELECT count(*) FROM {fq}.\"{t}$files\"", fetch=True)
        print(f"  {t}: {cnt[0][0]:,} rows, {files[0][0]} data files")

    print("prepare done")


if __name__ == "__main__":
    main()

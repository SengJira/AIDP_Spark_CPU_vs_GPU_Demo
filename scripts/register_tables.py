#!/usr/bin/env python3
"""Build Iceberg metadata over the demo Parquet files and register the
tables in the Starburst js_manuf_ice catalog.

Why this exists: the cluster disables Iceberg's add_files table-procedure
and register_table needs sysadmin + an existing metadata.json. pyiceberg can
append existing Parquet files to Iceberg manifests metadata-only (no data
rewrite), so we build the metadata on S3 then register_table it.

Env: OBS_* / S3_* (ObjectScale), DDAE_* / TRINO_* (Starburst basic auth).
"""
import functools
import os
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
    TimestampType,
)

# pyiceberg 0.12 add_files doesn't wire the ns->us downcast flag into the
# add_files schema check; default it on. (Spark on this build writes ts[ns].)
_orig_check = _paio._check_pyarrow_schema_compatible
_paio._check_pyarrow_schema_compatible = functools.partial(
    _orig_check, downcast_ns_timestamp_to_us=True
)

RESULTS_SCHEMA = Schema(
    NestedField(1, "run_id", StringType()),
    NestedField(2, "mode", StringType()),
    NestedField(3, "query", StringType()),
    NestedField(4, "seconds", DoubleType()),
    NestedField(5, "rows", LongType()),
    NestedField(6, "executor_instances", LongType()),
    NestedField(7, "executor_cores", LongType()),
    NestedField(8, "executor_memory", StringType()),
    NestedField(9, "gpu_per_executor", DoubleType()),
    NestedField(10, "data_rows", LongType()),
    NestedField(11, "run_ts", TimestampType()),
)

FEATURES_SCHEMA = Schema(
    NestedField(1, "customer_id", LongType()),
    NestedField(2, "txn_count", LongType()),
    NestedField(3, "total_amount", DoubleType()),
    NestedField(4, "avg_amount", DoubleType()),
    NestedField(5, "max_amount", DoubleType()),
    NestedField(6, "distinct_merchants", LongType()),
    NestedField(7, "night_txn_ratio", DoubleType()),
    NestedField(8, "flagged_count", LongType()),
    NestedField(9, "segment", StringType()),
    NestedField(10, "province", StringType()),
    NestedField(11, "age", IntegerType()),
    NestedField(12, "signup_date", DateType()),
    NestedField(13, "risk_tier", StringType()),
    NestedField(14, "mode", StringType()),
)

BUILD_NS = "aidp_demo_build"          # namespace inside the local sqlite catalog
TRINO_SCHEMA = "aidp_demo"            # js_manuf_ice.aidp_demo in Starburst
WAREHOUSE_PREFIX = "aidp-demo/warehouse-meta"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["OBS_ENDPOINT"],
        aws_access_key_id=os.environ["OBS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["OBS_SECRET_KEY"],
    )


def list_parquet(s3, bucket, prefix):
    out = []
    tok = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        page = s3.list_objects_v2(**kw)
        out += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", [])
                if o["Key"].endswith(".parquet")]
        if not page.get("IsTruncated"):
            return out
        tok = page["NextContinuationToken"]


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
        uri=f"sqlite:///{os.path.dirname(os.path.abspath(__file__))}/../results/iceberg-build.db",
        warehouse=f"s3://{bucket}/{WAREHOUSE_PREFIX}",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "s3.endpoint": endpoint,
            "s3.access-key-id": ak,
            "s3.secret-access-key": sk,
            "s3.path-style-access": "true",
        },
    )
    s3 = s3_client()
    try:
        cat.create_namespace(BUILD_NS)
    except Exception:
        pass

    tables = {
        "etl_results": {
            "schema": RESULTS_SCHEMA,
            "prefixes": ["aidp-demo/results/"],
        },
        "customer_features": {
            "schema": FEATURES_SCHEMA,
            "prefixes": ["aidp-demo/features/mode=cpu", "aidp-demo/features/mode=gpu"],
        },
    }

    metadata = {}
    for name, spec in tables.items():
        files = []
        for p in spec["prefixes"]:
            files += list_parquet(s3, bucket, p)
        if not files:
            print(f"!! no parquet files for {name} — run the jobs first")
            continue
        ident = (BUILD_NS, name)
        try:
            cat.drop_table(ident)
        except Exception:
            pass
        # wipe stale metadata files so register_table picks the latest cleanly
        prefix = f"{WAREHOUSE_PREFIX}/{name}/"
        tok = None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix}
            if tok:
                kw["ContinuationToken"] = tok
            page = s3.list_objects_v2(**kw)
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            if not page.get("IsTruncated"):
                break
            tok = page["NextContinuationToken"]
        tbl = cat.create_table(
            ident,
            schema=spec["schema"],
            location=f"s3://{bucket}/{WAREHOUSE_PREFIX}/{name}",
        )
        print(f"{name}: adding {len(files)} files ...")
        tbl.add_files(file_paths=files)
        rows = tbl.scan().to_arrow().num_rows
        print(f"{name}: {rows} rows -> {tbl.metadata_location}")
        metadata[name] = tbl.location()

    # ------------------------------------------------------------------
    # Register in Starburst (needs sysadmin role)
    # ------------------------------------------------------------------
    user = os.environ.get("DDAE_USER") or os.environ.get("TRINO_USER")
    password = os.environ.get("DDAE_PASSWORD") or os.environ.get("TRINO_PASS")
    if not user:
        sys.exit("set DDAE_USER/DDAE_PASSWORD (or TRINO_USER/TRINO_PASS)")
    urllib3.disable_warnings()
    conn = trino.dbapi.connect(
        host=os.environ.get("DDAE_HOST", "ddae.lab9bgp.com"), port=443,
        http_scheme="https", user=user,
        auth=trino.auth.BasicAuthentication(user, password),
        verify=False, request_timeout=300)
    cur = conn.cursor()
    cur.execute("SET ROLE sysadmin")
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS js_manuf_ice.{TRINO_SCHEMA} "
        f"WITH (location='s3://{bucket}/aidp-demo/warehouse/{TRINO_SCHEMA}')")
    for name, loc in metadata.items():
        # NOT "DROP TABLE": Iceberg DROP deletes the files in the manifests —
        # it would wipe the Spark-written parquet under aidp-demo/.
        try:
            cur.execute(f"CALL js_manuf_ice.system.unregister_table"
                        f"('{TRINO_SCHEMA}', '{name}')")
        except Exception:
            pass
        cur.execute(
            f"CALL js_manuf_ice.system.register_table('{TRINO_SCHEMA}', '{name}', '{loc}')")
        cur.execute(f"SELECT count(*) FROM js_manuf_ice.{TRINO_SCHEMA}.{name}")
        print(f"registered js_manuf_ice.{TRINO_SCHEMA}.{name}: {cur.fetchone()[0]} rows")


if __name__ == "__main__":
    main()

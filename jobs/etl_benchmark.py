"""Fraud feature pipeline benchmark — identical code for CPU and GPU runs.

Mode and labels come from spark.demo.* confs only; zero code changes between
runs. Every step is timed with time.perf_counter(). Queries that produce no
output use the 'noop' write format so only compute is measured. GPU-friendly
built-ins only: no Python UDFs, no pandas UDFs, no RDD code.

Confs:
  spark.demo.mode     cpu | gpu        (required)
  spark.demo.run_id   label for results (default <mode>-<epoch>)
  spark.demo.bucket   S3 bucket         (default js-demo)
"""
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def conf(spark, key, default=None):
    try:
        return spark.conf.get(key)
    except Exception:
        return default


def main():
    spark = SparkSession.builder.appName("aidp-etl-benchmark").getOrCreate()

    mode = conf(spark, "spark.demo.mode", "cpu")
    run_id = conf(spark, "spark.demo.run_id",
                  f"{mode}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    bucket = conf(spark, "spark.demo.bucket", "js-demo")

    raw = f"s3a://{bucket}/aidp-demo/raw"
    base = f"s3a://{bucket}/aidp-demo"

    plugins = conf(spark, "spark.plugins", "")
    rapids = conf(spark, "spark.rapids.sql.enabled", "false")
    exec_instances = int(conf(spark, "spark.executor.instances", "0"))
    exec_cores = int(conf(spark, "spark.executor.cores", "0"))
    exec_mem = conf(spark, "spark.executor.memory", "")
    gpu_per_exec = float(conf(spark, "spark.executor.resource.gpu.amount", "0") or 0)

    print("=" * 70)
    print(f"[bench] run_id={run_id} mode={mode} bucket={bucket}")
    print(f"[bench] spark.plugins={plugins}")
    print(f"[bench] spark.rapids.sql.enabled={rapids}")
    print(f"[bench] executors={exec_instances}x{exec_cores}c/{exec_mem} gpu/executor={gpu_per_exec}")
    print("=" * 70)

    wall0 = time.perf_counter()
    results = []  # (query, seconds, rows)

    txn = spark.read.parquet(f"{raw}/transactions")
    customers = spark.read.parquet(f"{raw}/customers")
    merchants = spark.read.parquet(f"{raw}/merchants")

    t = time.perf_counter()
    data_rows = txn.count()
    results.append(("scan-count", time.perf_counter() - t, data_rows))

    # ------------------------------------------------------------------
    # Q1 — scan + filter + project
    # ------------------------------------------------------------------
    t = time.perf_counter()
    q1 = (
        txn.filter((F.col("status") == "SUCCESS") & (F.col("amount") > 100))
        .select(
            "txn_id", "customer_id", "merchant_id", "amount", "channel",
            F.hour("txn_ts").alias("txn_hour"),
            F.dayofweek("txn_ts").alias("txn_dow"),
            F.when(F.col("amount") < 200, F.lit("small"))
             .when(F.col("amount") < 2000, F.lit("medium"))
             .otherwise(F.lit("large")).alias("amount_bucket"),
        )
    )
    q1.write.format("noop").mode("overwrite").save()
    results.append(("q1_scan_filter_project", time.perf_counter() - t, 0))

    # ------------------------------------------------------------------
    # Q2 — join + aggregate
    # ------------------------------------------------------------------
    t = time.perf_counter()
    q2 = (
        txn.join(customers, "customer_id")
        .join(F.broadcast(merchants), "merchant_id")
        .groupBy(
            "province", "segment", "category",
            F.date_trunc("month", "txn_ts").alias("txn_month"),
        )
        .agg(
            F.sum("amount").alias("sum_amount"),
            F.avg("amount").alias("avg_amount"),
            F.count("*").alias("txn_count"),
            F.countDistinct("customer_id").alias("distinct_customers"),
        )
    )
    if mode == "gpu":
        # AQE shows only the pre-execution skeleton (CPU names). Planning once
        # with AQE off lets GpuOverrides render the real Gpu* operators.
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        print("[bench] ---- Q2 physical plan (expect Gpu* operators) ----")
        q2.explain()
        print("[bench] ---- end Q2 plan ----")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
    q2.write.format("noop").mode("overwrite").save()
    results.append(("q2_join_aggregate", time.perf_counter() - t, 0))

    # ------------------------------------------------------------------
    # Q3 — window functions (fraud signals)
    # ------------------------------------------------------------------
    t = time.perf_counter()
    w = Window.partitionBy("customer_id").orderBy("txn_ts")
    w5 = w.rowsBetween(-5, -1)
    # RAPIDS <= 26.02 bug: running + bounded frames fused into one WindowExec
    # fail with "Couldn't find _gpu_w*". Materialising step1 first keeps the
    # bounded-frame AVG in its own physical pass (still on GPU).
    step1 = (
        txn.filter(F.col("status") == "SUCCESS")
        .withColumn("prev_ts", F.lag("txn_ts").over(w))
        .withColumn("rn", F.row_number().over(w))
        .cache()
    )
    step1.count()
    flagged = (
        step1
        .withColumn("roll_avg5", F.avg("amount").over(w5))
        .withColumn(
            "is_flagged",
            (F.col("txn_ts").cast("long") - F.col("prev_ts").cast("long") < 60)
            & (F.col("amount") > 3 * F.col("roll_avg5")),
        )
        .cache()
    )
    n_flagged = flagged.filter("is_flagged").count()
    results.append(("q3_window_fraud_signals", time.perf_counter() - t, n_flagged))

    # ------------------------------------------------------------------
    # Q4 — per-customer feature table (written out)
    # ------------------------------------------------------------------
    t = time.perf_counter()
    flag_counts = (
        flagged.filter("is_flagged")
        .groupBy("customer_id")
        .agg(F.count("*").alias("flagged_count"))
    )
    feats = (
        txn.groupBy("customer_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.countDistinct("merchant_id").alias("distinct_merchants"),
            F.avg((F.hour("txn_ts") < 6).cast("double")).alias("night_txn_ratio"),
        )
        .join(flag_counts, "customer_id", "left")
        .fillna({"flagged_count": 0})
        .join(customers, "customer_id")
        .withColumn("mode", F.lit(mode))
    )
    out_path = f"{base}/features/mode={mode}"
    feats.write.mode("overwrite").parquet(out_path)
    feat_rows = spark.read.parquet(out_path).count()
    results.append(("q4_feature_table_write", time.perf_counter() - t, feat_rows))

    wall_s = time.perf_counter() - wall0

    # ------------------------------------------------------------------
    # summary
    # ------------------------------------------------------------------
    print("=" * 70)
    print(f"[bench] RESULTS run_id={run_id} mode={mode}")
    print(f"[bench] {'query':<28}{'seconds':>10}{'rows':>14}")
    for name, secs, nrows in results:
        print(f"[bench] {name:<28}{secs:>10.2f}{nrows:>14}")
    print(f"[bench] {'TOTAL_WALL_CLOCK':<28}{wall_s:>10.2f}")
    print(f"[bench] rapids_enabled={rapids} plugins={plugins}")
    print("=" * 70)

    # ------------------------------------------------------------------
    # persist timings to s3a://<bucket>/aidp-demo/results/
    # ------------------------------------------------------------------
    run_ts = datetime.now(timezone.utc)
    res_df = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "mode": mode,
                "query": name,
                "seconds": float(secs),
                "rows": int(nrows),
                "executor_instances": exec_instances,
                "executor_cores": exec_cores,
                "executor_memory": exec_mem,
                "gpu_per_executor": gpu_per_exec,
                "data_rows": int(data_rows),
                "run_ts": run_ts,
            }
            for name, secs, nrows in results
        ]
        + [{
            "run_id": run_id,
            "mode": mode,
            "query": "TOTAL_WALL_CLOCK",
            "seconds": float(wall_s),
            "rows": int(data_rows),
            "executor_instances": exec_instances,
            "executor_cores": exec_cores,
            "executor_memory": exec_mem,
            "gpu_per_executor": gpu_per_exec,
            "data_rows": int(data_rows),
            "run_ts": run_ts,
        }]
    )
    res_df.write.mode("append").parquet(f"{base}/results")
    print(f"[bench] timings appended to {base}/results")

    spark.stop()


if __name__ == "__main__":
    main()

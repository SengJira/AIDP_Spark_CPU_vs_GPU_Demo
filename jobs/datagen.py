"""Generate synthetic card-transaction demo data to ObjectScale (S3A, Parquet).

All parameters come from spark.demo.* Spark confs (DPE CLI app-arg handling is
not used). Deterministic: fixed rand() seeds, no Python UDFs.

Confs:
  spark.demo.bucket       S3 bucket          (default js-demo)
  spark.demo.rows         transaction rows   (default 100000000)
  spark.demo.customers    customer rows      (default 10000000)
  spark.demo.merchants    merchant rows      (default 50000)
  spark.demo.txn.files    parquet file count for transactions (default 200)
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def conf(spark, key, default):
    return spark.conf.get(key, default)


def main():
    spark = SparkSession.builder.appName("aidp-datagen").getOrCreate()

    bucket = conf(spark, "spark.demo.bucket", "js-demo")
    rows = int(conf(spark, "spark.demo.rows", "100000000"))
    n_customers = int(conf(spark, "spark.demo.customers", "10000000"))
    n_merchants = int(conf(spark, "spark.demo.merchants", "50000"))
    txn_files = int(conf(spark, "spark.demo.txn.files", "200"))

    base = f"s3a://{bucket}/aidp-demo/raw"
    print(f"[datagen] rows={rows} customers={n_customers} merchants={n_merchants} -> {base}")

    provinces = [
        "Bangkok", "Chiang Mai", "Phuket", "Khon Kaen", "Nakhon Ratchasima",
        "Chonburi", "Songkhla", "Nonthaburi", "Pathum Thani", "Udon Thani",
        "Surat Thani", "Chiang Rai", "Nakhon Si Thammarat", "Rayong", "Samut Prakan",
    ]
    categories = [
        "grocery", "fuel", "restaurant", "travel", "electronics", "fashion",
        "pharmacy", "utilities", "ecommerce", "entertainment", "hotel",
        "airline", "gambling", "jewelry", "education",
    ]
    segments = ["RETAIL", "PREMIUM", "SME", "CORPORATE"]
    currencies = ["THB", "THB", "THB", "THB", "USD", "SGD", "MYR", "VND"]
    channels = ["POS", "ECOM", "ATM", "MOBILE"]
    countries = ["TH", "TH", "TH", "SG", "MY", "VN", "US", "JP"]

    t0 = time.perf_counter()

    # ---------------- transactions ----------------
    txn = (
        spark.range(rows)
        .select(
            F.col("id").alias("txn_id"),
            F.pmod(F.floor(F.rand(11) * 1e9), F.lit(n_customers)).cast("long").alias("customer_id"),
            F.pmod(F.col("id") * 31 + 7, F.lit(n_merchants)).cast("long").alias("merchant_id"),
            # skewed: most txns small, few large (rand^4 * 20000 + 20)
            F.round(20 + F.pow(F.rand(12), 4) * 20000, 2).alias("amount"),
            F.element_at(F.array(*[F.lit(c) for c in currencies]),
                         (F.pmod(F.col("id"), F.lit(len(currencies))) + 1).cast("int")).alias("currency"),
            F.element_at(F.array(*[F.lit(c) for c in channels]),
                         (F.pmod(F.col("id") * 13 + 3, F.lit(len(channels))) + 1).cast("int")).alias("channel"),
            F.when(F.rand(13) < 0.90, F.lit("SUCCESS"))
             .when(F.rand(14) < 0.90, F.lit("DECLINED"))
             .otherwise(F.lit("REVERSED")).alias("status"),
            F.from_unixtime(1729900000 - (F.rand(15) * 31536000).cast("long")).cast("timestamp").alias("txn_ts"),
        )
        .repartition(txn_files)
    )
    txn.write.mode("overwrite").parquet(f"{base}/transactions")
    print(f"[datagen] transactions written in {time.perf_counter()-t0:.1f}s")

    # ---------------- customers ----------------
    t1 = time.perf_counter()
    cust = (
        spark.range(n_customers)
        .select(
            F.col("id").alias("customer_id"),
            F.element_at(F.array(*[F.lit(s) for s in segments]),
                         (F.pmod(F.col("id") * 7 + 1, F.lit(len(segments))) + 1).cast("int")).alias("segment"),
            F.element_at(F.array(*[F.lit(p) for p in provinces]),
                         (F.pmod(F.col("id") * 17 + 5, F.lit(len(provinces))) + 1).cast("int")).alias("province"),
            (18 + F.pmod(F.col("id") * 3 + 11, F.lit(60))).cast("int").alias("age"),
            F.from_unixtime(1600000000 + (F.rand(16) * 90000000).cast("long")).cast("date").alias("signup_date"),
            F.when(F.rand(17) < 0.70, F.lit("LOW"))
             .when(F.rand(18) < 0.80, F.lit("MEDIUM"))
             .otherwise(F.lit("HIGH")).alias("risk_tier"),
        )
        .repartition(20)
    )
    cust.write.mode("overwrite").parquet(f"{base}/customers")
    print(f"[datagen] customers written in {time.perf_counter()-t1:.1f}s")

    # ---------------- merchants ----------------
    t2 = time.perf_counter()
    merch = (
        spark.range(n_merchants)
        .select(
            F.col("id").alias("merchant_id"),
            F.element_at(F.array(*[F.lit(c) for c in categories]),
                         (F.pmod(F.col("id") * 5 + 2, F.lit(len(categories))) + 1).cast("int")).alias("category"),
            F.format_string("%04d", F.pmod(F.col("id") * 19 + 5411, F.lit(8000))).alias("mcc"),
            F.element_at(F.array(*[F.lit(c) for c in countries]),
                         (F.pmod(F.col("id") * 3, F.lit(len(countries))) + 1).cast("int")).alias("country"),
        )
        .coalesce(4)
    )
    merch.write.mode("overwrite").parquet(f"{base}/merchants")
    print(f"[datagen] merchants written in {time.perf_counter()-t2:.1f}s")
    print(f"[datagen] DONE total={time.perf_counter()-t0:.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()

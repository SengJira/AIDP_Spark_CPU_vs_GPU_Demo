-- ===========================================================================
-- AIDP Spark CPU vs GPU — Starburst (DDAE) queries
-- Catalog: js_manuf_ice (Iceberg, warehouse on s3://js-demo — the ObjectScale
-- bucket the Spark jobs also write to).
--
-- TABLE SETUP happens in scripts/register_tables.py — this cluster disables
-- the Iceberg add_files table-procedure, so the script builds Iceberg
-- metadata over the existing Parquet files with pyiceberg (metadata only,
-- no data rewrite) and registers it via CALL system.register_table (needs
-- sysadmin role). Re-run it after new benchmark runs to pick up new results.
--
-- If your cluster HAS add_files enabled, the simpler path is:
--   CREATE TABLE … (same columns) ;
--   ALTER TABLE t EXECUTE add_files(location => 's3://…', format => 'PARQUET');
-- ===========================================================================

-- ===========================================================================
-- DEMO QUERY 1: per-query CPU vs GPU seconds + speedup
-- (uses the latest run of each mode; drop the rk=1 filter to see every run)
-- ===========================================================================
WITH r AS (
    SELECT *,
           dense_rank() OVER (PARTITION BY mode ORDER BY run_ts DESC) AS rk
    FROM js_manuf_ice.aidp_demo.etl_results
)
SELECT
    query,
    round(max(CASE WHEN mode='cpu' AND rk=1 THEN seconds END), 2) AS cpu_seconds,
    round(max(CASE WHEN mode='gpu' AND rk=1 THEN seconds END), 2) AS gpu_seconds,
    round(max(CASE WHEN mode='cpu' AND rk=1 THEN seconds END)
        / max(CASE WHEN mode='gpu' AND rk=1 THEN seconds END), 2) AS speedup_x
FROM r
GROUP BY query
ORDER BY query;

-- every recorded run (warm-up vs measured runs)
SELECT run_id, mode, query, round(seconds,2) AS seconds, "rows", run_ts
FROM js_manuf_ice.aidp_demo.etl_results
ORDER BY run_ts, mode, query;

-- ===========================================================================
-- DEMO QUERY 2: GPU-produced features are immediately queryable
-- "top risky provinces by fraud flags"
-- ===========================================================================
SELECT
    province,
    count(*)                          AS customers,
    sum(flagged_count)                AS fraud_flags,
    round(avg(night_txn_ratio), 3)    AS avg_night_ratio,
    round(avg(total_amount), 2)       AS avg_total_amount
FROM js_manuf_ice.aidp_demo.customer_features
WHERE mode = 'gpu'
GROUP BY province
ORDER BY fraud_flags DESC
LIMIT 10;

-- top risky individual customers (GPU mode)
SELECT customer_id, province, segment, risk_tier,
       txn_count, round(total_amount,2) AS total_amount,
       round(night_txn_ratio,3) AS night_txn_ratio, flagged_count
FROM js_manuf_ice.aidp_demo.customer_features
WHERE mode = 'gpu' AND flagged_count > 0
ORDER BY flagged_count DESC, total_amount DESC
LIMIT 20;

-- sanity: the two runs produce identical feature data (row counts & flags)
SELECT mode,
       count(*)          AS customers,
       sum(txn_count)    AS txns,
       sum(flagged_count) AS fraud_flags
FROM js_manuf_ice.aidp_demo.customer_features
GROUP BY mode;

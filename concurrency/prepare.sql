-- Reference SQL for the concurrency test.
-- prepare.py does the real work because:
--   * this cluster disables Iceberg's add_files -> raw tables are registered
--     via pyiceberg + CALL register_table (needs sysadmin)
--   * a 10M-row CTAS exceeds the user's ~167MB/node query-memory cap ->
--     the serving table is filled one province partition at a time.
-- Run with: ./concurrency/prepare.py   (credentials from scripts/lib.sh env)

SET ROLE sysadmin;

-- 1. raw Parquet registered as Iceberg (metadata built by pyiceberg first)
CALL js_manuf_ice.system.register_table('aidp_demo', 'transactions',
    's3://js-demo/aidp-demo/warehouse-meta/transactions');
CALL js_manuf_ice.system.register_table('aidp_demo', 'customers',
    's3://js-demo/aidp-demo/warehouse-meta/customers');
CALL js_manuf_ice.system.register_table('aidp_demo', 'merchants',
    's3://js-demo/aidp-demo/warehouse-meta/merchants');

-- 2. serving copy of the GPU-mode features, partitioned by province.
--    (sorted_by would be nice but the spec allows partitioning only;
--     a plain CTAS also exceeds the per-node memory cap, so prepare.py
--     loops INSERT per province instead.)
CREATE TABLE js_manuf_ice.aidp_demo.customer_features_serving (
    customer_id bigint, txn_count bigint, total_amount double,
    avg_amount double, max_amount double, distinct_merchants bigint,
    night_txn_ratio double, flagged_count bigint, segment varchar,
    province varchar, age integer, signup_date date, risk_tier varchar,
    mode varchar)
WITH (partitioning = ARRAY['province']);

-- per province (prepare.py loops these):
-- INSERT INTO js_manuf_ice.aidp_demo.customer_features_serving
--   SELECT <cols> FROM js_manuf_ice.aidp_demo.customer_features
--   WHERE mode='gpu' AND province='<p>';

-- 3. statistics + evidence
ANALYZE js_manuf_ice.aidp_demo.customer_features_serving;
ANALYZE js_manuf_ice.aidp_demo.customers;
ANALYZE js_manuf_ice.aidp_demo.merchants;
-- ANALYZE on `transactions` exceeds the per-node memory cap on this cluster.

SELECT count(*) FROM js_manuf_ice.aidp_demo.customer_features_serving;
SELECT count(*) FROM js_manuf_ice.aidp_demo."customer_features_serving$files";
SELECT count(*) FROM js_manuf_ice.aidp_demo.transactions;
SELECT count(*) FROM js_manuf_ice.aidp_demo."transactions$files";

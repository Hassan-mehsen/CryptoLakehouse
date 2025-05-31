-- =========================================================================
-- File      : warehouse/scripts/tests/exchange_data_tests.sql
-- Purpose   : Centralized quality tests for the 'exchange' domain in the DWH
-- Author    : Hassan Mehsen (Portfolio project)
-- Notes     : Run manually to check DWH integrity and inspect anomalies by context
-- =========================================================================

-- 1. Test NULLs in fact_exchange_assets (spot_volume_usd)
SELECT COUNT(*) AS null_spot_volume_usd
FROM fact_exchange_assets
WHERE spot_volume_usd IS NULL;

-- 2. Test duplicate keys in fact_exchange_assets
SELECT exchange_id, wallet_address, crypto_id, snapshot_timestamp, COUNT(*) AS cnt
FROM fact_exchange_assets
GROUP BY exchange_id, wallet_address, crypto_id, snapshot_timestamp
HAVING COUNT(*) > 1;

-- 3. Test wallet_weight outlier values
SELECT exchange_id, snapshot_timestamp, wallet_weight
FROM fact_exchange_assets
WHERE wallet_weight > 100;

-- 4. Test NULLs in dim_exchange_map (pipeline misalignment)
-- If the result includes the most recent snapshot after running the procedure
-- fill_nulls_exchange_map_info, it indicates an error: either the procedure failed,
-- wasn't triggered, or the /exchange/info endpoint hasn't updated yet.
-- This is acceptable temporarily, as both /exchange/map and /exchange/info are called weekly
-- and the correction can be applied manually with a snapshot offset if needed.
SELECT exchange_id, snapshot_timestamp
FROM dim_exchange_map
WHERE maker_fee IS NULL AND taker_fee IS NULL AND weekly_visits IS NULL;

-- 5. Test duplicate keys in dim_exchange_map (should not exist)
SELECT exchange_id, snapshot_timestamp, COUNT(*) AS cnt
FROM dim_exchange_map
GROUP BY exchange_id, snapshot_timestamp
HAVING COUNT(*) > 1;

-- 6. Test extreme values for maker/taker fees
SELECT exchange_id, snapshot_timestamp, maker_fee, taker_fee
FROM dim_exchange_map
WHERE maker_fee > 0.5 OR taker_fee > 0.5;

-- 7. Test potentially misleading weekly_visits values
-- Extremely high values might reflect data issues or parsing anomalies,
-- but could also be legitimate for very large exchanges. Review manually.
SELECT exchange_id, snapshot_timestamp, weekly_visits
FROM dim_exchange_map
WHERE weekly_visits > 100000000;

-- 8. Test missing exchange_id references in fact_exchange_assets (all references should exist in dim_exchange_id)
SELECT fea.exchange_id
FROM fact_exchange_assets fea
LEFT JOIN dim_exchange_id d ON fea.exchange_id = d.exchange_id
WHERE d.exchange_id IS NULL;

-- 9. Test NULLs in dim_exchange_id (should not exist)
SELECT *
FROM dim_exchange_id
WHERE name IS NULL;

-- 10. Test invalid URLs in dim_exchange_info
SELECT exchange_id
FROM dim_exchange_info
WHERE website_url IS NULL OR logo_url IS NULL;

-- 11. Test malformed date_launched (should not exist)
SELECT exchange_id, date_launched
FROM dim_exchange_info
WHERE date_launched IS NOT NULL AND NOT date_launched ~ '^\d{4}-\d{2}-\d{2}$';

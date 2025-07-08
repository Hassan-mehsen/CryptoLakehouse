-- ============================================================================
-- File      : warehouse/scripts/tests/crypto_data_tests.sql
-- Purpose   : Manual data quality checks for the crypto domain
-- Notes     : Run manually to inspect anomalies, outliers and broken integrity
-- ============================================================================

-- 1. dim_crypto_id - check for missing names or symbols
SELECT *
FROM dim_crypto_id
WHERE name IS NULL OR symbol IS NULL;


-- 2. dim_crypto_id - check for duplicate crypto_id (should not exist)
SELECT
    crypto_id,
    COUNT(*)
FROM dim_crypto_id
GROUP BY crypto_id
HAVING COUNT(*) > 1;


-- 3. dim_crypto_info - check for critical NULLs
SELECT *
FROM dim_crypto_info
WHERE logo_url IS NULL OR website_url IS NULL;


-- 4. dim_crypto_info - malformed or missing date formats
SELECT
    crypto_id,
    date_added,
    date_launched
FROM dim_crypto_info
WHERE
    (date_added IS NOT NULL AND date_added::text !~ '^\d{4}-\d{2}-\d{2}$')
    OR (date_launched IS NOT NULL AND date_launched::text !~ '^\d{4}-\d{2}-\d{2}$');


-- 5. dim_crypto_map - null rank or status
SELECT *
FROM dim_crypto_map
WHERE rank IS NULL OR is_active IS NULL;


-- 6. dim_crypto_map - duplicate keys
SELECT
    crypto_id,
    snapshot_timestamp,
    COUNT(*)
FROM dim_crypto_map
GROUP BY crypto_id, snapshot_timestamp
HAVING COUNT(*) > 1;


-- 7. fact_crypto_market - rows with NULL price
SELECT *
FROM fact_crypto_market
WHERE price_usd IS NULL;


-- 8. fact_crypto_market - outliers: extremely high volume/market cap ratio
SELECT
    crypto_id,
    snapshot_timestamp,
    volume_to_market_cap_ratio
FROM fact_crypto_market
WHERE volume_to_market_cap_ratio > 10;


-- 9. fact_crypto_market - supply utilization > 1.1 (suspicious)
SELECT
    crypto_id,
    snapshot_timestamp,
    supply_utilization
FROM fact_crypto_market
WHERE supply_utilization > 1.1;


-- 10. fact_crypto_market - FK issues (missing crypto_id reference)
SELECT fm.crypto_id
FROM fact_crypto_market AS fm
LEFT JOIN dim_crypto_id AS d ON fm.crypto_id = d.crypto_id
WHERE d.crypto_id IS NULL;


-- 11. Test NULLs in critical fields of dim_crypto_category
SELECT *
FROM dim_crypto_category
WHERE name IS NULL OR title IS NULL;


-- 12. Test missing category_id references in crypto_category_link
SELECT DISTINCT ccl.category_id
FROM crypto_category_link AS ccl
LEFT JOIN dim_crypto_category AS dcc ON ccl.category_id = dcc.category_id
WHERE dcc.category_id IS NULL;


-- 13. Test missing crypto_id references in crypto_category_link
SELECT DISTINCT ccl.crypto_id
FROM crypto_category_link AS ccl
LEFT JOIN dim_crypto_id AS dci ON ccl.crypto_id = dci.crypto_id
WHERE dci.crypto_id IS NULL;


-- 14. Test for NULL metrics in fact_crypto_category (possible ELT miss)
SELECT *
FROM fact_crypto_category
WHERE market_cap IS NULL OR volume IS NULL OR num_tokens IS NULL;


-- 15. Test duplicate keys in fact_crypto_category
SELECT
    category_id,
    snapshot_timestamp,
    COUNT(*)
FROM fact_crypto_category
GROUP BY category_id, snapshot_timestamp
HAVING COUNT(*) > 1;


-- 16. Test negative or extreme values in price_change_index
SELECT
    category_id,
    snapshot_timestamp,
    price_change_index
FROM fact_crypto_category
WHERE price_change_index < -100 OR price_change_index > 1000;


-- 17. Test extreme volume_to_market_cap_ratio
SELECT
    category_id,
    snapshot_timestamp,
    volume_to_market_cap_ratio
FROM fact_crypto_category
WHERE volume_to_market_cap_ratio > 10;

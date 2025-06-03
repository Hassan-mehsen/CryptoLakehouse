-- ============================================================================
-- File      : warehouse/scripts/tests/global_market_data_tests.sql
-- Purpose   : Manual data quality checks for the global market domain
-- Notes     : Run manually to validate critical indicators, spot outliers and verify logic
-- ============================================================================

-- 1. Test NULLs in critical metrics (market cap, volume, dominance)
SELECT *
FROM fact_global_market
WHERE
    total_market_cap IS NULL OR total_volume_24h IS NULL OR btc_dominance IS NULL;


-- 2. Test BTC or ETH dominance out of bounds [0, 100]
SELECT
    snapshot_timestamp,
    btc_dominance,
    eth_dominance
FROM fact_global_market
WHERE
    btc_dominance < 0 OR btc_dominance > 100
    OR eth_dominance < 0 OR eth_dominance > 100;


-- 3. Test liquidity ratio outliers (extremely high or zero)
SELECT
    snapshot_timestamp,
    market_liquidity_ratio
FROM fact_global_market
WHERE market_liquidity_ratio < 0.01 OR market_liquidity_ratio > 10;


-- 4. Test DeFi share inconsistencies
SELECT
    snapshot_timestamp,
    defi_volume_share
FROM fact_global_market
WHERE defi_volume_share < 0 OR defi_volume_share > 1;


-- 5. Test Stablecoin market share inconsistencies
SELECT
    snapshot_timestamp,
    stablecoin_market_share
FROM fact_global_market
WHERE stablecoin_market_share < 0 OR stablecoin_market_share > 1;


-- 6. Test crypto growth rate out of logical bounds
SELECT
    snapshot_timestamp,
    crypto_growth_rate_30d
FROM fact_global_market
WHERE crypto_growth_rate_30d < 0 OR crypto_growth_rate_30d > 0.5;


-- 7. Test for duplicate snapshot_timestamp (should be unique)
SELECT
    snapshot_timestamp,
    COUNT(*)
FROM fact_global_market
GROUP BY snapshot_timestamp
HAVING COUNT(*) > 1;


-- 8. Check if last_updated_timestamp is NULL or significantly outdated compared to snapshot_timestamp
-- Notes:
--   - The API updates data every 5 minutes.
--   - Any last_updated_timestamp earlier than snapshot_timestamp by more than 5 minutes may indicate a delay or ingestion issue.
SELECT
    snapshot_timestamp,
    last_updated_timestamp,
    EXTRACT(
        EPOCH FROM snapshot_timestamp - last_updated_timestamp
    ) AS delay_seconds
FROM fact_global_market
WHERE
    last_updated_timestamp IS NOT NULL
    AND snapshot_timestamp - last_updated_timestamp > INTERVAL '5 minutes';


-- 9. Test negative values in market metrics (not acceptable)
SELECT
    snapshot_timestamp,
    total_market_cap,
    total_volume_24h
FROM fact_global_market
WHERE total_market_cap < 0 OR total_volume_24h < 0;


-- 10. Compare tracked max/min daily new cryptos with actual values
SELECT
    snapshot_timestamp,
    new_cryptos_last_30d,
    max_daily_new_cryptos,
    min_daily_new_cryptos
FROM fact_global_market
WHERE
    new_cryptos_last_30d < min_daily_new_cryptos OR new_cryptos_last_30d > max_daily_new_cryptos;

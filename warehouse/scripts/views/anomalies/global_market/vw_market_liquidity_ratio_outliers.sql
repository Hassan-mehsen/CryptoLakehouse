-- ============================================================================
-- View      : vw_market_liquidity_ratio_outliers
-- Purpose   : Detect abnormal liquidity ratios in global market activity
-- Notes     :
--   - Ratio is calculated as: total_volume_24h / total_market_cap
--   - Normally expected to be between 0.01 and 10 (indicative range)
--   - Values outside this range may signal data quality issues or edge cases
-- ============================================================================

CREATE OR REPLACE VIEW vw_market_liquidity_ratio_outliers AS
SELECT
    snapshot_timestamp,
    total_volume_24h,
    total_market_cap,
    market_liquidity_ratio
FROM fact_global_market
WHERE market_liquidity_ratio < 0.01 OR market_liquidity_ratio > 10;

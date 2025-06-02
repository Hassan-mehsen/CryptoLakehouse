-- ============================================================================
-- View      : vw_crypto_market_cap_outliers
-- Purpose   : Identify cryptos with suspiciously high volume-to-market-cap ratios
-- Notes     : The ratio volume_24h / market_cap (calculated in Spark) normally ranges under 1.
--             Ratios >10 suggest anomalies: either overestimated volume or broken market cap values.
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_market_cap_outliers AS
SELECT
    crypto_id,
    snapshot_timestamp,
    volume_to_market_cap_ratio
FROM fact_crypto_market
WHERE volume_to_market_cap_ratio > 10;

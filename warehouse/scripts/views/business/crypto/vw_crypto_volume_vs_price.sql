-- ============================================================================
-- View      : vw_crypto_volume_vs_price
-- Purpose   : Analyze price, trading volume and short-term dynamics per crypto
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_volume_vs_price AS
SELECT
    f.crypto_id,
    c.name,
    f.snapshot_timestamp,
    f.price_usd,
    f.volume_24h_usd,
    f.volume_change_24h,
    f.percent_change_24h_usd,
    f.volume_to_market_cap_ratio
FROM fact_crypto_market AS f
INNER JOIN dim_crypto_id AS c ON f.crypto_id = c.crypto_id
WHERE f.price_usd IS NOT NULL AND f.volume_24h_usd IS NOT NULL
ORDER BY f.snapshot_timestamp DESC, f.volume_to_market_cap_ratio DESC;

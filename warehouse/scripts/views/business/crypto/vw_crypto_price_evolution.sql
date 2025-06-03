-- ============================================================================
-- View      : vw_crypto_price_evolution
-- Purpose   : Track historical price changes per crypto over time
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_price_evolution AS
SELECT
    f.crypto_id,
    c.name,
    f.snapshot_timestamp,
    f.price_usd,
    f.percent_change_24h_usd,
    f.percent_change_7d_usd
FROM fact_crypto_market AS f
INNER JOIN dim_crypto_id AS c ON f.crypto_id = c.crypto_id
WHERE f.price_usd IS NOT NULL
ORDER BY f.snapshot_timestamp DESC, price_usd DESC;

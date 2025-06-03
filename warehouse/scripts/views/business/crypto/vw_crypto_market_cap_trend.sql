-- ============================================================================
-- View      : vw_crypto_market_cap_trend
-- Purpose   : Track the evolution of market capitalization over time
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_market_cap_trend AS
SELECT
    f.crypto_id,
    c.name,
    f.snapshot_timestamp,
    f.market_cap_usd,
    f.market_cap_dominance_usd,
    fully_diluted_market_cap_usd
FROM fact_crypto_market AS f
INNER JOIN dim_crypto_id AS c ON f.crypto_id = c.crypto_id
WHERE f.market_cap_usd IS NOT NULL
ORDER BY market_cap_dominance_usd DESC;
--ORDER BY f.snapshot_timestamp DESC, f.market_cap_dominance_usd DESC;

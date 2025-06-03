-- ============================================================================
-- View      : vw_crypto_market_rank_history
-- Purpose   : Track market rank changes and exchange coverage per crypto
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_market_rank_history AS
SELECT
    f.crypto_id,
    c.name,
    f.snapshot_timestamp,
    f.cmc_rank,
    f.num_market_pairs
FROM fact_crypto_market AS f
INNER JOIN dim_crypto_id AS c ON f.crypto_id = c.crypto_id
WHERE f.cmc_rank IS NOT NULL
ORDER BY cmc_rank;

-- ============================================================================
-- View      : vw_category_dominance_index
-- Purpose   : Analyze category dominance per token (market_cap / num_tokens)
-- ============================================================================

CREATE OR REPLACE VIEW vw_category_dominance_index AS
SELECT
    fc.category_id,
    dc.name,
    fc.snapshot_timestamp,
    fc.dominance_per_token,
    fc.volume_to_market_cap_ratio
FROM fact_crypto_category AS fc
INNER JOIN dim_crypto_category AS dc ON fc.category_id = dc.category_id
WHERE fc.dominance_per_token IS NOT NULL
ORDER BY fc.category_id ASC, fc.snapshot_timestamp DESC;

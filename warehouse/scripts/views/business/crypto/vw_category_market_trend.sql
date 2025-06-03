-- ============================================================================
-- View      : vw_category_market_trend
-- Purpose   : Track market cap, volume, and token count per crypto category over time
-- ============================================================================

CREATE OR REPLACE VIEW vw_category_market_trend AS
SELECT
    fc.category_id,
    dc.name,
    fc.snapshot_timestamp,
    fc.num_tokens,
    fc.market_cap,
    fc.volume,
    fc.market_cap_change,
    fc.volume_change,
    fc.market_cap_change_rate,
    fc.volume_change_rate
FROM fact_crypto_category AS fc
INNER JOIN dim_crypto_category AS dc ON fc.category_id = dc.category_id
WHERE fc.market_cap IS NOT NULL AND fc.volume IS NOT NULL
ORDER BY fc.category_id ASC, fc.snapshot_timestamp DESC;

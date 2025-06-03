-- ============================================================================
-- View      : vw_top_categories_by_market_cap
-- Purpose   : Rank categories by market cap per snapshot
-- ============================================================================

CREATE OR REPLACE VIEW vw_top_categories_by_market_cap AS
SELECT
    fc.category_id,
    dc.name,
    fc.snapshot_timestamp,
    fc.market_cap,
    RANK() OVER (
        PARTITION BY fc.snapshot_timestamp
        ORDER BY fc.market_cap DESC
    ) AS category_rank
FROM fact_crypto_category AS fc
INNER JOIN dim_crypto_category AS dc ON fc.category_id = dc.category_id
WHERE fc.market_cap IS NOT NULL
ORDER BY fc.snapshot_timestamp DESC, category_rank ASC;

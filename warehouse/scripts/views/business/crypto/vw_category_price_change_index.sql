-- ============================================================================
-- View      : vw_category_price_change_index
-- Purpose   : Monitor the average price movement index per category
-- ============================================================================

CREATE OR REPLACE VIEW vw_category_price_change_index AS
SELECT
    fc.category_id,
    dc.name,
    fc.snapshot_timestamp,
    fc.avg_price_change,
    fc.price_change_index
FROM fact_crypto_category AS fc
INNER JOIN dim_crypto_category AS dc ON fc.category_id = dc.category_id
WHERE fc.price_change_index IS NOT NULL
ORDER BY fc.category_id ASC, fc.snapshot_timestamp DESC;

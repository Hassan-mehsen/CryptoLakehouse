-- ============================================================================
-- View      : vw_extreme_category_price_change_index
-- Purpose   : Detect outlier values in price_change_index (suspicious volatility or error)
-- Notes     : Detects if price_change_index is unusually high or negative
-- ============================================================================

CREATE OR REPLACE VIEW vw_extreme_category_price_change_index AS
SELECT
    category_id,
    snapshot_timestamp,
    price_change_index
FROM fact_crypto_category
WHERE price_change_index < -100 OR price_change_index > 1000;

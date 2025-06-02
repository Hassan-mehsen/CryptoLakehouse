-- ============================================================================
-- View      : vw_categories_volume_to_market_cap_anomalies
-- Purpose   : Highlight categories with suspiciously high volume-to-market-cap ratio
-- Notes     : Ratio > 10 often indicates inconsistency in upstream data or abnormal spikes
-- ============================================================================

CREATE OR REPLACE VIEW vw_categories_volume_to_market_cap_anomalies AS
SELECT
    category_id,
    snapshot_timestamp,
    volume_to_market_cap_ratio
FROM fact_crypto_category
WHERE volume_to_market_cap_ratio > 10;

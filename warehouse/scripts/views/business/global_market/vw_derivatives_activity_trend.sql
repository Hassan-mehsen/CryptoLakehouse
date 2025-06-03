-- ============================================================================
-- View      : vw_derivatives_activity_trend
-- Purpose   : Monitor derivatives trading volumes and their 24h changes
-- Notes     : Useful for understanding speculative activity
-- ============================================================================

CREATE OR REPLACE VIEW vw_derivatives_activity_trend AS
SELECT
    snapshot_timestamp,
    derivatives_volume_24h,
    derivatives_volume_24h_reported,
    derivatives_24h_percentage_change
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

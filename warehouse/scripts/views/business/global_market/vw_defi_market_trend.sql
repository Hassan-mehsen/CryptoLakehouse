-- ============================================================================
-- View      : vw_defi_market_trend
-- Purpose   : Monitor DeFi sector trends in volume and market share
-- Notes     : Includes DeFi metrics and dominance relative to global volume
-- ============================================================================

CREATE OR REPLACE VIEW vw_defi_market_trend AS
SELECT
    snapshot_timestamp,
    defi_market_cap,
    defi_volume_24h,
    defi_volume_24h_reported,
    defi_volume_share,
    defi_24h_percentage_change,
    check_defi_volume_share
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

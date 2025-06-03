-- ============================================================================
-- View      : vw_global_market_overview
-- Purpose   : Track global market cap and volume trends over time
-- Notes     : Useful for plotting overall market evolution
-- ============================================================================

CREATE OR REPLACE VIEW vw_global_market_overview AS
SELECT
    snapshot_timestamp,
    total_market_cap,
    total_volume_24h,
    total_market_cap_yesterday,
    total_volume_24h_yesterday,
    total_market_cap_yesterday_change,
    total_volume_24h_yesterday_change,
    total_volume_24h_reported,
    market_liquidity_ratio
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

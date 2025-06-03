-- ============================================================================
-- View      : vw_stablecoin_market_trend
-- Purpose   : Track stablecoin market cap and volume share over time
-- Notes     : Focuses on stablecoin activity vs. total market
-- ============================================================================

CREATE OR REPLACE VIEW vw_stablecoin_market_trend AS
SELECT
    snapshot_timestamp,
    stablecoin_market_cap,
    stablecoin_volume_24h,
    stablecoin_volume_24h_reported,
    stablecoin_market_share,
    stablecoin_24h_percentage_change,
    check_stablecoin_market_share
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;



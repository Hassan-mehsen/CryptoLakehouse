-- ============================================================================
-- View      : vw_stablecoin_market_share_anomalies
-- Purpose   : Detect anomalies in stablecoin dominance over global market cap
-- Notes     :
--   - Ratio = stablecoin_market_cap / total_market_cap
--   - Should be between 0.0 and 1.0 (0% - 100%)
--   - Values outside this range are logically invalid
-- ============================================================================

CREATE OR REPLACE VIEW vw_stablecoin_market_share_anomalies AS
SELECT
    snapshot_timestamp,
    stablecoin_market_cap,
    total_market_cap,
    stablecoin_market_share
FROM fact_global_market
WHERE stablecoin_market_share < 0 OR stablecoin_market_share > 1;

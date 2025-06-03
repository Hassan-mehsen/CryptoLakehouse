-- ============================================================================
-- View      : vw_btc_eth_dominance_trend
-- Purpose   : Track Bitcoin and Ethereum dominance over time and their daily variations
-- Notes     : Useful to visualize BTC/ETH influence in market dynamics
-- ============================================================================

CREATE OR REPLACE VIEW vw_btc_eth_dominance_trend AS
SELECT
    snapshot_timestamp,
    btc_dominance,
    eth_dominance,
    btc_dominance_yesterday,
    eth_dominance_yesterday,
    btc_dominance_24h_change,
    eth_dominance_24h_change,
    btc_dominance_delta,
    eth_dominance_delta
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

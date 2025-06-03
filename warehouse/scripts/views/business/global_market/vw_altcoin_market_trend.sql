-- ============================================================================
-- View      : vw_altcoin_market_trend
-- Purpose   : Follow altcoin activity across market cap and volume
-- Notes     : Complements BTC/ETH views by focusing on rest of market
-- ============================================================================

CREATE OR REPLACE VIEW vw_altcoin_market_trend AS
SELECT
    snapshot_timestamp,
    altcoin_market_cap,
    altcoin_volume_24h,
    altcoin_volume_24h_reported
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

-- ============================================================================
-- View      : vw_market_participants_trend
-- Purpose   : Track the evolution of active cryptocurrencies, exchanges, and market pairs
-- Notes     : Helps visualize market size and participation growth
-- ============================================================================

CREATE OR REPLACE VIEW vw_market_participants_trend AS
SELECT
    snapshot_timestamp,
    active_cryptocurrencies,
    total_cryptocurrencies,
    active_market_pairs,
    active_exchanges,
    total_exchanges
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;

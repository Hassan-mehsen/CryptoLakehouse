-- ============================================================================
-- View      : vw_missing_critical_market_metrics
-- Purpose   : Detect rows missing critical market KPIs like market cap, volume or btc dominance
-- Notes     : 
--   - These fields should almost always be populated from the API.
--   - Repeated NULLs may indicate upstream issues or transformation bugs.
-- ============================================================================

CREATE OR REPLACE VIEW vw_missing_critical_market_metrics AS
SELECT
    snapshot_timestamp,
    total_market_cap,
    total_volume_24h,
    btc_dominance
FROM fact_global_market
WHERE
    total_market_cap IS NULL
    OR total_volume_24h IS NULL
    OR btc_dominance IS NULL;

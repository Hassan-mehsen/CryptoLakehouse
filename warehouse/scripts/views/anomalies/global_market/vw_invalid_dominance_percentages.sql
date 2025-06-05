-- ============================================================================
-- View      : vw_invalid_dominance_percentages
-- Purpose   : Detect invalid or inconsistent dominance values for BTC and ETH
-- Author    : Hassan Mehsen (Portfolio project)
-- Notes     : 
--   - Dominance values should always be between 0 and 90.
--   - Any value outside this range is invalid and likely caused by data parsing or transformation error.
-- ============================================================================

CREATE OR REPLACE VIEW vw_invalid_dominance_percentages AS
SELECT
    snapshot_timestamp,
    btc_dominance,
    eth_dominance
FROM fact_global_market
WHERE
    btc_dominance < 0 OR btc_dominance > 90
    OR eth_dominance < 0 OR eth_dominance > 90;

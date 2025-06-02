-- ============================================================================
-- View      : vw_supply_utilization_outliers
-- Purpose   : Detect cryptos with illogical supply usage over 100%
-- Notes     : The KPI supply_utilization = circulating_supply / max_supply. 
--             Values >1.1 indicate broken max_supply, or faulty reporting by API sources.
-- ============================================================================

CREATE OR REPLACE VIEW vw_supply_utilization_outliers AS
SELECT
    crypto_id,
    snapshot_timestamp,
    supply_utilization
FROM fact_crypto_market
WHERE supply_utilization > 1.1;

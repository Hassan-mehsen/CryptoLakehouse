-- ============================================================================
-- View      : vw_defi_volume_share_anomalies
-- Purpose   : Detect inconsistent DeFi volume share values (should be in [0,1])
-- Notes     :
--   - DeFi volume share is calculated as: defi_volume_24h / total_volume_24h
--   - It must always be between 0.0 and 1.0.
--   - Any negative or > 1 values indicate data transformation or ELT error.
-- ============================================================================

CREATE OR REPLACE VIEW vw_defi_volume_share_anomalies AS
SELECT
    snapshot_timestamp,
    defi_volume_24h,
    total_volume_24h,
    defi_volume_share
FROM fact_global_market
WHERE defi_volume_share < 0 OR defi_volume_share > 1;

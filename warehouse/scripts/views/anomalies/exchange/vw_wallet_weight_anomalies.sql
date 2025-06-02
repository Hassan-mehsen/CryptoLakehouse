-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_wallet_weight_anomalies
-- Purpose: Expose all wallet records with wallet_weight > 100 (suspicious or abnormal concentration)
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_wallet_weight_anomalies AS
SELECT
    exchange_id,
    snapshot_timestamp,
    wallet_address,
    crypto_id,
    wallet_weight,
    total_usd_value,
    spot_volume_usd
FROM fact_exchange_assets
WHERE wallet_weight > 100;

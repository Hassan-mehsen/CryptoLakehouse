-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_missing_spot_volume
-- Purpose: Monitor fact_exchange_assets rows where spot_volume_usd is still missing
-- Notes: These rows were either not filled by the procedure or data from / exchange/info was not available yet
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_missing_spot_volume AS
SELECT
 exchange_id,
 snapshot_timestamp,
 wallet_address,
 crypto_id,
 balance,
 currency_price_usd,
 total_usd_value
FROM fact_exchange_assets
WHERE spot_volume_usd IS NULL;

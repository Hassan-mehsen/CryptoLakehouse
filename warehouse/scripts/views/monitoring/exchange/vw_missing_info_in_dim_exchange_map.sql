-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_pipeline_misalignment_exchange_map
-- Purpose: Detect incomplete rows in dim_exchange_map where all key fields are NULL
-- Notes: Typically occurs when / exchange/map is updated but / exchange/info is delayed
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_pipeline_misalignment_exchange_map AS
SELECT
 exchange_id,
 snapshot_timestamp
FROM dim_exchange_map
WHERE maker_fee IS NULL AND taker_fee IS NULL AND weekly_visits IS NULL;

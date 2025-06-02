-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_weekly_visits_outliers
-- Purpose: Flag exchanges with extremely high weekly_visits count
-- Notes: Useful for detecting parsing issues or unexpected popularity spikes
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_weekly_visits_outliers AS
SELECT
    exchange_id,
    snapshot_timestamp,
    weekly_visits
FROM dim_exchange_map
WHERE weekly_visits > 100000000;

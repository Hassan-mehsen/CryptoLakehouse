-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_exchange_weekly_visits_trend
-- Purpose: Monitor the evolution of weekly visits for each exchange
-- Notes: Enables time-series visualizations of exchange popularity
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_exchange_weekly_visits_trend AS
SELECT
 m.exchange_id,
 i.name,
 m.snapshot_timestamp,
 m.weekly_visits
FROM dim_exchange_map m
JOIN dim_exchange_id i ON m.exchange_id = i.exchange_id
WHERE m.weekly_visits IS NOT NULL
ORDER BY m.exchange_id, m.snapshot_timestamp DESC;

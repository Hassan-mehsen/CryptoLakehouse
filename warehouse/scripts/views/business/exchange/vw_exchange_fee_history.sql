-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_exchange_fee_history
-- Purpose: Track the historical evolution of maker and taker fees per exchange
-- Notes: Useful for fee trend analysis and comparing exchanges over time
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_exchange_fee_history AS
SELECT
 m.exchange_id,
 i.name,
 m.snapshot_timestamp,
 m.maker_fee,
 m.taker_fee
FROM dim_exchange_map m
JOIN dim_exchange_id i ON m.exchange_id = i.exchange_id
ORDER BY m.maker_fee DESC, m.taker_fee DESC, m.snapshot_timestamp DESC;
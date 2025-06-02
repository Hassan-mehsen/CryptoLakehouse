-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_high_fee_exchanges
-- Purpose: Identify exchanges with suspiciously high maker/taker fees
-- Notes: High values may reflect parsing errors or platform misconfigurations
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_high_fee_exchanges AS
SELECT
    exchange_id,
    snapshot_timestamp,
    maker_fee,
    taker_fee
FROM dim_exchange_map
WHERE maker_fee > 0.5 OR taker_fee > 0.5;

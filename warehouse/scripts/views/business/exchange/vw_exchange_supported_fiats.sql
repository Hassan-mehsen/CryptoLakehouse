-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_exchange_supported_fiats
-- Purpose: List all fiat currencies supported by each exchange
-- Location: scripts/views/business/vw_exchange_supported_fiats.sql
-- Notes: Can be used to explore fiat compatibility across platforms
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_exchange_supported_fiats AS
SELECT
  ex.exchange_id,
  id.name,
  UNNEST(ex.fiats) AS fiat_currency
FROM dim_exchange_info ex
JOIN dim_exchange_id id ON ex.exchange_id = id.exchange_id
WHERE ex.fiats IS NOT NULL
GROUP BY ex.exchange_id, id.name
ORDER BY ex.exchange_id,fiat_currency DESC;
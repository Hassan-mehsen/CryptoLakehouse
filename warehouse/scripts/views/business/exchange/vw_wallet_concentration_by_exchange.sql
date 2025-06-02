-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_wallet_concentration_by_exchange
-- Purpose: Analyze concentration of funds per exchange using wallet_weight KPIs
-- Notes: Useful for comparing dispersion of funds across exchanges and time
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_wallet_concentration_by_exchange AS
SELECT
    fea.exchange_id,
    ei.name,
    fea.snapshot_timestamp,
    COUNT(fea.*) AS wallet_count,
    AVG(fea.wallet_weight) AS avg_weight,
    MAX(fea.wallet_weight) AS max_weight
FROM fact_exchange_assets AS fea
INNER JOIN dim_exchange_id AS ei ON fea.exchange_id = ei.exchange_id
WHERE fea.wallet_weight IS NOT NULL
GROUP BY fea.exchange_id, ei.name, fea.snapshot_timestamp
ORDER BY avg_weight DESC, max_weight DESC;

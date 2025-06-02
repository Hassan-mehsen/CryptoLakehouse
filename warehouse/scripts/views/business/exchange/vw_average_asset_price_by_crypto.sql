-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_average_asset_price_by_crypto
-- Purpose: Track average price(USD) of crypto assets per snapshot
-- Notes: Can be used to visualize market trends across exchanges
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_average_asset_price_by_crypto AS
SELECT
    fea.crypto_id,
    ci.name,
    ci.symbol,
    fea.snapshot_timestamp,
    AVG(fea.currency_price_usd) AS avg_price_usd
FROM fact_exchange_assets AS fea
INNER JOIN dim_crypto_id AS ci ON fea.crypto_id = ci.crypto_id
WHERE fea.currency_price_usd IS NOT NULL
GROUP BY fea.crypto_id, ci.name, ci.symbol, fea.snapshot_timestamp
ORDER BY fea.crypto_id;

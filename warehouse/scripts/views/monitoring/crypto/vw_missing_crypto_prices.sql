-- ============================================================================
-- View      : vw_missing_crypto_prices
-- Purpose   : Monitor rows in fact_crypto_market with missing USD price
-- Notes     : price_usd is essential for all financial KPIs and business charts.
--             Null values might appear if the /cryptocurrency/listings/latest endpoint fails partially.
-- ============================================================================

CREATE OR REPLACE VIEW vw_missing_crypto_prices AS
SELECT
    crypto_id,
    snapshot_timestamp
FROM fact_crypto_market
WHERE price_usd IS NULL;

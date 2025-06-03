-- ============================================================================
-- View      : vw_crypto_creation_activity
-- Purpose   : Follow trends in the creation of new cryptocurrencies
-- Notes     : Useful for understanding ecosystem expansion
-- ============================================================================

-- bad data from the API !! 

CREATE OR REPLACE VIEW vw_crypto_creation_activity AS
SELECT
    snapshot_timestamp,
    total_cryptocurrencies,
    new_cryptos_today,
    new_cryptos_last_24h,
    new_cryptos_last_7d,
    new_cryptos_last_30d,
    crypto_growth_rate_30d,
    max_daily_new_cryptos,
    max_new_cryptos_day,
    min_daily_new_cryptos,
    min_new_cryptos_day
FROM fact_global_market
ORDER BY snapshot_timestamp DESC;




-- ============================================================================
-- View      : vw_crypto_missing_urls
-- Purpose   : Detect cryptos without logo or website in dim_crypto_info
-- Notes     : Logos and websites are fetched from /cryptocurrency/info. 
--             These fields are essential for display and client usage. 
--             Missing them reduces data completeness and limits dashboard visuals.
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_missing_urls AS
SELECT crypto_id
FROM dim_crypto_info
WHERE logo_url IS NULL OR website_url IS NULL;

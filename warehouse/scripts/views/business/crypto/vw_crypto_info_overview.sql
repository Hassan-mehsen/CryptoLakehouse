-- ============================================================================
-- View      : vw_crypto_info_overview
-- Purpose   : Present static crypto metadata including launch dates, description,
--             logos and official URLs for business-facing dashboards
-- Notes     : View designed for informative UI use cases (e.g. Metabase cards with logos).
--             All columns come from /cryptocurrency/info.
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_info_overview AS
SELECT
    ci.crypto_id,
    dci.name,
    ci.slug,
    ci.category,
    ci.description,
    ci.logo_url,
    ci.website_url,
    ci.twitter_url,
    ci.source_code_url,
    ci.technical_doc_url
FROM dim_crypto_info ci
join dim_crypto_id dci on ci.crypto_id = dci.crypto_id;

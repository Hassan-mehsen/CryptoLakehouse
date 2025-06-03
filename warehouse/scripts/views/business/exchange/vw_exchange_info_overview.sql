-- ============================================================================
-- View      : vw_exchange_info_overview
-- Purpose   : Provide a business-friendly overview of each exchange with
--             metadata and useful links (e.g. logo, website, fees)
-- Notes     : Ideal for business dashboards with logos or drill-downs in Metabase.
--             All fields are static and come from /exchange/info.
-- ============================================================================

CREATE OR REPLACE VIEW vw_exchange_info_overview AS
SELECT
    ei.exchange_id,
    di.name,
    ei.slug,
    ei.description,
    ei.logo_url,
    ei.website_url,
    ei.fee_url
FROM dim_exchange_info ei
JOIN dim_exchange_id di ON ei.exchange_id = di.exchange_id;

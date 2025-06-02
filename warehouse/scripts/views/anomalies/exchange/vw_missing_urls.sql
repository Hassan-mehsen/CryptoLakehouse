-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_missing_urls
-- Purpose: Identify exchanges missing website or logo URLs
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

CREATE OR REPLACE VIEW vw_missing_urls AS
SELECT
    exchange_id,
    website_url,
    logo_url
FROM dim_exchange_info
WHERE website_url IS NULL OR logo_url IS NULL;

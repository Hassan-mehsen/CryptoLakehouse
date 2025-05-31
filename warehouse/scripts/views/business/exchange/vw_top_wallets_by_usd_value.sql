-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
-- View: vw_top_wallets_by_usd_value
-- Purpose: Expose individual wallets with highest total USD value per snapshot
-- Notes: Can be filtered in Metabase to show top N wallets over time
-- == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==


CREATE OR REPLACE VIEW vw_top_wallets_by_usd_value AS
SELECT
    fea.exchange_id,
    ei.name,
    fea.snapshot_timestamp,
    fea.wallet_address,
    fea.crypto_id,
    fea.total_usd_value,
    ROW_NUMBER() OVER (
        PARTITION BY fea.exchange_id, fea.snapshot_timestamp
        ORDER BY fea.total_usd_value DESC
    ) AS wallet_rank
FROM fact_exchange_assets fea
JOIN dim_exchange_id ei ON fea.exchange_id = ei.exchange_id
WHERE fea.total_usd_value IS NOT NULL
ORDER BY fea.snapshot_timestamp DESC,fea.exchange_id;

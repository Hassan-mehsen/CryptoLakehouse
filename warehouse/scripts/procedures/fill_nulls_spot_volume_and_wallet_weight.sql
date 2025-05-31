-- ============================================================================
-- Procedure : fill_nulls_spot_volume_and_wallet_weight
-- Purpose   : Fill missing spot_volume_usd values in fact_exchange_assets
--             using the latest known value per exchange_id before the current
--             snapshot, and recalculate wallet_weight accordingly.
-- Usage     : CALL fill_nulls_spot_volume_and_wallet_weight();
-- Notes     : Only one NULL snapshot per exchange_id (most recent) is corrected per run.
-- ============================================================================

CREATE OR REPLACE PROCEDURE fill_nulls_spot_volume_and_wallet_weight()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Select the most recent snapshot per exchange_id where spot_volume_usd is NULL.
    -- This happens because spot_volume_usd comes from the /exchange/info endpoint,
    -- while the rest of the data comes from /exchange/assets, and both are ingested at different frequencies.
    -- We use DISTINCT ON (exchange_id) to avoid processing duplicates.
    -- This is safe and optimal since each exchange has one spot_volume_usd per snapshot.
    WITH latest_null_spot_per_exchange AS (
        SELECT DISTINCT ON (exchange_id)
            exchange_id,
            snapshot_timestamp
        FROM fact_exchange_assets
        WHERE spot_volume_usd IS NULL
        ORDER BY exchange_id, snapshot_timestamp DESC
    ),

    -- Step 2: For each of those exchange_id + snapshot_timestamp pairs,
    -- find the most recent previous snapshot (before the NULL one) with a valid spot_volume_usd.
    -- We use a LATERAL JOIN to correlate subqueries per row (per exchange_id).
    latest_valid_spot_volume AS (
        SELECT
            n.exchange_id,
            n.snapshot_timestamp,
            prev.spot_volume_usd
        FROM latest_null_spot_per_exchange n
        JOIN LATERAL (
            SELECT a.spot_volume_usd
            FROM fact_exchange_assets a
            WHERE a.exchange_id = n.exchange_id
              AND a.snapshot_timestamp < n.snapshot_timestamp
              AND a.spot_volume_usd IS NOT NULL
            ORDER BY a.snapshot_timestamp DESC
            LIMIT 1
        ) prev ON TRUE -- the conditon is in the nested query 
    )

    -- Step 3: Update all matching rows in fact_exchange_assets with the recovered spot_volume_usd,
    -- and recalculate wallet_weight accordingly (wallet_weight = total_usd_value / spot_volume_usd).
    UPDATE fact_exchange_assets fea
    SET
        spot_volume_usd = lvs.spot_volume_usd,
        wallet_weight = fea.total_usd_value / lvs.spot_volume_usd
    FROM latest_valid_spot_volume lvs
    WHERE fea.exchange_id = lvs.exchange_id
      AND fea.snapshot_timestamp = lvs.snapshot_timestamp
      AND fea.spot_volume_usd IS NULL;

    RAISE NOTICE 'spot_volume_usd and wallet_weight fields updated successfully.';
END;
$$;

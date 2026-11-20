-- C_LOAD_DATES: newest business date each target holds and the batch that last loaded it.
SELECT TARGET_TABLE, LAYER, LAST_LOAD_DT, LAST_RUN_DT, LAST_BATCH_ID
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES
ORDER BY CASE LAYER WHEN 'BRONZE' THEN 1 WHEN 'SILVER' THEN 2 WHEN 'GOLD' THEN 3 ELSE 4 END, TARGET_TABLE;

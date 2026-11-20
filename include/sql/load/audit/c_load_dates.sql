-- C_LOAD_DATES: runs last, only when every gate passed. One row per target named in C_BATCH_SCRIPTS.
-- LAST_LOAD_DT never moves backwards here; only load/gold/rewind_from_ds.sql moves Gold back.
MERGE INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES t
USING (
    SELECT TRIM(f.VALUE) AS TARGET_TABLE, MAX(s.LAYER) AS LAYER
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_SCRIPTS s,
         LATERAL SPLIT_TO_TABLE(s.TARGET_TABLE, ',') f
    WHERE s.ACTIVE_FLG AND s.LAYER <> 'CONTROL' AND s.TARGET_TABLE IS NOT NULL
    GROUP BY 1
) s
ON t.TARGET_TABLE = s.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
    LAYER = s.LAYER,
    LAST_LOAD_DT = GREATEST(t.LAST_LOAD_DT, '{{ ds }}'::DATE),
    LAST_RUN_DT = '{{ ds }}'::DATE,
    LAST_BATCH_ID = '{{ run_id }}',
    UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (TARGET_TABLE, LAYER, LAST_LOAD_DT, LAST_RUN_DT, LAST_BATCH_ID, UPDATE_TS)
    VALUES (s.TARGET_TABLE, s.LAYER, '{{ ds }}'::DATE, '{{ ds }}'::DATE, '{{ run_id }}', CURRENT_TIMESTAMP());

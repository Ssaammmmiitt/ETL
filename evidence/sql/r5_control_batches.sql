-- One row per batch (Airflow run) per business day, from C_BATCH_LOG + C_PARAM + C_REJ_TABLE.
SELECT
    l.BUSINESS_DT,
    l.BATCH_ID,
    COUNT(DISTINCT l.TASK_ID)                          AS TASKS_LOGGED,
    COUNT_IF(l.STATUS <> 'SUCCESS')                    AS FAILED_OR_RETRY,
    DATEDIFF(second, MIN(l.START_TS), MAX(l.END_TS))   AS WALL_SEC,
    SUM(l.ROWS_AFFECTED)                               AS ROWS_AFFECTED,
    (SELECT COUNT(*)
     FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM p
     WHERE p.BATCH_ID = l.BATCH_ID)                    AS PARAMS,
    (SELECT COUNT(*)
     FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE r
     WHERE r.INGEST_DT = l.BUSINESS_DT)                AS REJECTED_ROWS
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_LOG l
WHERE l.BUSINESS_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
GROUP BY l.BUSINESS_DT, l.BATCH_ID
ORDER BY l.BUSINESS_DT, MIN(l.START_TS);

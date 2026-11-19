-- Gate G8 helper: return today's TMP inventory count and the prior loaded day's STG count.
-- The DAG Python task fails when |today - prior| / prior > 0.20 (skip if no prior day).

SELECT
    (
        SELECT COUNT(*)
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT
        WHERE _INGEST_DT = '{{ ds }}'
    ) AS TODAY_CNT,
    (
        SELECT COUNT(*)
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY
        WHERE _INGEST_DT = (
            SELECT MAX(_INGEST_DT)
            FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY
            WHERE _INGEST_DT < '{{ ds }}'::DATE
        )
    ) AS PRIOR_CNT,
    (
        SELECT MAX(_INGEST_DT)
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY
        WHERE _INGEST_DT < '{{ ds }}'::DATE
    ) AS PRIOR_DT
;

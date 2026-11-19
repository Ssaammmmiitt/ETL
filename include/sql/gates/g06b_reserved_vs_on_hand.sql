-- Gate G6b (alert only): RESERVED_QTY > ON_HAND_QTY on today's cleaned snapshot.
-- The DAG task prints these rows; it does not fail the run and does not quarantine.
SELECT
    SNAPSHOT_DT,
    ITEM_ID,
    LOCN_ID,
    ON_HAND_QTY,
    RESERVED_QTY,
    AVAILABLE_QTY
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT
WHERE _INGEST_DT = '{{ ds }}'
  AND RESERVED_QTY > ON_HAND_QTY
ORDER BY ITEM_ID, LOCN_ID;

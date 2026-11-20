-- Gate G7: sales lines whose ITEM_ID / LOCN_ID do not resolve to a *current* dim key.
-- Quarantine only (alert is a DAG task). Idempotent clear of prior G7 rows for day D.

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
 WHERE INGEST_DT = '{{ ds }}'
   AND GATE_ID = 'G7'
   AND OBJECT_NM = 'FCT_SALES';

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    t._INGEST_DT,
    t._SOURCE_FILE,
    'FCT_SALES',
    'G7',
    CASE
        WHEN i.ITEM_KY IS NULL AND l.LOCN_KY IS NULL THEN 'ORPHAN_ITEM_AND_LOCN'
        WHEN i.ITEM_KY IS NULL THEN 'ORPHAN_ITEM'
        ELSE 'ORPHAN_LOCN'
    END,
    OBJECT_CONSTRUCT(t.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE t
LEFT JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM i
    ON i.ITEM_ID = t.ITEM_ID AND i.CURRENT_FLG
LEFT JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l
    ON l.LOCN_ID = t.LOCN_ID AND l.CURRENT_FLG
WHERE t._INGEST_DT = '{{ ds }}'
  AND (i.ITEM_KY IS NULL OR l.LOCN_KY IS NULL);

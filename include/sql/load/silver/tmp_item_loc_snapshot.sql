-- Load TMP_ITEM_LOC_SNAPSHOT for day D from STG_INVENTORY.
-- G12 on ON_HAND_QTY; G6b (RESERVED > ON_HAND) is alert-only in the DAG, still loads.
-- TRY_TO_NUMBER must include (precision, scale) — default scale is 0 (integer).

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
 WHERE INGEST_DT = '{{ ds }}'
   AND OBJECT_NM = 'TMP_ITEM_LOC_SNAPSHOT';

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT
 WHERE _INGEST_DT = '{{ ds }}';

-- G3 — keys
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_ITEM_LOC_SNAPSHOT',
    'G3',
    CASE
        WHEN NULLIF(TRIM(s.ITEM_ID), '') IS NULL THEN 'NULL_ITEM_ID'
        WHEN NULLIF(TRIM(s.LOCN_ID), '') IS NULL THEN 'NULL_LOCN_ID'
        ELSE 'NULL_SNAPSHOT_DT'
    END,
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY s
WHERE s._INGEST_DT = '{{ ds }}'
  AND (
        NULLIF(TRIM(s.ITEM_ID), '') IS NULL
     OR NULLIF(TRIM(s.LOCN_ID), '') IS NULL
     OR TRY_TO_DATE(TRIM(s.SNAPSHOT_DT)) IS NULL
  );

-- G12 — on-hand absolute limit
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_ITEM_LOC_SNAPSHOT',
    'G12',
    'ON_HAND_TOO_HIGH',
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY s
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_ITEM_LOC_SNAPSHOT'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  )
  AND TRY_TO_NUMBER(TRIM(s.ON_HAND_QTY), 18, 3) > {{ var.json.medallion_config.outlier_rules.max_on_hand_qty }};

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT (
    SNAPSHOT_DT, ITEM_ID, LOCN_ID,
    ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY, AVAILABLE_QTY,
    UNIT_PRICE_AMT, PROMO_PRICE_AMT,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM
)
SELECT
    TRY_TO_DATE(TRIM(s.SNAPSHOT_DT)),
    TRIM(s.ITEM_ID),
    TRIM(s.LOCN_ID),
    TRY_TO_NUMBER(TRIM(s.ON_HAND_QTY), 18, 3),
    TRY_TO_NUMBER(TRIM(s.RESERVED_QTY), 18, 3),
    TRY_TO_NUMBER(TRIM(s.IN_TRANSIT_QTY), 18, 3),
    (TRY_TO_NUMBER(TRIM(s.ON_HAND_QTY), 18, 3) - TRY_TO_NUMBER(TRIM(s.RESERVED_QTY), 18, 3)),
    TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2),
    TRY_TO_NUMBER(NULLIF(TRIM(s.PROMO_PRICE_AMT), ''), 18, 2),
    s._INGEST_DT,
    s._SOURCE_FILE,
    s._SOURCE_ROW_NUM
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY s
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_ITEM_LOC_SNAPSHOT'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  );

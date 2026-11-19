-- Load TMP_SALES_LINE for day D from STG_SALES.
-- Order: clear day → quarantine G3/G4/G5/G6/G12 → insert survivors.
-- Note: TRY_TO_NUMBER(x) defaults to scale 0 (integer) — always pass (precision, scale).
-- Note: after a JOIN, use OBJECT_CONSTRUCT(s.*) — OBJECT_CONSTRUCT(*) errors on duplicate keys.

-- 1) Idempotent clear for day D
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
 WHERE INGEST_DT = '{{ ds }}'
   AND OBJECT_NM = 'TMP_SALES_LINE';

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE
 WHERE _INGEST_DT = '{{ ds }}';

-- 2) G3 — business keys not null (after trim)
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_SALES_LINE',
    'G3',
    CASE
        WHEN NULLIF(TRIM(s.TRANSACTION_ID), '') IS NULL THEN 'NULL_TRANSACTION_ID'
        WHEN NULLIF(TRIM(s.LINE_NUM), '') IS NULL THEN 'NULL_LINE_NUM'
        WHEN NULLIF(TRIM(s.LOCN_ID), '') IS NULL THEN 'NULL_LOCN_ID'
        WHEN NULLIF(TRIM(s.ITEM_ID), '') IS NULL THEN 'NULL_ITEM_ID'
        ELSE 'NULL_KEY'
    END,
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
WHERE s._INGEST_DT = '{{ ds }}'
  AND (
        NULLIF(TRIM(s.TRANSACTION_ID), '') IS NULL
     OR NULLIF(TRIM(s.LINE_NUM), '') IS NULL
     OR NULLIF(TRIM(s.LOCN_ID), '') IS NULL
     OR NULLIF(TRIM(s.ITEM_ID), '') IS NULL
  );

-- 3) G4 — duplicate (TRANSACTION_ID, LINE_NUM): keep highest _SOURCE_ROW_NUM
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_SALES_LINE',
    'G4',
    'DUPLICATE_TXN_LINE',
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
INNER JOIN (
    SELECT
        TRIM(TRANSACTION_ID) AS TRANSACTION_ID,
        TRIM(LINE_NUM) AS LINE_NUM,
        MAX(_SOURCE_ROW_NUM) AS KEEP_ROW
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
    WHERE _INGEST_DT = '{{ ds }}'
      AND NULLIF(TRIM(TRANSACTION_ID), '') IS NOT NULL
      AND NULLIF(TRIM(LINE_NUM), '') IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) > 1
) d
  ON TRIM(s.TRANSACTION_ID) = d.TRANSACTION_ID
 AND TRIM(s.LINE_NUM) = d.LINE_NUM
 AND s._SOURCE_ROW_NUM <> d.KEEP_ROW
WHERE s._INGEST_DT = '{{ ds }}';

-- 4) G5 — SALE_DT within D-7 .. D
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_SALES_LINE',
    'G5',
    'SALE_DT_OUT_OF_WINDOW',
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_SALES_LINE'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  )
  AND (
        TRY_TO_DATE(TRIM(s.SALE_DT)) IS NULL
     OR TRY_TO_DATE(TRIM(s.SALE_DT)) < DATEADD(day, -7, '{{ ds }}'::DATE)
     OR TRY_TO_DATE(TRIM(s.SALE_DT)) > '{{ ds }}'::DATE
  );

-- 5) G6 — SOLD_QTY > 0 and NET ≈ SALES − DISCOUNT (±0.01)
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_SALES_LINE',
    'G6',
    CASE
        WHEN TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) IS NULL
          OR TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) <= 0
            THEN 'BAD_OR_NONPOSITIVE_QTY'
        ELSE 'NET_NE_SALES_MINUS_DISCOUNT'
    END,
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_SALES_LINE'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  )
  AND (
        TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) IS NULL
     OR TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) <= 0
     OR TRY_TO_NUMBER(TRIM(s.SALES_AMT), 18, 2) IS NULL
     OR TRY_TO_NUMBER(TRIM(s.DISCOUNT_AMT), 18, 2) IS NULL
     OR TRY_TO_NUMBER(TRIM(s.NET_SALES_AMT), 18, 2) IS NULL
     OR ABS(
            TRY_TO_NUMBER(TRIM(s.NET_SALES_AMT), 18, 2)
          - (TRY_TO_NUMBER(TRIM(s.SALES_AMT), 18, 2) - TRY_TO_NUMBER(TRIM(s.DISCOUNT_AMT), 18, 2))
        ) > 0.01
  );

-- 6) G12 — configurable outlier limits (Variable outlier_rules)
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    (INGEST_DT, SOURCE_FILE, OBJECT_NM, GATE_ID, REASON_CD, RAW_RECORD)
SELECT
    s._INGEST_DT,
    s._SOURCE_FILE,
    'TMP_SALES_LINE',
    'G12',
    CASE
        WHEN TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) > {{ var.json.medallion_config.outlier_rules.max_sold_qty }}
            THEN 'QTY_TOO_HIGH'
        WHEN TRY_TO_NUMBER(TRIM(s.NET_SALES_AMT), 18, 2) > {{ var.json.medallion_config.outlier_rules.max_line_net_amt }}
            THEN 'LINE_AMT_TOO_HIGH'
        WHEN ref.REF_PRICE IS NOT NULL
         AND TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2)
             > ref.REF_PRICE * {{ var.json.medallion_config.outlier_rules.price_ratio_max }}
            THEN 'PRICE_TOO_HIGH'
        WHEN ref.REF_PRICE IS NOT NULL AND ref.REF_PRICE > 0
         AND TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2)
             < ref.REF_PRICE * {{ var.json.medallion_config.outlier_rules.price_ratio_min }}
            THEN 'PRICE_TOO_LOW'
        ELSE 'OUTLIER'
    END,
    OBJECT_CONSTRUCT(s.*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
LEFT JOIN (
    SELECT
        TRIM(ITEM_ID) AS ITEM_ID,
        MAX(TRY_TO_NUMBER(TRIM(UNIT_PRICE_AMT), 18, 2)) AS REF_PRICE
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY
    WHERE _INGEST_DT = '{{ ds }}'
    GROUP BY 1
) ref
  ON TRIM(s.ITEM_ID) = ref.ITEM_ID
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_SALES_LINE'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  )
  AND (
        TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3) > {{ var.json.medallion_config.outlier_rules.max_sold_qty }}
     OR TRY_TO_NUMBER(TRIM(s.NET_SALES_AMT), 18, 2) > {{ var.json.medallion_config.outlier_rules.max_line_net_amt }}
     OR (
            ref.REF_PRICE IS NOT NULL
        AND TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2)
            > ref.REF_PRICE * {{ var.json.medallion_config.outlier_rules.price_ratio_max }}
        )
     OR (
            ref.REF_PRICE IS NOT NULL AND ref.REF_PRICE > 0
        AND TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2)
            < ref.REF_PRICE * {{ var.json.medallion_config.outlier_rules.price_ratio_min }}
        )
  );

-- 7) Survivors → TMP_SALES_LINE
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE (
    TRANSACTION_ID, LINE_NUM, SALE_DT, LOCN_ID, ITEM_ID, CUSTOMER_ID,
    SOLD_QTY, UNIT_PRICE_AMT, DISCOUNT_AMT, SALES_AMT, TAX_AMT, NET_SALES_AMT,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM
)
SELECT
    TRIM(s.TRANSACTION_ID),
    TRY_TO_NUMBER(TRIM(s.LINE_NUM), 38, 0),
    TRY_TO_DATE(TRIM(s.SALE_DT)),
    TRIM(s.LOCN_ID),
    TRIM(s.ITEM_ID),
    NULLIF(TRIM(s.CUSTOMER_ID), ''),
    TRY_TO_NUMBER(TRIM(s.SOLD_QTY), 18, 3),
    TRY_TO_NUMBER(TRIM(s.UNIT_PRICE_AMT), 18, 2),
    TRY_TO_NUMBER(TRIM(s.DISCOUNT_AMT), 18, 2),
    TRY_TO_NUMBER(TRIM(s.SALES_AMT), 18, 2),
    TRY_TO_NUMBER(TRIM(s.TAX_AMT), 18, 2),
    TRY_TO_NUMBER(TRIM(s.NET_SALES_AMT), 18, 2),
    s._INGEST_DT,
    s._SOURCE_FILE,
    s._SOURCE_ROW_NUM
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
WHERE s._INGEST_DT = '{{ ds }}'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE q
        WHERE q.INGEST_DT = s._INGEST_DT
          AND q.OBJECT_NM = 'TMP_SALES_LINE'
          AND q.SOURCE_FILE = s._SOURCE_FILE
          AND q.RAW_RECORD:"_SOURCE_ROW_NUM"::NUMBER = s._SOURCE_ROW_NUM
  );

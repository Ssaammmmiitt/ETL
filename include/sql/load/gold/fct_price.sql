-- FCT_PRICE periods from inventory prices. Same-day re-run updates in place.

CREATE OR REPLACE TEMPORARY TABLE TMP_PRICE_D AS
SELECT
    i.ITEM_KY,
    l.LOCN_KY,
    t.UNIT_PRICE_AMT,
    t.PROMO_PRICE_AMT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT t
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM i
    ON i.ITEM_ID = t.ITEM_ID AND i.CURRENT_FLG
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l
    ON l.LOCN_ID = t.LOCN_ID AND l.CURRENT_FLG
WHERE t._INGEST_DT = '{{ ds }}';

-- Same-day correction
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE p
SET
    UNIT_PRICE_AMT = s.UNIT_PRICE_AMT,
    PROMO_PRICE_AMT = s.PROMO_PRICE_AMT,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM TMP_PRICE_D s
WHERE p.CURRENT_FLG
  AND p.EFFECTIVE_START_DT = '{{ ds }}'::DATE
  AND p.ITEM_KY = s.ITEM_KY
  AND p.LOCN_KY = s.LOCN_KY
  AND HASH(p.UNIT_PRICE_AMT, COALESCE(p.PROMO_PRICE_AMT, -1))
   <> HASH(s.UNIT_PRICE_AMT, COALESCE(s.PROMO_PRICE_AMT, -1));

-- Close older open periods whose prices changed
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE p
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM TMP_PRICE_D s
WHERE p.CURRENT_FLG
  AND p.EFFECTIVE_START_DT < '{{ ds }}'::DATE
  AND p.ITEM_KY = s.ITEM_KY
  AND p.LOCN_KY = s.LOCN_KY
  AND HASH(p.UNIT_PRICE_AMT, COALESCE(p.PROMO_PRICE_AMT, -1))
   <> HASH(s.UNIT_PRICE_AMT, COALESCE(s.PROMO_PRICE_AMT, -1));

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE (
    PRICE_KY, ITEM_KY, LOCN_KY, UNIT_PRICE_AMT, PROMO_PRICE_AMT,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_PRICE_KY.NEXTVAL,
    s.ITEM_KY,
    s.LOCN_KY,
    s.UNIT_PRICE_AMT,
    s.PROMO_PRICE_AMT,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM TMP_PRICE_D s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE p
    WHERE p.ITEM_KY = s.ITEM_KY
      AND p.LOCN_KY = s.LOCN_KY
      AND p.CURRENT_FLG
);

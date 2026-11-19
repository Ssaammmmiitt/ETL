-- SCD2 on ITEM_NM, BRAND_NM, ITEM_TYPE, SUBCATEGORY parent, ACTIVE_FLG.
-- ITEM_DESC is SCD1 (updated in place on the current row afterward).

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.ITEM_ID,
        t.ITEM_NM,
        t.BRAND_NM,
        t.ITEM_TYPE,
        t.ACTIVE_FLG,
        p.SUBCATEGORY_KY
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY p
        ON p.SUBCATEGORY_ID = t.SUBCATEGORY_ID AND p.CURRENT_FLG
    WHERE t._INGEST_DT = '{{ ds }}'
) s
WHERE d.CURRENT_FLG
  AND d.ITEM_ID = s.ITEM_ID
  AND HASH(
        COALESCE(d.ITEM_NM, '~'),
        COALESCE(d.BRAND_NM, '~'),
        COALESCE(d.ITEM_TYPE, '~'),
        d.SUBCATEGORY_KY,
        CASE WHEN d.ACTIVE_FLG THEN 'Y' ELSE 'N' END
      ) <> HASH(
        COALESCE(s.ITEM_NM, '~'),
        COALESCE(s.BRAND_NM, '~'),
        COALESCE(s.ITEM_TYPE, '~'),
        s.SUBCATEGORY_KY,
        CASE WHEN s.ACTIVE_FLG THEN 'Y' ELSE 'N' END
      );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM (
    ITEM_KY, ITEM_ID, ITEM_NM, ITEM_DESC, BRAND_NM, ITEM_TYPE, SUBCATEGORY_KY, ACTIVE_FLG,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_ITEM_KY.NEXTVAL,
    s.ITEM_ID,
    s.ITEM_NM,
    s.ITEM_DESC,
    s.BRAND_NM,
    s.ITEM_TYPE,
    s.SUBCATEGORY_KY,
    s.ACTIVE_FLG,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.ITEM_ID,
        t.ITEM_NM,
        t.ITEM_DESC,
        t.BRAND_NM,
        t.ITEM_TYPE,
        t.ACTIVE_FLG,
        p.SUBCATEGORY_KY
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY p
        ON p.SUBCATEGORY_ID = t.SUBCATEGORY_ID AND p.CURRENT_FLG
    WHERE t._INGEST_DT = '{{ ds }}'
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM d
    WHERE d.ITEM_ID = s.ITEM_ID
      AND d.CURRENT_FLG
);

-- SCD1: always refresh ITEM_DESC on the current version
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM d
SET
    ITEM_DESC = t.ITEM_DESC,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER t
WHERE d.CURRENT_FLG
  AND d.ITEM_ID = t.ITEM_ID
  AND t._INGEST_DT = '{{ ds }}'
  AND EQUAL_NULL(d.ITEM_DESC, t.ITEM_DESC) = FALSE;

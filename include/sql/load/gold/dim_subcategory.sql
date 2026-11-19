UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.SUBCATEGORY_ID,
        t.SUBCATEGORY_NM,
        t.SUBCATEGORY_DESC,
        p.CATEGORY_KY
    FROM (
        SELECT DISTINCT SUBCATEGORY_ID, SUBCATEGORY_NM, SUBCATEGORY_DESC, CATEGORY_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY p
        ON p.CATEGORY_ID = t.CATEGORY_ID AND p.CURRENT_FLG
) s
WHERE d.CURRENT_FLG
  AND d.SUBCATEGORY_ID = s.SUBCATEGORY_ID
  AND HASH(
        COALESCE(d.SUBCATEGORY_NM, '~'),
        COALESCE(d.SUBCATEGORY_DESC, '~'),
        d.CATEGORY_KY
      ) <> HASH(
        COALESCE(s.SUBCATEGORY_NM, '~'),
        COALESCE(s.SUBCATEGORY_DESC, '~'),
        s.CATEGORY_KY
      );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY (
    SUBCATEGORY_KY, SUBCATEGORY_ID, SUBCATEGORY_NM, SUBCATEGORY_DESC, CATEGORY_KY,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_SUBCATEGORY_KY.NEXTVAL,
    s.SUBCATEGORY_ID,
    s.SUBCATEGORY_NM,
    s.SUBCATEGORY_DESC,
    s.CATEGORY_KY,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.SUBCATEGORY_ID,
        t.SUBCATEGORY_NM,
        t.SUBCATEGORY_DESC,
        p.CATEGORY_KY
    FROM (
        SELECT DISTINCT SUBCATEGORY_ID, SUBCATEGORY_NM, SUBCATEGORY_DESC, CATEGORY_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY p
        ON p.CATEGORY_ID = t.CATEGORY_ID AND p.CURRENT_FLG
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY d
    WHERE d.SUBCATEGORY_ID = s.SUBCATEGORY_ID
      AND d.CURRENT_FLG
);

-- Close changed categories (attrs or parent department)
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.CATEGORY_ID,
        t.CATEGORY_NM,
        t.CATEGORY_DESC,
        p.DEPARTMENT_KY
    FROM (
        SELECT DISTINCT CATEGORY_ID, CATEGORY_NM, CATEGORY_DESC, DEPARTMENT_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT p
        ON p.DEPARTMENT_ID = t.DEPARTMENT_ID AND p.CURRENT_FLG
) s
WHERE d.CURRENT_FLG
  AND d.CATEGORY_ID = s.CATEGORY_ID
  AND HASH(
        COALESCE(d.CATEGORY_NM, '~'),
        COALESCE(d.CATEGORY_DESC, '~'),
        d.DEPARTMENT_KY
      ) <> HASH(
        COALESCE(s.CATEGORY_NM, '~'),
        COALESCE(s.CATEGORY_DESC, '~'),
        s.DEPARTMENT_KY
      );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY (
    CATEGORY_KY, CATEGORY_ID, CATEGORY_NM, CATEGORY_DESC, DEPARTMENT_KY,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CATEGORY_KY.NEXTVAL,
    s.CATEGORY_ID,
    s.CATEGORY_NM,
    s.CATEGORY_DESC,
    s.DEPARTMENT_KY,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.CATEGORY_ID,
        t.CATEGORY_NM,
        t.CATEGORY_DESC,
        p.DEPARTMENT_KY
    FROM (
        SELECT DISTINCT CATEGORY_ID, CATEGORY_NM, CATEGORY_DESC, DEPARTMENT_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT p
        ON p.DEPARTMENT_ID = t.DEPARTMENT_ID AND p.CURRENT_FLG
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY d
    WHERE d.CATEGORY_ID = s.CATEGORY_ID
      AND d.CURRENT_FLG
);

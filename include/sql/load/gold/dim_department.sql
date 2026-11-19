-- SCD2 DIM_DEPARTMENT from today's TMP_ITEM_HIER (distinct departments).

-- Close current rows whose tracked attrs changed
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT DISTINCT
        DEPARTMENT_ID,
        DEPARTMENT_NM,
        DEPARTMENT_DESC,
        HASH(COALESCE(DEPARTMENT_NM, '~'), COALESCE(DEPARTMENT_DESC, '~')) AS ATTR_HASH
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
    WHERE _INGEST_DT = '{{ ds }}'
) s
WHERE d.CURRENT_FLG
  AND d.DEPARTMENT_ID = s.DEPARTMENT_ID
  AND HASH(COALESCE(d.DEPARTMENT_NM, '~'), COALESCE(d.DEPARTMENT_DESC, '~')) <> s.ATTR_HASH;

-- Insert new keys + changed (no current row left)
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT (
    DEPARTMENT_KY, DEPARTMENT_ID, DEPARTMENT_NM, DEPARTMENT_DESC,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_DEPARTMENT_KY.NEXTVAL,
    s.DEPARTMENT_ID,
    s.DEPARTMENT_NM,
    s.DEPARTMENT_DESC,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT DISTINCT DEPARTMENT_ID, DEPARTMENT_NM, DEPARTMENT_DESC
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER
    WHERE _INGEST_DT = '{{ ds }}'
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT d
    WHERE d.DEPARTMENT_ID = s.DEPARTMENT_ID
      AND d.CURRENT_FLG
);

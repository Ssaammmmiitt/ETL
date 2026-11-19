UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.REGION_ID,
        t.REGION_NM,
        p.DIVISION_KY
    FROM (
        SELECT DISTINCT REGION_ID, REGION_NM, DIVISION_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION p
        ON p.DIVISION_ID = t.DIVISION_ID AND p.CURRENT_FLG
) s
WHERE d.CURRENT_FLG
  AND d.REGION_ID = s.REGION_ID
  AND HASH(COALESCE(d.REGION_NM, '~'), d.DIVISION_KY)
    <> HASH(COALESCE(s.REGION_NM, '~'), s.DIVISION_KY);

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION (
    REGION_KY, REGION_ID, REGION_NM, DIVISION_KY,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_REGION_KY.NEXTVAL,
    s.REGION_ID,
    s.REGION_NM,
    s.DIVISION_KY,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.REGION_ID,
        t.REGION_NM,
        p.DIVISION_KY
    FROM (
        SELECT DISTINCT REGION_ID, REGION_NM, DIVISION_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION p
        ON p.DIVISION_ID = t.DIVISION_ID AND p.CURRENT_FLG
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION d
    WHERE d.REGION_ID = s.REGION_ID AND d.CURRENT_FLG
);

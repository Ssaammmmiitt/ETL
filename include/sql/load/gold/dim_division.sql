UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT DISTINCT DIVISION_ID, DIVISION_NM
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
    WHERE _INGEST_DT = '{{ ds }}'
) s
WHERE d.CURRENT_FLG
  AND d.DIVISION_ID = s.DIVISION_ID
  AND HASH(COALESCE(d.DIVISION_NM, '~')) <> HASH(COALESCE(s.DIVISION_NM, '~'));

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION (
    DIVISION_KY, DIVISION_ID, DIVISION_NM,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_DIVISION_KY.NEXTVAL,
    s.DIVISION_ID,
    s.DIVISION_NM,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT DISTINCT DIVISION_ID, DIVISION_NM
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
    WHERE _INGEST_DT = '{{ ds }}'
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION d
    WHERE d.DIVISION_ID = s.DIVISION_ID AND d.CURRENT_FLG
);

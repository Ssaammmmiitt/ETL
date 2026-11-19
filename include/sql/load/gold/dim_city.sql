UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.CITY_ID,
        t.CITY_NM,
        t.STATE_PROVINCE_NM,
        t.COUNTRY_NM,
        p.REGION_KY
    FROM (
        SELECT DISTINCT CITY_ID, CITY_NM, STATE_PROVINCE_NM, COUNTRY_NM, REGION_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION p
        ON p.REGION_ID = t.REGION_ID AND p.CURRENT_FLG
) s
WHERE d.CURRENT_FLG
  AND d.CITY_ID = s.CITY_ID
  AND HASH(
        COALESCE(d.CITY_NM, '~'),
        COALESCE(d.STATE_PROVINCE_NM, '~'),
        COALESCE(d.COUNTRY_NM, '~'),
        d.REGION_KY
      ) <> HASH(
        COALESCE(s.CITY_NM, '~'),
        COALESCE(s.STATE_PROVINCE_NM, '~'),
        COALESCE(s.COUNTRY_NM, '~'),
        s.REGION_KY
      );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY (
    CITY_KY, CITY_ID, CITY_NM, STATE_PROVINCE_NM, COUNTRY_NM, REGION_KY,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CITY_KY.NEXTVAL,
    s.CITY_ID,
    s.CITY_NM,
    s.STATE_PROVINCE_NM,
    s.COUNTRY_NM,
    s.REGION_KY,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.CITY_ID,
        t.CITY_NM,
        t.STATE_PROVINCE_NM,
        t.COUNTRY_NM,
        p.REGION_KY
    FROM (
        SELECT DISTINCT CITY_ID, CITY_NM, STATE_PROVINCE_NM, COUNTRY_NM, REGION_ID
        FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER
        WHERE _INGEST_DT = '{{ ds }}'
    ) t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION p
        ON p.REGION_ID = t.REGION_ID AND p.CURRENT_FLG
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY d
    WHERE d.CITY_ID = s.CITY_ID AND d.CURRENT_FLG
);

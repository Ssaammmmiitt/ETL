-- SCD2 DIM_LOCN — D5 changes CITY_ID (parent) and opens a new version.

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN d
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.LOCN_ID,
        t.LOCN_NM,
        t.LOCN_TYPE,
        t.ADDRESS_TXT,
        t.POSTAL_CD,
        t.ACTIVE_FLG,
        p.CITY_KY
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY p
        ON p.CITY_ID = t.CITY_ID AND p.CURRENT_FLG
    WHERE t._INGEST_DT = '{{ ds }}'
) s
WHERE d.CURRENT_FLG
  AND d.LOCN_ID = s.LOCN_ID
  AND HASH(
        COALESCE(d.LOCN_NM, '~'),
        COALESCE(d.LOCN_TYPE, '~'),
        COALESCE(d.ADDRESS_TXT, '~'),
        COALESCE(d.POSTAL_CD, '~'),
        d.CITY_KY,
        CASE WHEN d.ACTIVE_FLG THEN 'Y' ELSE 'N' END
      ) <> HASH(
        COALESCE(s.LOCN_NM, '~'),
        COALESCE(s.LOCN_TYPE, '~'),
        COALESCE(s.ADDRESS_TXT, '~'),
        COALESCE(s.POSTAL_CD, '~'),
        s.CITY_KY,
        CASE WHEN s.ACTIVE_FLG THEN 'Y' ELSE 'N' END
      );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN (
    LOCN_KY, LOCN_ID, LOCN_NM, LOCN_TYPE, ADDRESS_TXT, POSTAL_CD, CITY_KY, ACTIVE_FLG,
    EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_LOCN_KY.NEXTVAL,
    s.LOCN_ID,
    s.LOCN_NM,
    s.LOCN_TYPE,
    s.ADDRESS_TXT,
    s.POSTAL_CD,
    s.CITY_KY,
    s.ACTIVE_FLG,
    '{{ ds }}'::DATE,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM (
    SELECT
        t.LOCN_ID,
        t.LOCN_NM,
        t.LOCN_TYPE,
        t.ADDRESS_TXT,
        t.POSTAL_CD,
        t.ACTIVE_FLG,
        p.CITY_KY
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER t
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY p
        ON p.CITY_ID = t.CITY_ID AND p.CURRENT_FLG
    WHERE t._INGEST_DT = '{{ ds }}'
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN d
    WHERE d.LOCN_ID = s.LOCN_ID AND d.CURRENT_FLG
);

-- G11: refresh recon for every day present in STG_SALES (expect 5 pipeline days).
-- variance = STG net − quarantined net − FCT net  (must be 0).

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_NET_SALES
 WHERE DAY_KY IN (
    SELECT DISTINCT TO_NUMBER(TO_CHAR(_INGEST_DT, 'YYYYMMDD'))
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
 );

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_NET_SALES (
    DAY_KY, SOURCE_NET_AMT, QUARANTINE_NET_AMT, FCT_NET_AMT, VARIANCE_AMT, RECON_TS
)
WITH days AS (
    SELECT DISTINCT _INGEST_DT AS INGEST_DT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
),
stg AS (
    SELECT
        _INGEST_DT AS INGEST_DT,
        COALESCE(SUM(TRY_TO_NUMBER(TRIM(NET_SALES_AMT), 18, 2)), 0) AS SOURCE_NET_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
    GROUP BY 1
),
quar AS (
    SELECT
        INGEST_DT,
        COALESCE(SUM(TRY_TO_NUMBER(RAW_RECORD:NET_SALES_AMT::VARCHAR, 18, 2)), 0) AS QUARANTINE_NET_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
    WHERE OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES')
    GROUP BY 1
),
fct AS (
    SELECT
        TO_DATE(TO_VARCHAR(DAY_KY), 'YYYYMMDD') AS INGEST_DT,
        COALESCE(SUM(NET_SALES_AMT), 0) AS FCT_NET_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
    GROUP BY 1
)
SELECT
    TO_NUMBER(TO_CHAR(d.INGEST_DT, 'YYYYMMDD')),
    COALESCE(s.SOURCE_NET_AMT, 0),
    COALESCE(q.QUARANTINE_NET_AMT, 0),
    COALESCE(f.FCT_NET_AMT, 0),
    COALESCE(s.SOURCE_NET_AMT, 0)
      - COALESCE(q.QUARANTINE_NET_AMT, 0)
      - COALESCE(f.FCT_NET_AMT, 0),
    CURRENT_TIMESTAMP()
FROM days d
LEFT JOIN stg s ON s.INGEST_DT = d.INGEST_DT
LEFT JOIN quar q ON q.INGEST_DT = d.INGEST_DT
LEFT JOIN fct f ON f.INGEST_DT = d.INGEST_DT;

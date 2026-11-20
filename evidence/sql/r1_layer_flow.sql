-- One row per loaded day: which run loaded it, rows through every layer, and the money tie-out.
-- Expect ROW_DIFF = 0 (source = Gold + quarantine), G11_VARIANCE = 0 and G9_DELTA = 0.
WITH days AS (
    SELECT CALENDAR_DT AS DT, DAY_KY
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DAY
    WHERE CALENDAR_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
),
stg AS (
    SELECT _INGEST_DT AS DT, MAX(_BATCH_ID) AS RUN_ID, COUNT(*) AS BRONZE_ROWS,
           COALESCE(SUM(TRY_TO_NUMBER(TRIM(NET_SALES_AMT), 18, 2)), 0) AS SOURCE_NET
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
    WHERE _INGEST_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
    GROUP BY 1
),
tmp AS (
    SELECT _INGEST_DT AS DT, COUNT(*) AS SILVER_ROWS
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE
    WHERE _INGEST_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
    GROUP BY 1
),
quar AS (
    SELECT INGEST_DT AS DT,
           COUNT_IF(OBJECT_NM = 'TMP_SALES_LINE') AS QUAR_SILVER,
           COUNT_IF(OBJECT_NM = 'FCT_SALES') AS QUAR_G7,
           COALESCE(SUM(IFF(OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES'),
                            TRY_TO_NUMBER(RAW_RECORD:NET_SALES_AMT::VARCHAR, 18, 2), 0)), 0) AS QUAR_NET
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
    WHERE INGEST_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
    GROUP BY 1
),
fct AS (
    SELECT DAY_KY, COUNT(*) AS GOLD_ROWS, SUM(NET_SALES_AMT) AS GOLD_NET
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
    GROUP BY 1
),
agg AS (
    SELECT DAY_KY, SUM(NET_SALES_AMT) AS AGG_NET
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
    GROUP BY 1
)
SELECT
    d.DT,
    s.RUN_ID,
    s.BRONZE_ROWS,
    COALESCE(t.SILVER_ROWS, 0) AS SILVER_ROWS,
    COALESCE(q.QUAR_SILVER, 0) AS QUAR_SILVER,
    COALESCE(q.QUAR_G7, 0) AS QUAR_G7,
    COALESCE(f.GOLD_ROWS, 0) AS GOLD_ROWS,
    s.BRONZE_ROWS - COALESCE(f.GOLD_ROWS, 0) - COALESCE(q.QUAR_SILVER, 0) - COALESCE(q.QUAR_G7, 0) AS ROW_DIFF,
    s.SOURCE_NET AS SOURCE_NET_AMT,
    COALESCE(f.GOLD_NET, 0) AS GOLD_NET_AMT,
    COALESCE(q.QUAR_NET, 0) AS QUARANTINE_NET_AMT,
    s.SOURCE_NET - COALESCE(f.GOLD_NET, 0) - COALESCE(q.QUAR_NET, 0) AS G11_VARIANCE,
    COALESCE(a.AGG_NET, 0) - COALESCE(f.GOLD_NET, 0) AS G9_DELTA
FROM days d
JOIN stg s ON s.DT = d.DT
LEFT JOIN tmp t ON t.DT = d.DT
LEFT JOIN quar q ON q.DT = d.DT
LEFT JOIN fct f ON f.DAY_KY = d.DAY_KY
LEFT JOIN agg a ON a.DAY_KY = d.DAY_KY
ORDER BY 1;

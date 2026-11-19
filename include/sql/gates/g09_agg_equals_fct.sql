-- Gate G9: AGG totals for day D must equal FCT_SALES totals (any row = fail).
SELECT
    'NET' AS METRIC,
    COALESCE(a.NET_SALES_AMT, 0) AS AGG_AMT,
    COALESCE(f.NET_SALES_AMT, 0) AS FCT_AMT,
    COALESCE(a.NET_SALES_AMT, 0) - COALESCE(f.NET_SALES_AMT, 0) AS DELTA
FROM (
    SELECT SUM(NET_SALES_AMT) AS NET_SALES_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
    WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
) a
CROSS JOIN (
    SELECT SUM(NET_SALES_AMT) AS NET_SALES_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
    WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
) f
WHERE ABS(COALESCE(a.NET_SALES_AMT, 0) - COALESCE(f.NET_SALES_AMT, 0)) > 0.01

UNION ALL

SELECT
    'LINES',
    COALESCE(a.CNT, 0),
    COALESCE(f.CNT, 0),
    COALESCE(a.CNT, 0) - COALESCE(f.CNT, 0)
FROM (
    SELECT SUM(SALES_LINE_CNT) AS CNT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
    WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
) a
CROSS JOIN (
    SELECT COUNT(*) AS CNT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
    WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
) f
WHERE COALESCE(a.CNT, 0) <> COALESCE(f.CNT, 0);

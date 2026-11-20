-- The reporting view (VW_DAILY_ITEM_LOCN_SALES) rolled up the location hierarchy to region.
SELECT
    rg.REGION_NM,
    COUNT(DISTINCT v.DAY_KY) AS DAYS,
    SUM(v.SALES_LINE_CNT) AS LINES,
    SUM(v.SOLD_QTY) AS SOLD_QTY,
    SUM(v.NET_SALES_AMT) AS NET_SALES_AMT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.VW_DAILY_ITEM_LOCN_SALES v
JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l ON l.LOCN_KY = v.LOCN_KY
JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY c ON c.CITY_KY = l.CITY_KY
JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION rg ON rg.REGION_KY = c.REGION_KY
WHERE v.DAY_KY BETWEEN TO_NUMBER(TO_CHAR('{{ params.from_ds }}'::DATE, 'YYYYMMDD'))
                   AND TO_NUMBER(TO_CHAR('{{ params.to_ds }}'::DATE, 'YYYYMMDD'))
GROUP BY 1
ORDER BY 5 DESC;

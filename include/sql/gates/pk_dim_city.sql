SELECT 'CURRENT_DUP' AS ISSUE, CITY_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.CITY_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY b
    ON a.CITY_ID = b.CITY_ID AND a.CITY_KY < b.CITY_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

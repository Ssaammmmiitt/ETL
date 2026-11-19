SELECT 'CURRENT_DUP' AS ISSUE, SUBCATEGORY_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.SUBCATEGORY_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY b
    ON a.SUBCATEGORY_ID = b.SUBCATEGORY_ID AND a.SUBCATEGORY_KY < b.SUBCATEGORY_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

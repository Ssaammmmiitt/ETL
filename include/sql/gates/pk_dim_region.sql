SELECT 'CURRENT_DUP' AS ISSUE, REGION_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.REGION_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION b
    ON a.REGION_ID = b.REGION_ID AND a.REGION_KY < b.REGION_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

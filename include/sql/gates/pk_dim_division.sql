SELECT 'CURRENT_DUP' AS ISSUE, DIVISION_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.DIVISION_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION b
    ON a.DIVISION_ID = b.DIVISION_ID AND a.DIVISION_KY < b.DIVISION_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

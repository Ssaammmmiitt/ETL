SELECT 'CURRENT_DUP' AS ISSUE, LOCN_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.LOCN_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN b
    ON a.LOCN_ID = b.LOCN_ID AND a.LOCN_KY < b.LOCN_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

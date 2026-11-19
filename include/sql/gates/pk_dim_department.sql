-- SCD2: one current row per natural key; no overlapping effective ranges.
SELECT 'CURRENT_DUP' AS ISSUE, DEPARTMENT_ID AS NAT_KEY, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT
WHERE CURRENT_FLG
GROUP BY 1, 2
HAVING COUNT(*) > 1
UNION ALL
SELECT 'OVERLAP', a.DEPARTMENT_ID, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT b
    ON a.DEPARTMENT_ID = b.DEPARTMENT_ID AND a.DEPARTMENT_KY < b.DEPARTMENT_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
GROUP BY 1, 2;

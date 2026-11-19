-- PK checker: one CURRENT_FLG row per (ITEM_KY, LOCN_KY); version key unique.
SELECT 'CURRENT_DUP' AS ISSUE, ITEM_KY::VARCHAR AS K1, LOCN_KY::VARCHAR AS K2, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
WHERE CURRENT_FLG
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
UNION ALL
SELECT 'VERSION_DUP', ITEM_KY::VARCHAR, LOCN_KY::VARCHAR || '|' || SNAPSHOT_DT::VARCHAR, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
GROUP BY ITEM_KY, LOCN_KY, SNAPSHOT_DT
HAVING COUNT(*) > 1;

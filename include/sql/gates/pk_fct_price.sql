-- PK checker: one CURRENT_FLG row per (ITEM_KY, LOCN_KY); period start unique.
-- Overlaps are gate G10 (separate).
SELECT 'CURRENT_DUP' AS ISSUE, ITEM_KY::VARCHAR AS K1, LOCN_KY::VARCHAR AS K2, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
WHERE CURRENT_FLG
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
UNION ALL
SELECT 'START_DUP', ITEM_KY::VARCHAR, LOCN_KY::VARCHAR || '|' || EFFECTIVE_START_DT::VARCHAR, COUNT(*)
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
GROUP BY ITEM_KY, LOCN_KY, EFFECTIVE_START_DT
HAVING COUNT(*) > 1;

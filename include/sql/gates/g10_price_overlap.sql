-- Gate G10: overlapping FCT_PRICE periods for the same (ITEM_KY, LOCN_KY) → fail.
-- Any row returned = failure (DAG Python task asserts empty).

SELECT
    a.ITEM_KY,
    a.LOCN_KY,
    a.PRICE_KY AS PRICE_KY_A,
    b.PRICE_KY AS PRICE_KY_B,
    a.EFFECTIVE_START_DT AS START_A,
    a.EFFECTIVE_END_DT AS END_A,
    b.EFFECTIVE_START_DT AS START_B,
    b.EFFECTIVE_END_DT AS END_B
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE a
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE b
    ON a.ITEM_KY = b.ITEM_KY
   AND a.LOCN_KY = b.LOCN_KY
   AND a.PRICE_KY < b.PRICE_KY
   AND a.EFFECTIVE_START_DT <= b.EFFECTIVE_END_DT
   AND b.EFFECTIVE_START_DT <= a.EFFECTIVE_END_DT
ORDER BY a.ITEM_KY, a.LOCN_KY
LIMIT 100;

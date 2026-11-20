-- SCD2: the moving store keeps history (old city closed, new city current).
-- SCD1: the revised item keeps one row; its description is overwritten in place.
SELECT
    'DIM_LOCN (SCD2)' AS DIM,
    l.LOCN_ID AS NATURAL_ID,
    c.CITY_NM AS TRACKED_VALUE,
    l.EFFECTIVE_START_DT,
    l.EFFECTIVE_END_DT,
    l.CURRENT_FLG
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l
JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY c
  ON c.CITY_KY = l.CITY_KY
WHERE l.LOCN_ID = '{{ params.scd2_locn_id }}'
UNION ALL
SELECT
    'DIM_ITEM (SCD1)',
    ITEM_ID,
    ITEM_DESC,
    EFFECTIVE_START_DT,
    EFFECTIVE_END_DT,
    CURRENT_FLG
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM
WHERE ITEM_ID = '{{ params.scd1_item_id }}'
ORDER BY 1, 4;

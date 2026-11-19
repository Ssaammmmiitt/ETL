-- Live rollup view (same grain as AGG). Reports can use AGG for speed; view always matches FCT.
CREATE OR REPLACE VIEW {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.VW_DAILY_ITEM_LOCN_SALES AS
SELECT
    DAY_KY,
    ITEM_KY,
    LOCN_KY,
    COUNT(*) AS SALES_LINE_CNT,
    SUM(SOLD_QTY) AS SOLD_QTY,
    SUM(SALES_AMT) AS SALES_AMT,
    SUM(DISCOUNT_AMT) AS DISCOUNT_AMT,
    SUM(TAX_AMT) AS TAX_AMT,
    SUM(NET_SALES_AMT) AS NET_SALES_AMT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
GROUP BY DAY_KY, ITEM_KY, LOCN_KY;

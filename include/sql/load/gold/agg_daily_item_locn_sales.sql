-- Recompute AGG for day D only (partition prune on DAY_KY).
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
 WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'));

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES (
    DAY_KY, ITEM_KY, LOCN_KY,
    SALES_LINE_CNT, SOLD_QTY, SALES_AMT, DISCOUNT_AMT, TAX_AMT, NET_SALES_AMT,
    INSERT_TS, UPDATE_TS
)
SELECT
    DAY_KY,
    ITEM_KY,
    LOCN_KY,
    COUNT(*),
    SUM(SOLD_QTY),
    SUM(SALES_AMT),
    SUM(DISCOUNT_AMT),
    SUM(TAX_AMT),
    SUM(NET_SALES_AMT),
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
GROUP BY DAY_KY, ITEM_KY, LOCN_KY;

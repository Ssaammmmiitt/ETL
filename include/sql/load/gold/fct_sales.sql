-- FCT_SALES: MERGE survivors of G7 (keys resolve to current DIM_ITEM / DIM_LOCN).
-- DAY_KY = SALE_DT as YYYYMMDD. CUSTOMER_KY nullable (non-member lines).

MERGE INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES t
USING (
    SELECT
        s.TRANSACTION_ID,
        s.LINE_NUM,
        TO_NUMBER(TO_CHAR(s.SALE_DT, 'YYYYMMDD')) AS DAY_KY,
        s.SALE_DT,
        i.ITEM_KY,
        l.LOCN_KY,
        c.CUSTOMER_KY,
        s.SOLD_QTY,
        s.UNIT_PRICE_AMT,
        s.DISCOUNT_AMT,
        s.SALES_AMT,
        s.TAX_AMT,
        s.NET_SALES_AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE s
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM i
        ON i.ITEM_ID = s.ITEM_ID AND i.CURRENT_FLG
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l
        ON l.LOCN_ID = s.LOCN_ID AND l.CURRENT_FLG
    LEFT JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CUSTOMER c
        ON c.CUSTOMER_ID = s.CUSTOMER_ID
    WHERE s._INGEST_DT = '{{ ds }}'
) src
ON t.TRANSACTION_ID = src.TRANSACTION_ID
 AND t.LINE_NUM = src.LINE_NUM
WHEN MATCHED THEN UPDATE SET
    DAY_KY = src.DAY_KY,
    SALE_DT = src.SALE_DT,
    ITEM_KY = src.ITEM_KY,
    LOCN_KY = src.LOCN_KY,
    CUSTOMER_KY = src.CUSTOMER_KY,
    SOLD_QTY = src.SOLD_QTY,
    UNIT_PRICE_AMT = src.UNIT_PRICE_AMT,
    DISCOUNT_AMT = src.DISCOUNT_AMT,
    SALES_AMT = src.SALES_AMT,
    TAX_AMT = src.TAX_AMT,
    NET_SALES_AMT = src.NET_SALES_AMT,
    UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    SALES_LINE_KY, TRANSACTION_ID, LINE_NUM, DAY_KY, SALE_DT, ITEM_KY, LOCN_KY, CUSTOMER_KY,
    SOLD_QTY, UNIT_PRICE_AMT, DISCOUNT_AMT, SALES_AMT, TAX_AMT, NET_SALES_AMT,
    INSERT_TS, UPDATE_TS
) VALUES (
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_SALES_LINE_KY.NEXTVAL,
    src.TRANSACTION_ID, src.LINE_NUM, src.DAY_KY, src.SALE_DT, src.ITEM_KY, src.LOCN_KY, src.CUSTOMER_KY,
    src.SOLD_QTY, src.UNIT_PRICE_AMT, src.DISCOUNT_AMT, src.SALES_AMT, src.TAX_AMT, src.NET_SALES_AMT,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

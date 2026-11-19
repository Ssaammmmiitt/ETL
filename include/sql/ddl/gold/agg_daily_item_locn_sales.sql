-- AGG_DAILY_ITEM_LOCN_SALES: rollup of FCT_SALES by (DAY_KY, ITEM_KY, LOCN_KY).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES (
    DAY_KY           NUMBER(8,0)   NOT NULL,
    ITEM_KY          NUMBER(38,0)  NOT NULL,
    LOCN_KY          NUMBER(38,0)  NOT NULL,
    SALES_LINE_CNT   NUMBER(38,0)  NOT NULL,
    SOLD_QTY         NUMBER(18,3)  NOT NULL,
    SALES_AMT        NUMBER(18,2)  NOT NULL,
    DISCOUNT_AMT     NUMBER(18,2)  NOT NULL,
    TAX_AMT          NUMBER(18,2)  NOT NULL,
    NET_SALES_AMT    NUMBER(18,2)  NOT NULL,
    INSERT_TS        TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS        TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_AGG_DAILY_ITEM_LOCN_SALES PRIMARY KEY (DAY_KY, ITEM_KY, LOCN_KY)
)
CLUSTER BY (DAY_KY)
COMMENT = 'Gold Aggregate — daily item x location sales rollup';

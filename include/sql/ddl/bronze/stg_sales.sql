-- STG_SALES (Bronze): sales_YYYYMMDD.csv as landed. Daily delta, one POS line per row.
-- Feeds TMP_SALES_LINE and TMP_CUSTOMER, then FCT_SALES and DIM_CUSTOMER.
-- Badge on the diagram: CSV | _INGEST_DT | delta.
CREATE OR REPLACE TABLE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES (
    TRANSACTION_ID   VARCHAR,
    LINE_NUM         VARCHAR,
    SALE_DT          VARCHAR,
    LOCN_ID          VARCHAR,
    ITEM_ID          VARCHAR,
    CUSTOMER_ID      VARCHAR,
    CUSTOMER_NM      VARCHAR,
    CUSTOMER_TYPE    VARCHAR,
    EMAIL_TXT        VARCHAR,
    PHONE_NUM        VARCHAR,
    SOLD_QTY         VARCHAR,
    UNIT_PRICE_AMT   VARCHAR,
    DISCOUNT_AMT     VARCHAR,
    SALES_AMT        VARCHAR,
    TAX_AMT          VARCHAR,
    NET_SALES_AMT    VARCHAR,
    _INGEST_DT       DATE          NOT NULL,
    _SOURCE_FILE     VARCHAR       NOT NULL,
    _SOURCE_ROW_NUM  NUMBER        NOT NULL,
    _INGEST_TS       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _BATCH_ID        VARCHAR       NOT NULL
)
COMMENT = 'STG (Bronze): raw POS lines + member attributes, no transformation';

-- TMP_SALES_LINE (Silver): typed, cleaned POS lines for day D. Delta by _INGEST_DT.
-- Feeds FCT_SALES. Bad rows go to AUDIT.C_REJ_TABLE (G3–G6, G12), not here.
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE (
    TRANSACTION_ID   VARCHAR(50)   NOT NULL,
    LINE_NUM         NUMBER(38,0)  NOT NULL,
    SALE_DT          DATE          NOT NULL,
    LOCN_ID          VARCHAR(50)   NOT NULL,
    ITEM_ID          VARCHAR(50)   NOT NULL,
    CUSTOMER_ID      VARCHAR(50),
    SOLD_QTY         NUMBER(18,3)  NOT NULL,
    UNIT_PRICE_AMT   NUMBER(18,2)  NOT NULL,
    DISCOUNT_AMT     NUMBER(18,2)  NOT NULL,
    SALES_AMT        NUMBER(18,2)  NOT NULL,
    TAX_AMT          NUMBER(18,2)  NOT NULL,
    NET_SALES_AMT    NUMBER(18,2)  NOT NULL,
    _INGEST_DT       DATE          NOT NULL,
    _SOURCE_FILE     VARCHAR(200)  NOT NULL,
    _SOURCE_ROW_NUM  NUMBER(38,0)  NOT NULL
)
COMMENT = 'TMP (Silver): cleaned sales lines — typed, deduped, gated';

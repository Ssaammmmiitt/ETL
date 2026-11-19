-- TMP_CUSTOMER (Silver): members seen on sales. SCD1 candidate for DIM_CUSTOMER.
-- LAST_SEEN_DT = most recent SALE_DT we loaded for that customer.
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_CUSTOMER (
    CUSTOMER_ID      VARCHAR(50)   NOT NULL,
    CUSTOMER_NM      VARCHAR(200),
    CUSTOMER_TYPE    VARCHAR(50),
    EMAIL_TXT        VARCHAR(254),
    PHONE_NUM        VARCHAR(30),
    LAST_SEEN_DT     DATE          NOT NULL,
    _INGEST_DT       DATE          NOT NULL
)
COMMENT = 'TMP (Silver): distinct members from cleaned sales; LAST_SEEN_DT updated each load';

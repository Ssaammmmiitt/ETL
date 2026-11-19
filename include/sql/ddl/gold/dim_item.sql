-- ITEM_DESC is SCD1 (in-place); other tracked attrs are SCD2 (departure 2).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM (
    ITEM_KY             NUMBER(38,0)  NOT NULL,
    ITEM_ID             VARCHAR(50)   NOT NULL,
    ITEM_NM             VARCHAR(200),
    ITEM_DESC           VARCHAR(1000),
    BRAND_NM            VARCHAR(100),
    ITEM_TYPE           VARCHAR(50),
    SUBCATEGORY_KY      NUMBER(38,0)  NOT NULL,
    ACTIVE_FLG          BOOLEAN       NOT NULL,
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_DIM_ITEM PRIMARY KEY (ITEM_KY)
)
COMMENT = 'Product hierarchy leaf — SCD2 + ITEM_DESC SCD1';

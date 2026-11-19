-- FCT_PRICE (Gold): price periods. Surrogate PK; business key (ITEM_KY, LOCN_KY, EFFECTIVE_START_DT).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE (
    PRICE_KY            NUMBER(38,0)  NOT NULL,
    ITEM_KY             NUMBER(38,0)  NOT NULL,
    LOCN_KY             NUMBER(38,0)  NOT NULL,
    UNIT_PRICE_AMT      NUMBER(18,2)  NOT NULL,
    PROMO_PRICE_AMT     NUMBER(18,2),
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_FCT_PRICE PRIMARY KEY (PRICE_KY),
    CONSTRAINT UQ_FCT_PRICE_PERIOD UNIQUE (ITEM_KY, LOCN_KY, EFFECTIVE_START_DT)
)
COMMENT = 'Price periods — close on UNIT/PROMO change; G10 checks no overlap (L10)';

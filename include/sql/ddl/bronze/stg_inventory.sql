-- STG_INVENTORY (Bronze): inventory_YYYYMMDD.csv as landed. Daily full snapshot, NO header.
-- Feeds TMP_ITEM_LOC_SNAPSHOT, then FCT_INVENTORY and FCT_PRICE.
-- Badge on the diagram: CSV | _INGEST_DT | full.
-- The business columns are in FILE ORDER: the positional COPY maps $1..$8 onto them.
CREATE OR REPLACE TABLE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY (
    SNAPSHOT_DT      VARCHAR,
    ITEM_ID          VARCHAR,
    LOCN_ID          VARCHAR,
    ON_HAND_QTY      VARCHAR,
    RESERVED_QTY     VARCHAR,
    IN_TRANSIT_QTY   VARCHAR,
    UNIT_PRICE_AMT   VARCHAR,
    PROMO_PRICE_AMT  VARCHAR,
    _INGEST_DT       DATE          NOT NULL,
    _SOURCE_FILE     VARCHAR       NOT NULL,
    _SOURCE_ROW_NUM  NUMBER        NOT NULL,
    _INGEST_TS       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _BATCH_ID        VARCHAR       NOT NULL
)
COMMENT = 'STG (Bronze): raw item x location stock and prices, positional file';

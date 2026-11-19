-- TMP_ITEM_LOC_SNAPSHOT (Silver): typed inventory snapshot + AVAILABLE_QTY.
-- Feeds FCT_INVENTORY and FCT_PRICE. Partition / reload by SNAPSHOT_DT (= day D).
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT (
    SNAPSHOT_DT      DATE          NOT NULL,
    ITEM_ID          VARCHAR(50)   NOT NULL,
    LOCN_ID          VARCHAR(50)   NOT NULL,
    ON_HAND_QTY      NUMBER(18,3)  NOT NULL,
    RESERVED_QTY     NUMBER(18,3)  NOT NULL,
    IN_TRANSIT_QTY   NUMBER(18,3)  NOT NULL,
    AVAILABLE_QTY    NUMBER(18,3)  NOT NULL,
    UNIT_PRICE_AMT   NUMBER(18,2)  NOT NULL,
    PROMO_PRICE_AMT  NUMBER(18,2),
    _INGEST_DT       DATE          NOT NULL,
    _SOURCE_FILE     VARCHAR(200)  NOT NULL,
    _SOURCE_ROW_NUM  NUMBER(38,0)  NOT NULL
)
COMMENT = 'TMP (Silver): cleaned item x location snapshot; AVAILABLE = ON_HAND - RESERVED';

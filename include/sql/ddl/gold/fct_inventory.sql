-- FCT_INVENTORY (Gold): soft-close (departure 3). Surrogate PK; business key (ITEM_KY, LOCN_KY, SNAPSHOT_DT).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY (
    INVENTORY_KY        NUMBER(38,0)  NOT NULL,
    ITEM_KY             NUMBER(38,0)  NOT NULL,
    LOCN_KY             NUMBER(38,0)  NOT NULL,
    SNAPSHOT_DT         DATE          NOT NULL,
    ON_HAND_QTY         NUMBER(18,3)  NOT NULL,
    RESERVED_QTY        NUMBER(18,3)  NOT NULL,
    IN_TRANSIT_QTY      NUMBER(18,3)  NOT NULL,
    AVAILABLE_QTY       NUMBER(18,3)  NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_FCT_INVENTORY PRIMARY KEY (INVENTORY_KY),
    CONSTRAINT UQ_FCT_INVENTORY_VERSION UNIQUE (ITEM_KY, LOCN_KY, SNAPSHOT_DT)
)
CLUSTER BY (SNAPSHOT_DT)
COMMENT = 'Inventory soft-close — close then insert; never delete';

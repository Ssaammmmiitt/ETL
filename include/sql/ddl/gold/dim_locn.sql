CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN (
    LOCN_KY             NUMBER(38,0)  NOT NULL,
    LOCN_ID             VARCHAR(50)   NOT NULL,
    LOCN_NM             VARCHAR(200),
    LOCN_TYPE           VARCHAR(50),
    ADDRESS_TXT         VARCHAR(500),
    POSTAL_CD           VARCHAR(20),
    CITY_KY             NUMBER(38,0)  NOT NULL,
    ACTIVE_FLG          BOOLEAN       NOT NULL,
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_DIM_LOCN PRIMARY KEY (LOCN_KY)
)
COMMENT = 'Location hierarchy leaf — SCD2 (D5 city move opens a new version)';

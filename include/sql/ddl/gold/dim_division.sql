CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION (
    DIVISION_KY         NUMBER(38,0)  NOT NULL,
    DIVISION_ID         VARCHAR(50)   NOT NULL,
    DIVISION_NM         VARCHAR(200),
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_DIM_DIVISION PRIMARY KEY (DIVISION_KY)
)
COMMENT = 'Location hierarchy root — SCD2';

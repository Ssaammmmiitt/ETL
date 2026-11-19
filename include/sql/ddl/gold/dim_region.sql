CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION (
    REGION_KY           NUMBER(38,0)  NOT NULL,
    REGION_ID           VARCHAR(50)   NOT NULL,
    REGION_NM           VARCHAR(200),
    DIVISION_KY         NUMBER(38,0)  NOT NULL,
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_DIM_REGION PRIMARY KEY (REGION_KY)
)
COMMENT = 'Location hierarchy — SCD2; parent = DIM_DIVISION';

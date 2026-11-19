CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY (
    CATEGORY_KY         NUMBER(38,0)  NOT NULL,
    CATEGORY_ID         VARCHAR(50)   NOT NULL,
    CATEGORY_NM         VARCHAR(200),
    CATEGORY_DESC       VARCHAR(500),
    DEPARTMENT_KY       NUMBER(38,0)  NOT NULL,
    EFFECTIVE_START_DT  DATE          NOT NULL,
    EFFECTIVE_END_DT    DATE          NOT NULL,
    CURRENT_FLG         BOOLEAN       NOT NULL,
    INSERT_TS           TIMESTAMP_NTZ NOT NULL,
    UPDATE_TS           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_DIM_CATEGORY PRIMARY KEY (CATEGORY_KY)
)
COMMENT = 'Product hierarchy — SCD2; parent = DIM_DEPARTMENT';

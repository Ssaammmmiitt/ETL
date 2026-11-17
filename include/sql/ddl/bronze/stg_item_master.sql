-- STG_ITEM_MASTER (Bronze): item_master_YYYYMMDD.csv as landed. Daily full extract.
-- Flattened item > subcategory > category > department.
-- Feeds TMP_ITEM_HIER, then DIM_DEPARTMENT, DIM_CATEGORY, DIM_SUBCATEGORY, DIM_ITEM.
-- Badge on the diagram: CSV | _INGEST_DT | full.
CREATE OR REPLACE TABLE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_ITEM_MASTER (
    ITEM_ID           VARCHAR,
    ITEM_NM           VARCHAR,
    ITEM_DESC         VARCHAR,
    BRAND_NM          VARCHAR,
    ITEM_TYPE         VARCHAR,
    SUBCATEGORY_ID    VARCHAR,
    SUBCATEGORY_NM    VARCHAR,
    SUBCATEGORY_DESC  VARCHAR,
    CATEGORY_ID       VARCHAR,
    CATEGORY_NM       VARCHAR,
    CATEGORY_DESC     VARCHAR,
    DEPARTMENT_ID     VARCHAR,
    DEPARTMENT_NM     VARCHAR,
    DEPARTMENT_DESC   VARCHAR,
    ACTIVE_FLG        VARCHAR,
    _INGEST_DT        DATE          NOT NULL,
    _SOURCE_FILE      VARCHAR       NOT NULL,
    _SOURCE_ROW_NUM   NUMBER        NOT NULL,
    _INGEST_TS        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _BATCH_ID         VARCHAR       NOT NULL
)
COMMENT = 'STG (Bronze): raw item master with its product hierarchy';

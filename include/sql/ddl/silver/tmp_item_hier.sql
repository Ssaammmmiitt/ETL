-- TMP_ITEM_HIER (Silver): flattened product hierarchy from item_master (full overwrite each run).
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER (
    ITEM_ID           VARCHAR(50)   NOT NULL,
    ITEM_NM           VARCHAR(200),
    ITEM_DESC         VARCHAR(1000),
    BRAND_NM          VARCHAR(100),
    ITEM_TYPE         VARCHAR(50),
    SUBCATEGORY_ID    VARCHAR(50)   NOT NULL,
    SUBCATEGORY_NM    VARCHAR(200),
    SUBCATEGORY_DESC  VARCHAR(500),
    CATEGORY_ID       VARCHAR(50)   NOT NULL,
    CATEGORY_NM       VARCHAR(200),
    CATEGORY_DESC     VARCHAR(500),
    DEPARTMENT_ID     VARCHAR(50)   NOT NULL,
    DEPARTMENT_NM     VARCHAR(200),
    DEPARTMENT_DESC   VARCHAR(500),
    ACTIVE_FLG        BOOLEAN       NOT NULL,
    _INGEST_DT        DATE          NOT NULL
)
COMMENT = 'TMP (Silver): item hierarchy flattened; INSERT OVERWRITE each run';

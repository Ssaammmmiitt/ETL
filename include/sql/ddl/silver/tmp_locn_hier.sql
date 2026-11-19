-- TMP_LOCN_HIER (Silver): flattened location hierarchy from store_master (full overwrite each run).
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER (
    LOCN_ID            VARCHAR(50)   NOT NULL,
    LOCN_NM            VARCHAR(200),
    LOCN_TYPE          VARCHAR(50),
    ADDRESS_TXT        VARCHAR(500),
    POSTAL_CD          VARCHAR(20),
    CITY_ID            VARCHAR(50)   NOT NULL,
    CITY_NM            VARCHAR(200),
    STATE_PROVINCE_NM  VARCHAR(100),
    COUNTRY_NM         VARCHAR(100),
    REGION_ID          VARCHAR(50)   NOT NULL,
    REGION_NM          VARCHAR(200),
    DIVISION_ID        VARCHAR(50)   NOT NULL,
    DIVISION_NM        VARCHAR(200),
    ACTIVE_FLG         BOOLEAN       NOT NULL,
    _INGEST_DT         DATE          NOT NULL
)
COMMENT = 'TMP (Silver): location hierarchy flattened; INSERT OVERWRITE each run';

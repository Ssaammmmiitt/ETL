-- STG_STORE_MASTER (Bronze): store_master_YYYYMMDD.csv as landed. Daily full extract.
-- Flattened location > city > region > division.
-- Feeds TMP_LOCN_HIER, then DIM_DIVISION, DIM_REGION, DIM_CITY, DIM_LOCN.
-- Badge on the diagram: CSV | _INGEST_DT | full.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_STORE_MASTER (
    LOCN_ID            VARCHAR,
    LOCN_NM            VARCHAR,
    LOCN_TYPE          VARCHAR,
    ADDRESS_TXT        VARCHAR,
    POSTAL_CD          VARCHAR,
    CITY_ID            VARCHAR,
    CITY_NM            VARCHAR,
    STATE_PROVINCE_NM  VARCHAR,
    COUNTRY_NM         VARCHAR,
    REGION_ID          VARCHAR,
    REGION_NM          VARCHAR,
    DIVISION_ID        VARCHAR,
    DIVISION_NM        VARCHAR,
    ACTIVE_FLG         VARCHAR,
    _INGEST_DT         DATE          NOT NULL,
    _SOURCE_FILE       VARCHAR       NOT NULL,
    _SOURCE_ROW_NUM    NUMBER        NOT NULL,
    _INGEST_TS         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _BATCH_ID          VARCHAR       NOT NULL
)
COMMENT = 'STG (Bronze): raw store master with its location hierarchy';

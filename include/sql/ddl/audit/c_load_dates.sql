-- C_LOAD_DATES (control): last successful business date per target table.
-- LAST_LOAD_DT is the high-water mark; a Gold rewind (reload of a past day) moves it back to D - 1.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES (
    TARGET_TABLE     VARCHAR(100)   NOT NULL,
    LAYER            VARCHAR(20)    NOT NULL,
    LAST_LOAD_DT     DATE           NOT NULL,
    LAST_RUN_DT      DATE           NOT NULL,
    LAST_BATCH_ID    VARCHAR(250)   NOT NULL,
    UPDATE_TS        TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_C_LOAD_DATES PRIMARY KEY (TARGET_TABLE)
)
COMMENT = 'Control: LAST_LOAD_DT = newest business date loaded; LAST_RUN_DT = date of the latest successful run';

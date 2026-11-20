-- C_PARAM (control): run parameters (business date, batch id, rules) as name/value rows per batch.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM (
    BATCH_ID         VARCHAR(250)   NOT NULL,
    BUSINESS_DT      DATE           NOT NULL,
    PARAM_NM         VARCHAR(100)   NOT NULL,
    PARAM_VAL        VARCHAR(1000),
    INSERT_TS        TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_C_PARAM PRIMARY KEY (BATCH_ID, PARAM_NM)
)
COMMENT = 'Control: parameters each batch ran with (BATCH_ID = Airflow run_id)';

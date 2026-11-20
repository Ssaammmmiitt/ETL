-- C_BATCH_SCRIPTS (control): which scripts run, in which order. Seeded every run by load/audit/c_batch_scripts.sql.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_SCRIPTS (
    RUN_SEQ          NUMBER(5,0)    NOT NULL,
    TASK_ID          VARCHAR(100)   NOT NULL,
    LAYER            VARCHAR(20)    NOT NULL,
    SCRIPT_PATH      VARCHAR(300)   NOT NULL,
    TARGET_TABLE     VARCHAR(300),
    GATE_ID          VARCHAR(30),
    ON_FAIL          VARCHAR(20)    NOT NULL,
    ACTIVE_FLG       BOOLEAN        NOT NULL,
    UPDATE_TS        TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_C_BATCH_SCRIPTS PRIMARY KEY (TASK_ID)
)
COMMENT = 'Control: one row per DAG task, RUN_SEQ = execution order, the SQL it runs and the table it writes';

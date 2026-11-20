-- C_BATCH_LOG (control): one row per task attempt, written by the DAG's success/failure callbacks.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_LOG (
    BATCH_ID         VARCHAR(250)   NOT NULL,
    BUSINESS_DT      DATE           NOT NULL,
    TASK_ID          VARCHAR(100)   NOT NULL,
    MAP_INDEX        NUMBER(5,0)    NOT NULL,
    TRY_NUMBER       NUMBER(5,0)    NOT NULL,
    SCRIPT_PATH      VARCHAR(300),
    STATUS           VARCHAR(20)    NOT NULL,
    START_TS         TIMESTAMP_NTZ,
    END_TS           TIMESTAMP_NTZ  NOT NULL,
    ELAPSED_SEC      NUMBER(10,1),
    ROWS_AFFECTED    NUMBER(18,0),
    FAILURE_REASON   VARCHAR(2000),
    CONSTRAINT PK_C_BATCH_LOG PRIMARY KEY (BATCH_ID, TASK_ID, MAP_INDEX, TRY_NUMBER)
)
COMMENT = 'Control: status, rows, elapsed time and failure reason of every task attempt';

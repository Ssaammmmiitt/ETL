-- C_REJ_TABLE (control): rejected rows and the rule that rejected them. Never drop a row silently.
-- Written by the Silver gates (G3-G6, G12) and by G7 before FCT_SALES; stored in AUDIT with the other C_ tables.
-- G1/G2 reject whole files: those fail the task and the reason is in C_BATCH_LOG.FAILURE_REASON.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE (
    INGEST_DT        DATE           NOT NULL,
    SOURCE_FILE      VARCHAR(200)   NOT NULL,
    OBJECT_NM        VARCHAR(100)   NOT NULL,
    GATE_ID          VARCHAR(10)    NOT NULL,
    REASON_CD        VARCHAR(100)   NOT NULL,
    RAW_RECORD       VARIANT        NOT NULL,
    QUARANTINED_TS   TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Control: rejected rows — OBJECT_NM = table the row was headed for, GATE_ID + REASON_CD = rule, RAW_RECORD = full row';

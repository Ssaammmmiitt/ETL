-- TMP_QUARANTINE (Silver): rejected rows with the gate that caught them. Never drop silently.
-- Transient: rebuildable; keep ~90 days in practice by deleting old INGEST_DT later.
CREATE TRANSIENT TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE (
    INGEST_DT        DATE           NOT NULL,
    SOURCE_FILE      VARCHAR(200)   NOT NULL,
    OBJECT_NM        VARCHAR(100)   NOT NULL,
    GATE_ID          VARCHAR(10)    NOT NULL,
    REASON_CD        VARCHAR(100)   NOT NULL,
    RAW_RECORD       VARIANT        NOT NULL,
    QUARANTINED_TS   TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'TMP (Silver): quarantined STG rows — GATE_ID + REASON_CD + full RAW_RECORD';

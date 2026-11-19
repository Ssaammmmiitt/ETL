-- L12: store BEFORE/AFTER fingerprints for one business day (idempotency proof).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.IDEM_DAY_SNAPSHOT (
    DAY_KY       NUMBER(8,0)   NOT NULL,
    PHASE        VARCHAR(20)   NOT NULL,
    METRICS      VARIANT       NOT NULL,
    SNAPSHOT_TS  TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_IDEM_DAY_SNAPSHOT PRIMARY KEY (DAY_KY, PHASE, SNAPSHOT_TS)
)
COMMENT = 'L12 — BEFORE/AFTER metric fingerprints for day D (counts and nets)';

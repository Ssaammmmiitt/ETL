-- G11 output: five reconciliation checks per business day. Every STATUS must be PASS.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_CHECKS (
    BUSINESS_DT      DATE           NOT NULL,
    CHECK_SEQ        NUMBER(2,0)    NOT NULL,
    CHECK_NM         VARCHAR(20)    NOT NULL,
    RULE_TXT         VARCHAR(200)   NOT NULL,
    ACTUAL_VAL       VARCHAR(200),
    EXPECTED_VAL     VARCHAR(200),
    STATUS           VARCHAR(4)     NOT NULL,
    BATCH_ID         VARCHAR(250)   NOT NULL,
    CHECK_TS         TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_RECON_DAILY_CHECKS PRIMARY KEY (BUSINESS_DT, CHECK_NM)
)
COMMENT = 'G11: row count, amount, orphans, freshness, duplicates per day (PASS/FAIL)';

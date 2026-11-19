-- G11 output: one row per day. VARIANCE_AMT must be 0.
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_NET_SALES (
    DAY_KY           NUMBER(8,0)   NOT NULL,
    SOURCE_NET_AMT   NUMBER(18,2)  NOT NULL,
    QUARANTINE_NET_AMT NUMBER(18,2) NOT NULL,
    FCT_NET_AMT      NUMBER(18,2)  NOT NULL,
    VARIANCE_AMT     NUMBER(18,2)  NOT NULL,
    RECON_TS         TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_RECON_DAILY_NET_SALES PRIMARY KEY (DAY_KY)
)
COMMENT = 'G11 — STG net − quarantine net − FCT net = VARIANCE (must be 0)';

-- SCD1 MERGE from TMP_CUSTOMER (members seen through Silver).

MERGE INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CUSTOMER t
USING (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NM,
        CUSTOMER_TYPE,
        EMAIL_TXT,
        PHONE_NUM,
        LAST_SEEN_DT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_CUSTOMER
    WHERE _INGEST_DT = '{{ ds }}'
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    CUSTOMER_NM = s.CUSTOMER_NM,
    CUSTOMER_TYPE = s.CUSTOMER_TYPE,
    EMAIL_TXT = s.EMAIL_TXT,
    PHONE_NUM = s.PHONE_NUM,
    LAST_SEEN_DT = s.LAST_SEEN_DT,
    UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_KY, CUSTOMER_ID, CUSTOMER_NM, CUSTOMER_TYPE, EMAIL_TXT, PHONE_NUM,
    LAST_SEEN_DT, INSERT_TS, UPDATE_TS
) VALUES (
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CUSTOMER_KY.NEXTVAL,
    s.CUSTOMER_ID,
    s.CUSTOMER_NM,
    s.CUSTOMER_TYPE,
    s.EMAIL_TXT,
    s.PHONE_NUM,
    s.LAST_SEEN_DT,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

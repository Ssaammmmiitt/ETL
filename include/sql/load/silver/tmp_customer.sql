-- Upsert members from today's cleaned TMP_SALES_LINE + STG member attributes.
-- Only rows that survived sales gates (join on txn keys via STG for name/email).

MERGE INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_CUSTOMER t
USING (
    SELECT
        tsl.CUSTOMER_ID,
        MAX(NULLIF(TRIM(s.CUSTOMER_NM), '')) AS CUSTOMER_NM,
        MAX(NULLIF(TRIM(s.CUSTOMER_TYPE), '')) AS CUSTOMER_TYPE,
        MAX(NULLIF(TRIM(s.EMAIL_TXT), '')) AS EMAIL_TXT,
        MAX(NULLIF(TRIM(s.PHONE_NUM), '')) AS PHONE_NUM,
        MAX(tsl.SALE_DT) AS LAST_SEEN_DT,
        MAX(tsl._INGEST_DT) AS _INGEST_DT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE tsl
    INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES s
        ON s._INGEST_DT = tsl._INGEST_DT
       AND s._SOURCE_FILE = tsl._SOURCE_FILE
       AND s._SOURCE_ROW_NUM = tsl._SOURCE_ROW_NUM
    WHERE tsl._INGEST_DT = '{{ ds }}'
      AND tsl.CUSTOMER_ID IS NOT NULL
    GROUP BY tsl.CUSTOMER_ID
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    CUSTOMER_NM = s.CUSTOMER_NM,
    CUSTOMER_TYPE = s.CUSTOMER_TYPE,
    EMAIL_TXT = s.EMAIL_TXT,
    PHONE_NUM = s.PHONE_NUM,
    LAST_SEEN_DT = s.LAST_SEEN_DT,
    _INGEST_DT = s._INGEST_DT
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, CUSTOMER_NM, CUSTOMER_TYPE, EMAIL_TXT, PHONE_NUM, LAST_SEEN_DT, _INGEST_DT
) VALUES (
    s.CUSTOMER_ID, s.CUSTOMER_NM, s.CUSTOMER_TYPE, s.EMAIL_TXT, s.PHONE_NUM, s.LAST_SEEN_DT, s._INGEST_DT
);

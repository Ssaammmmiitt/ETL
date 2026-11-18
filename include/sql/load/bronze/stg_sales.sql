-- Idempotent STG load for day D: replace the day's rows, then COPY with FORCE.
-- FORCE reloads a file Snowflake already recorded in load history (64 days).
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
WHERE _INGEST_DT = '{{ ds }}'::DATE;

COPY INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES (
    TRANSACTION_ID, LINE_NUM, SALE_DT, LOCN_ID, ITEM_ID,
    CUSTOMER_ID, CUSTOMER_NM, CUSTOMER_TYPE, EMAIL_TXT, PHONE_NUM,
    SOLD_QTY, UNIT_PRICE_AMT, DISCOUNT_AMT, SALES_AMT, TAX_AMT, NET_SALES_AMT,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
           '{{ ds }}'::DATE, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{{ run_id }}'
    FROM @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/
)
FILES       = ('sales_{{ ds_nodash }}.csv.gz')
FILE_FORMAT = (FORMAT_NAME = '{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_HEADER')
ON_ERROR    = ABORT_STATEMENT
FORCE       = TRUE;

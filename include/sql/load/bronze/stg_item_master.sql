-- Idempotent STG load for day D: replace the day's rows, then COPY with FORCE.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_ITEM_MASTER
WHERE _INGEST_DT = '{{ ds }}'::DATE;

COPY INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_ITEM_MASTER (
    ITEM_ID, ITEM_NM, ITEM_DESC, BRAND_NM, ITEM_TYPE,
    SUBCATEGORY_ID, SUBCATEGORY_NM, SUBCATEGORY_DESC,
    CATEGORY_ID, CATEGORY_NM, CATEGORY_DESC,
    DEPARTMENT_ID, DEPARTMENT_NM, DEPARTMENT_DESC, ACTIVE_FLG,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
           '{{ ds }}'::DATE, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{{ run_id }}'
    FROM @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/
)
FILES       = ('item_master_{{ ds_nodash }}.csv.gz')
FILE_FORMAT = (FORMAT_NAME = '{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_HEADER')
ON_ERROR    = ABORT_STATEMENT
FORCE       = TRUE;

-- Idempotent STG load for day D: replace the day's rows, then positional COPY with FORCE.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY
WHERE _INGEST_DT = '{{ ds }}'::DATE;

COPY INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY (
    SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY,
    UNIT_PRICE_AMT, PROMO_PRICE_AMT,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8,
           '{{ ds }}'::DATE, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{{ run_id }}'
    FROM @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/
)
FILES       = ('inventory_{{ ds_nodash }}.csv.gz')
FILE_FORMAT = (FORMAT_NAME = '{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_POSITIONAL')
ON_ERROR    = ABORT_STATEMENT
FORCE       = TRUE;

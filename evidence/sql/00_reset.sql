-- RESET (destructive): empty every data table so a backfill starts from a clean slate.
-- Keeps all DDL, sequences, stage, file formats and the view. Run on purpose only:
--   python /opt/airflow/evidence/run_queries.py reset --yes
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_ITEM_MASTER;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_STORE_MASTER;

TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_CUSTOMER;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_HIER;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_LOCN_HIER;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE;

TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CUSTOMER;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DAY;

TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_CHECKS;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_LOG;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM;
TRUNCATE TABLE IF EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES;

REMOVE @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE;

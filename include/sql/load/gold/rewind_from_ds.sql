-- Reload one bad day D (= ds): rewind Gold to its state at the end of D - 1, then the run loads D on top.
-- A load of day X closes the old version with END = X - 1 and opens the new one with START = X, so:
--   delete what loads >= D opened, reopen what loads >= D closed. Running it twice changes nothing.
-- Afterwards C_LOAD_DATES says Gold is current to D - 1; replay D+1 .. old high-water in order to catch up.
-- DIM_DAY (seed) and DIM_CUSTOMER (SCD1, no history) are not rewound; the replay re-applies them.

-- Audit trail: this batch rewound Gold, and from which high-water date.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM
WHERE BATCH_ID = '{{ run_id }}' AND PARAM_NM = 'GOLD_REWOUND_FROM';

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM (
    BATCH_ID, BUSINESS_DT, PARAM_NM, PARAM_VAL, INSERT_TS
)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, 'GOLD_REWOUND_FROM', MAX(LAST_LOAD_DT)::VARCHAR, CURRENT_TIMESTAMP()
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES
WHERE LAYER = 'GOLD';

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
WHERE DAY_KY >= TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'));

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
WHERE DAY_KY >= TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'));

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
WHERE SNAPSHOT_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_SUBCATEGORY
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CATEGORY
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DEPARTMENT
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CITY
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_REGION
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION
WHERE EFFECTIVE_START_DT >= '{{ ds }}'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DIVISION
SET EFFECTIVE_END_DT = '9999-12-31'::DATE, CURRENT_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHERE EFFECTIVE_END_DT >= DATEADD(day, -1, '{{ ds }}'::DATE) AND EFFECTIVE_END_DT < '9999-12-31'::DATE;

UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES
SET LAST_LOAD_DT = DATEADD(day, -1, '{{ ds }}'::DATE), UPDATE_TS = CURRENT_TIMESTAMP()
WHERE LAYER = 'GOLD' AND LAST_LOAD_DT >= '{{ ds }}'::DATE;

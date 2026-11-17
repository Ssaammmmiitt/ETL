-- Lesson 2: reusable CSV parsing rules for every COPY INTO in the pipeline.
-- CREATE OR REPLACE is safe here: a file format holds no data, so replacing it just applies edits.

-- Files with a header row: sales, item_master, store_master (and the DIM_DAY seed).
CREATE OR REPLACE FILE FORMAT {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_HEADER
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    SKIP_HEADER                    = 1
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    EMPTY_FIELD_AS_NULL            = TRUE
    NULL_IF                        = ('', 'NULL', 'null')
    TRIM_SPACE                     = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ENCODING                       = 'UTF8'
    COMMENT                        = 'CSV with one header row; Bronze keeps values as landed';

-- Files with no header and a contracted column order: inventory (positional load).
CREATE OR REPLACE FILE FORMAT {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_POSITIONAL
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    SKIP_HEADER                    = 0
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    EMPTY_FIELD_AS_NULL            = TRUE
    NULL_IF                        = ('', 'NULL', 'null')
    TRIM_SPACE                     = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ENCODING                       = 'UTF8'
    COMMENT                        = 'CSV without header; columns mapped by position ($1, $2, ...)';

-- Lesson 3: the internal stage that receives the daily files, one folder per business date.
-- IF NOT EXISTS, not OR REPLACE: replacing a stage deletes every file inside it.
CREATE STAGE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE
    COMMENT = 'Internal stage: daily source files under YYYY-MM-DD/ folders, gzip-compressed by PUT';

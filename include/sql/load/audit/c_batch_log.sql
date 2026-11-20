-- C_BATCH_LOG: one row per task attempt. Values are bound by runner.log_task (pyformat placeholders).
-- Do not put pyformat tokens in comments: the Snowflake binder scans the whole statement.
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_LOG (
    BATCH_ID, BUSINESS_DT, TASK_ID, MAP_INDEX, TRY_NUMBER, SCRIPT_PATH, STATUS,
    START_TS, END_TS, ELAPSED_SEC, ROWS_AFFECTED, FAILURE_REASON
)
SELECT
    %(batch_id)s, %(business_dt)s::DATE, %(task_id)s, %(map_index)s, %(try_number)s,
    (SELECT MAX(SCRIPT_PATH)
     FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_SCRIPTS
     WHERE TASK_ID = %(task_id)s),
    %(status)s, %(start_ts)s::TIMESTAMP_NTZ, %(end_ts)s::TIMESTAMP_NTZ, %(elapsed_sec)s, %(rows_affected)s, %(failure_reason)s;

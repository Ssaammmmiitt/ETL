-- C_PARAM: the parameters this batch runs with. Re-running the same run_id replaces its rows.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM
WHERE BATCH_ID = '{{ run_id }}';

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_PARAM (
    BATCH_ID, BUSINESS_DT, PARAM_NM, PARAM_VAL, INSERT_TS
)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, v.PARAM_NM, v.PARAM_VAL, CURRENT_TIMESTAMP()
FROM (VALUES
    ('BUSINESS_DT',      '{{ ds }}'),
    ('BATCH_ID',         '{{ run_id }}'),
    ('RUN_TYPE',         '{{ dag_run.run_type }}'),
    ('TRY_NUMBER',       '{{ ti.try_number }}'),
    ('LANDING_PATH',     '{{ var.json.medallion_config.landing_path }}'),
    ('ARCHIVE_PATH',     '{{ var.json.medallion_config.archive_path }}'),
    ('MAX_SOLD_QTY',     '{{ var.json.medallion_config.outlier_rules.max_sold_qty }}'),
    ('MAX_LINE_NET_AMT', '{{ var.json.medallion_config.outlier_rules.max_line_net_amt }}'),
    ('MAX_ON_HAND_QTY',  '{{ var.json.medallion_config.outlier_rules.max_on_hand_qty }}'),
    ('PRICE_RATIO_MIN',  '{{ var.json.medallion_config.outlier_rules.price_ratio_min }}'),
    ('PRICE_RATIO_MAX',  '{{ var.json.medallion_config.outlier_rules.price_ratio_max }}'),
    ('G8_MAX_DRIFT',     '{{ params.g8_max_drift }}'),
    ('ORPHAN_MAX_PCT',   '{{ params.orphan_max_pct }}')
) AS v (PARAM_NM, PARAM_VAL);

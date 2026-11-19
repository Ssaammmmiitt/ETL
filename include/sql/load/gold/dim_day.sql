-- DIM_DAY: full replace from seed (SCD0). Idempotent.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DAY;

COPY INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DAY (
    DAY_KY, CALENDAR_DT, DAY_OF_MONTH, DAY_OF_WEEK_NUM, DAY_OF_WEEK_NM,
    WEEK_OF_YEAR_NUM, MONTH_NUM, MONTH_NM, QUARTER_NUM, YEAR_NUM,
    WEEKEND_FLG, HOLIDAY_FLG
)
FROM (
    SELECT
        TRY_TO_NUMBER($1, 8, 0),
        TRY_TO_DATE($2),
        TRY_TO_NUMBER($3, 2, 0),
        TRY_TO_NUMBER($4, 1, 0),
        $5,
        TRY_TO_NUMBER($6, 2, 0),
        TRY_TO_NUMBER($7, 2, 0),
        $8,
        TRY_TO_NUMBER($9, 1, 0),
        TRY_TO_NUMBER($10, 4, 0),
        $11 = 'Y',
        $12 = 'Y'
    FROM @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/seed/
)
FILES = ('seed_dim_day.csv.gz')
FILE_FORMAT = (FORMAT_NAME = '{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_HEADER')
ON_ERROR = ABORT_STATEMENT
FORCE = TRUE;

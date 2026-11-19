-- DIM_DAY (Gold): calendar seed. SCD0. Loaded straight from seed_dim_day.csv (departure 1).
CREATE TABLE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_DAY (
    DAY_KY            NUMBER(8,0)   NOT NULL,
    CALENDAR_DT       DATE          NOT NULL,
    DAY_OF_MONTH      NUMBER(2,0)   NOT NULL,
    DAY_OF_WEEK_NUM   NUMBER(1,0)   NOT NULL,
    DAY_OF_WEEK_NM    VARCHAR(20)   NOT NULL,
    WEEK_OF_YEAR_NUM  NUMBER(2,0)   NOT NULL,
    MONTH_NUM         NUMBER(2,0)   NOT NULL,
    MONTH_NM          VARCHAR(20)   NOT NULL,
    QUARTER_NUM       NUMBER(1,0)   NOT NULL,
    YEAR_NUM          NUMBER(4,0)   NOT NULL,
    WEEKEND_FLG       BOOLEAN       NOT NULL,
    HOLIDAY_FLG       BOOLEAN       NOT NULL,
    CONSTRAINT PK_DIM_DAY PRIMARY KEY (DAY_KY)
)
COMMENT = 'DIM_DAY: seeded calendar (no STG/TMP)';

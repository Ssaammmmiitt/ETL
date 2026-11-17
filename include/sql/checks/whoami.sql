-- Lesson 1 smoke test: who Airflow connects as, and which config it rendered.
-- var.json.medallion_config.database and conn.snowflake_medallion.extra_dejson.warehouse
-- are Jinja templates in the SELECT below; keep them out of comments.
SELECT
    CURRENT_USER()                                              AS current_user_name,
    CURRENT_ROLE()                                              AS current_role_name,
    CURRENT_WAREHOUSE()                                         AS current_warehouse_name,
    '{{ var.json.medallion_config.database }}'                  AS configured_database,
    '{{ conn.snowflake_medallion.extra_dejson.warehouse }}'     AS connection_warehouse,
    '{{ ds }}'                                                  AS logical_date,
    '{{ var.json.medallion_config.alert_email }}'               AS alert_email;

"""Lesson 8: Gold dimensions — SCD2 hierarchies, SCD1 customer/item desc, DIM_DAY seed.

Expects Silver TMP loaded for {{ ds }} (run 07_silver first).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

from include.medallion.config import SNOWFLAKE_CONN_ID, SQL_DIR

DDL_FILES = [
    "ddl/gold/sequences.sql",
    "ddl/gold/dim_day.sql",
    "ddl/gold/dim_department.sql",
    "ddl/gold/dim_category.sql",
    "ddl/gold/dim_subcategory.sql",
    "ddl/gold/dim_item.sql",
    "ddl/gold/dim_division.sql",
    "ddl/gold/dim_region.sql",
    "ddl/gold/dim_city.sql",
    "ddl/gold/dim_locn.sql",
    "ddl/gold/dim_customer.sql",
]


@dag(
    dag_id="08_gold_dims",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "08"],
)
def gold_dims():
    prev = None
    for path in DDL_FILES:
        name = path.rsplit("/", 1)[-1].replace(".sql", "")
        create = SQLExecuteQueryOperator(
            task_id=f"create_{name}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=path,
            split_statements=True,
        )
        if prev is not None:
            prev >> create
        prev = create

    put_seed = SQLExecuteQueryOperator(
        task_id="put_seed_dim_day",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="stage/put_seed_dim_day.sql",
    )
    load_day = SQLExecuteQueryOperator(
        task_id="load_dim_day",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_day.sql",
        split_statements=True,
    )

    load_dept = SQLExecuteQueryOperator(
        task_id="load_dim_department",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_department.sql",
        split_statements=True,
    )
    load_cat = SQLExecuteQueryOperator(
        task_id="load_dim_category",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_category.sql",
        split_statements=True,
    )
    load_sub = SQLExecuteQueryOperator(
        task_id="load_dim_subcategory",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_subcategory.sql",
        split_statements=True,
    )
    load_item = SQLExecuteQueryOperator(
        task_id="load_dim_item",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_item.sql",
        split_statements=True,
    )

    load_div = SQLExecuteQueryOperator(
        task_id="load_dim_division",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_division.sql",
        split_statements=True,
    )
    load_reg = SQLExecuteQueryOperator(
        task_id="load_dim_region",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_region.sql",
        split_statements=True,
    )
    load_city = SQLExecuteQueryOperator(
        task_id="load_dim_city",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_city.sql",
        split_statements=True,
    )
    load_locn = SQLExecuteQueryOperator(
        task_id="load_dim_locn",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_locn.sql",
        split_statements=True,
    )
    load_cust = SQLExecuteQueryOperator(
        task_id="load_dim_customer",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_customer.sql",
    )

    prev >> put_seed >> load_day
    load_day >> load_dept >> load_cat >> load_sub >> load_item
    load_day >> load_div >> load_reg >> load_city >> load_locn
    [load_item, load_locn] >> load_cust


gold_dims()

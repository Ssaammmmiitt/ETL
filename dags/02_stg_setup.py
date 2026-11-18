"""Lesson 2: create the CSV file formats and one STG (Bronze) table per source file.

Every statement lives in a SQL file under include/sql/. The table for source `sales` is
built by ddl/bronze/stg_sales.sql, and so on for the other three sources.
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

from include.medallion.config import SNOWFLAKE_CONN_ID, SOURCES, SQL_DIR


@dag(
    dag_id="02_stg_setup",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "02"],
)
def stg_setup():
    create_file_formats = SQLExecuteQueryOperator(
        task_id="create_file_formats",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="setup/01_file_formats.sql",
    )

    for source in SOURCES:
        create_table = SQLExecuteQueryOperator(
            task_id=f"create_stg_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"ddl/bronze/stg_{source}.sql",
        )
        create_file_formats >> create_table


stg_setup()

"""Lesson 3: put the day's four files on the internal stage, list them, and COPY them into STG.

Run it for one business date, e.g. `airflow dags test 03_stage_load 2026-11-10`:
{{ ds }} picks the files (<source>_20261110.csv) and the stage folder (2026-11-10/).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag

from include.medallion.config import SNOWFLAKE_CONN_ID, SOURCES, SQL_DIR


@dag(
    dag_id="03_stage_load",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "03"],
)
def stage_load():
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="setup/02_stages.sql",
    )

    list_stage = SQLExecuteQueryOperator(
        task_id="list_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="checks/list_stage.sql",
    )

    for source in SOURCES:
        put_file = SQLExecuteQueryOperator(
            task_id=f"put_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="stage/put_file.sql",
            params={"source": source, "overwrite": "FALSE"},
        )
        copy_file = SQLExecuteQueryOperator(
            task_id=f"copy_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"load/bronze/stg_{source}.sql",
        )
        create_stage >> put_file >> list_stage >> copy_file


stage_load()

"""Lesson 1 smoke test: Airflow can reach Snowflake and read the project config.

The Snowflake account, user and key live in the `snowflake_medallion` connection.
Database and schema names come from the `medallion_config` Variable.
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable, dag, task

from include.medallion.config import CONFIG_VARIABLE, SNOWFLAKE_CONN_ID, SQL_DIR


@dag(
    dag_id="01_connection_check",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "01"],
)
def connection_check():
    whoami = SQLExecuteQueryOperator(
        task_id="whoami",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="checks/whoami.sql",
        show_return_value_in_logs=True,
    )

    @task
    def check_schemas():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        database = cfg["database"]
        expected = set(cfg["schemas"].values())

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(f"SELECT schema_name FROM {database}.information_schema.schemata")
        found = {row[0] for row in rows}

        missing = expected - found
        if missing:
            raise ValueError(f"Missing schemas in {database}: {sorted(missing)}")
        print(f"All expected schemas exist in {database}: {sorted(expected)}")

    whoami >> check_schemas()


connection_check()

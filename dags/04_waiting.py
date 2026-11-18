"""Lesson 4: wait for the four landing files (gate G1), PUT them, assert they are on the stage.

FileSensor (mapped x4) = "files arrive by SLA". assert_files_on_stage = "files on stage, non-empty".
Assets are taught in the lesson notes; this DAG uses FileSensor (files come from outside Airflow).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Variable, dag, task

from include.medallion.config import (
    CONFIG_VARIABLE,
    LANDING_FS_CONN_ID,
    SENSOR_POKE_SECONDS,
    SENSOR_TIMEOUT_SECONDS,
    SNOWFLAKE_CONN_ID,
    SOURCES,
    SQL_DIR,
)


@dag(
    dag_id="04_waiting",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "04"],
)
def waiting():
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="setup/02_stages.sql",
    )

    # One mapped task per source. filepath is relative to the fs_landing connection path.
    # Strings keep {{ ds_nodash }} so Airflow fills the date at run time.
    wait_for_file = FileSensor.partial(
        task_id="wait_for_file",
        fs_conn_id=LANDING_FS_CONN_ID,
        poke_interval=SENSOR_POKE_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    ).expand(filepath=[f"{source}_{{{{ ds_nodash }}}}.csv" for source in SOURCES])

    @task(task_id="assert_files_on_stage")
    def assert_files_on_stage(ds: str) -> list[dict]:
        """Gate G1: LIST the day's stage folder; require exactly 4 non-empty files."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        stage = f"@{cfg['database']}.{cfg['schemas']['bronze']}.MY_INT_STAGE/{ds}/"
        expected = len(SOURCES)

        # LIST + RESULT_SCAN must share one session (LAST_QUERY_ID is per session).
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"LIST {stage}")
            cur.execute(
                'SELECT "name", "size" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY 1'
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        files = [{"name": name, "size": size} for name, size in rows]
        if len(files) != expected:
            raise ValueError(
                f"G1 failed: expected {expected} files on {stage}, found {len(files)}: "
                f"{[f['name'] for f in files]}"
            )
        empty = [f["name"] for f in files if not f["size"]]
        if empty:
            raise ValueError(f"G1 failed: empty staged file(s): {empty}")

        print(f"G1 OK: {expected} non-empty files on {stage}")
        for f in files:
            print(f"  {f['name']}  ({f['size']} bytes)")
        return files

    create_stage >> wait_for_file

    assert_stage = assert_files_on_stage()

    for source in SOURCES:
        put_file = SQLExecuteQueryOperator(
            task_id=f"put_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="stage/put_file.sql",
            params={"source": source, "overwrite": "FALSE"},
        )
        wait_for_file >> put_file >> assert_stage


waiting()

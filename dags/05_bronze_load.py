"""Lesson 5: Bronze load for one day — wait, PUT (overwrite), G1/G2, idempotent COPY, archive.

Idempotent pattern per table:
  DELETE WHERE _INGEST_DT = ds  +  COPY ... FORCE = TRUE
so a second run for the same date keeps the same row counts.
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
    SOURCE_CONTRACTS,
    SNOWFLAKE_CONN_ID,
    SOURCES,
    SQL_DIR,
)


@dag(
    dag_id="05_bronze_load",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "05"],
)
def bronze_load():
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="setup/02_stages.sql",
    )

    wait_for_file = FileSensor.partial(
        task_id="wait_for_file",
        fs_conn_id=LANDING_FS_CONN_ID,
        poke_interval=SENSOR_POKE_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    ).expand(filepath=[f"{source}_{{{{ ds_nodash }}}}.csv" for source in SOURCES])

    @task(task_id="assert_files_on_stage")
    def assert_files_on_stage(ds: str) -> list[dict]:
        """Gate G1 presence: LIST the day's stage folder; require 4 non-empty files."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        stage = f"@{cfg['database']}.{cfg['schemas']['bronze']}.MY_INT_STAGE/{ds}/"
        expected = len(SOURCES)

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
        return files

    @task
    def g2_check_file(source: str) -> dict:
        """Gate G2: landing file column count (and header presence) matches the contract."""
        import csv
        from pathlib import Path

        from airflow.sdk import get_current_context

        ds_nodash = get_current_context()["ds_nodash"]
        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        contract = SOURCE_CONTRACTS[source]
        path = Path(cfg["landing_path"]) / f"{source}_{ds_nodash}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"G2 failed: missing {path}")

        with path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))

        if not rows:
            raise ValueError(f"G2 failed: {path.name} has no rows")

        expected = contract["columns"]
        if contract["has_header"]:
            header, data_rows = rows[0], rows[1:]
            if len(header) != expected:
                raise ValueError(
                    f"G2 failed: {path.name} header has {len(header)} fields, "
                    f"expected {expected}: {header}"
                )
        else:
            data_rows = rows

        bad = [i for i, row in enumerate(data_rows, start=1) if len(row) != expected]
        if bad:
            raise ValueError(
                f"G2 failed: {path.name} expected {expected} fields per row; "
                f"bad data-row indexes {bad[:5]} (up to 5 shown)"
            )

        print(
            f"G2 OK: {path.name}  header={contract['has_header']}  "
            f"cols={expected}  data_rows={len(data_rows)}"
        )
        return {"source": source, "columns": expected, "data_rows": len(data_rows)}

    @task(task_id="archive_landing_files")
    def archive_landing_files() -> list[str]:
        """Move the day's landing CSVs into archive/<ds>/ after a successful load."""
        import shutil
        from pathlib import Path

        from airflow.sdk import get_current_context

        ctx = get_current_context()
        ds, ds_nodash = ctx["ds"], ctx["ds_nodash"]
        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        landing = Path(cfg["landing_path"])
        archive_dir = Path(cfg["archive_path"]) / ds
        archive_dir.mkdir(parents=True, exist_ok=True)

        moved = []
        for source in SOURCES:
            src = landing / f"{source}_{ds_nodash}.csv"
            dst = archive_dir / src.name
            if not src.is_file():
                raise FileNotFoundError(f"archive failed: missing {src}")
            shutil.move(str(src), str(dst))
            moved.append(str(dst))
            print(f"archived {src.name} → {dst}")
        return moved

    create_stage >> wait_for_file
    assert_stage = assert_files_on_stage()
    archive = archive_landing_files()

    for source in SOURCES:
        put_file = SQLExecuteQueryOperator(
            task_id=f"put_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="stage/put_file.sql",
            params={"source": source, "overwrite": "TRUE"},
        )
        g2 = g2_check_file.override(task_id=f"g2_check_{source}")(source)
        load = SQLExecuteQueryOperator(
            task_id=f"load_stg_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"load/bronze/stg_{source}.sql",
            split_statements=True,
        )
        wait_for_file >> put_file >> assert_stage
        assert_stage >> g2 >> load >> archive


bronze_load()

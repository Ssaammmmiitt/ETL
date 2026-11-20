"""Medallion pipeline for ONE logical date ``ds``; run many dates with a backfill.

  setup_platform → control (C_BATCH_SCRIPTS, C_PARAM) → generate_landing_day → wait_for_files
  → bronze → silver → gold_dims → gold_facts → reconcile → load_c_load_dates

Every task attempt is written to C_BATCH_LOG by the callbacks. A ds older than Gold's newest
date rewinds Gold to ds - 1 first (reload one bad day). Task-by-task notes: ``context/final_state.md``.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup, Variable, chain, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from include.medallion import runner
from include.medallion.config import (
    CONFIG_VARIABLE,
    G8_MAX_DRIFT,
    LANDING_FS_CONN_ID,
    ORPHAN_MAX_PCT,
    SENSOR_POKE_SECONDS,
    SENSOR_TIMEOUT_SECONDS,
    SNOWFLAKE_CONN_ID,
    SOURCES,
    SQL_DIR,
)

SILVER_SALES = "load/silver/tmp_sales_line.sql"
SILVER_AFTER_SALES = ["load/silver/tmp_customer.sql", "load/silver/tmp_item_loc_snapshot.sql"]
SILVER_HIERARCHIES = ["load/silver/tmp_item_hier.sql", "load/silver/tmp_locn_hier.sql"]
ITEM_DIMS = ["load/gold/dim_department.sql", "load/gold/dim_category.sql", "load/gold/dim_subcategory.sql", "load/gold/dim_item.sql"]
LOCN_DIMS = ["load/gold/dim_division.sql", "load/gold/dim_region.sql", "load/gold/dim_city.sql", "load/gold/dim_locn.sql"]


def sql_task(path: str) -> SQLExecuteQueryOperator:
    """One SQL file = one task; task_id from the file name (load_<table>, g07_..., put_...)."""
    stem = Path(path).stem
    return SQLExecuteQueryOperator(
        task_id=f"load_{stem}" if path.startswith("load/") else stem,
        conn_id=SNOWFLAKE_CONN_ID,
        sql=path,
        split_statements=True,
    )


@dag(
    dag_id="retail_medallion_daily",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",  # periodic: required by Airflow 3 `backfill create`
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,  # SCD2 + inventory soft-close need chronological days
    template_searchpath=[str(SQL_DIR)],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_success_callback": runner.log_success,
        "on_failure_callback": runner.log_failure,
        "on_retry_callback": runner.log_retry,
    },
    params={"g8_max_drift": G8_MAX_DRIFT, "orphan_max_pct": ORPHAN_MAX_PCT},
    tags=["medallion", "daily"],
    doc_md=__doc__,
)
def retail_medallion_daily():
    @task
    def setup_platform(ds: str) -> int:
        """File formats, stage and every STG/TMP/Gold/Audit/control table (IF NOT EXISTS), one session."""
        files = runner.ddl_files()
        runner.run_sql_files(files, ds=ds)
        return len(files)

    @task
    def generate_landing_day(ds: str) -> list[dict]:
        """Four CSVs for this ds; planted defects are appended to data/store/defect_log.txt."""
        return runner.generate_day(ds)

    wait_for_files = FileSensor.partial(
        task_id="wait_for_files",
        fs_conn_id=LANDING_FS_CONN_ID,
        poke_interval=SENSOR_POKE_SECONDS,
        timeout=SENSOR_TIMEOUT_SECONDS,
        mode="reschedule",
    ).expand(filepath=[f"{source}_{{{{ ds_nodash }}}}.csv" for source in SOURCES])

    with TaskGroup(group_id="bronze") as bronze:

        @task
        def ingest_one(source: str) -> int:
            """PUT → G2 contract → DELETE + COPY FORCE for one source file."""
            ctx = get_current_context()
            ds, run_id = ctx["ds"], ctx["run_id"]
            runner.run_sql_file("stage/put_file.sql", ds=ds, run_id=run_id, params={"source": source, "overwrite": "TRUE"})
            rows = runner.g2_check(source, ds)
            runner.run_sql_file(f"load/bronze/stg_{source}.sql", ds=ds, run_id=run_id)
            return rows

        @task
        def assert_stage_g1(ds: str) -> list[tuple]:
            files = runner.list_stage(ds)
            if len(files) != len(SOURCES) or not all(size for _, size in files):
                raise AirflowFailException(f"G1: expected {len(SOURCES)} non-empty files, got {files}")
            return files

        @task
        def archive_landing(ds: str, ds_nodash: str) -> None:
            cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
            archive = Path(cfg["archive_path"]) / ds
            archive.mkdir(parents=True, exist_ok=True)
            for source in SOURCES:
                src = Path(cfg["landing_path"]) / f"{source}_{ds_nodash}.csv"
                src.replace(archive / src.name)

        ingest_one.expand(source=SOURCES) >> assert_stage_g1() >> archive_landing()

    with TaskGroup(group_id="silver") as silver:  # quarantine gates: bad rows logged, run stays green

        @task
        def assert_sales_identity(ds: str) -> None:
            stg, tmp, quar = runner.fetch("checks/silver_identity_sales.sql", ds=ds, first=True)
            if int(stg) != int(tmp) + int(quar):
                raise AirflowFailException(f"identity {ds}: STG {stg} != TMP {tmp} + QUAR {quar}")

        @task
        def g06b_alert(ds: str) -> int:
            """G6b alert only: reserved > on hand is logged, the row still loads."""
            rows = runner.fetch("gates/g06b_reserved_vs_on_hand.sql", ds=ds)
            print(f"G6b alert {ds}: {len(rows)} row(s) reserved > on hand {rows[:5]}")
            return len(rows)

        sales = sql_task(SILVER_SALES)
        after_sales = [sql_task(p) for p in SILVER_AFTER_SALES]
        sales >> after_sales
        after_sales[1] >> g06b_alert()
        chain(after_sales, *[sql_task(p) for p in SILVER_HIERARCHIES], assert_sales_identity())

    with TaskGroup(group_id="gold_dims") as gold_dims:  # SCD2 hierarchies, SCD1 item desc + customer

        @task
        def rewind_gold_for_reload(ds: str, run_id: str) -> str | None:
            """Reload of a past day: Gold back to ds - 1 so SCD2, inventory and price history replay in order."""
            high_water = runner.rewind_gold_if_past_day(ds, run_id)
            if high_water:
                print(f"RELOAD {ds}: Gold rewound to the day before; replay up to {high_water} in date order")
            else:
                print(f"No rewind {ds}: not older than Gold's newest loaded day")
            return high_water

        load_day = (
            rewind_gold_for_reload()
            >> sql_task("stage/put_seed_dim_day.sql")
            >> sql_task("load/gold/dim_day.sql")
        )
        item = [sql_task(p) for p in ITEM_DIMS]
        locn = [sql_task(p) for p in LOCN_DIMS]
        chain(load_day, *item)
        chain(load_day, *locn)
        [item[-1], locn[-1]] >> sql_task("load/gold/dim_customer.sql")

    with TaskGroup(group_id="gold_facts") as gold_facts:

        @task
        def g07_alert(ds: str) -> int:
            """G7 alert: orphans were quarantined by g07_keys_resolve; log the count."""
            rows = runner.fetch("checks/g07_orphan_summary.sql", ds=ds)
            print(f"G7 alert {ds}: {rows}")
            return sum(int(c) for _, c in rows)

        @task
        def g08_inventory_count(ds: str) -> None:
            """G8 fail: inventory rows within ±G8_MAX_DRIFT of the previous loaded day (skipped on the first day)."""
            today, prior, prior_dt = runner.fetch("gates/g08_inventory_count.sql", ds=ds, first=True)
            if not prior:
                print(f"G8 skipped {ds}: no earlier inventory day")
                return
            drift = abs(int(today) - int(prior)) / int(prior)
            print(f"G8 {ds}: today={today} prior({prior_dt})={prior} drift={drift:.1%}")
            if drift > G8_MAX_DRIFT:
                raise AirflowFailException(f"G8 {ds}: inventory drift {drift:.1%} > {G8_MAX_DRIFT:.0%}")

        @task
        def g10_price_overlap(ds: str) -> None:
            """G10 fail: no two price periods overlap for the same item and store."""
            rows = runner.fetch("gates/g10_price_overlap.sql", ds=ds)
            if rows:
                raise AirflowFailException(f"G10 {ds}: overlapping price periods {rows[:5]}")

        g07 = sql_task("gates/g07_keys_resolve.sql")
        g07 >> g07_alert() >> sql_task("load/gold/fct_sales.sql")
        g07 >> g08_inventory_count() >> sql_task("load/gold/fct_inventory.sql")
        g07 >> sql_task("load/gold/fct_price.sql") >> g10_price_overlap()

    with TaskGroup(group_id="reconcile") as reconcile:  # the only stage that fails the run on bad data

        @task
        def assert_reconciliation(ds: str) -> None:
            """G9 aggregate = fact; G11 the five RECON_DAILY_CHECKS rows of ds must all be PASS."""
            g9 = runner.fetch("gates/g09_agg_equals_fct.sql", ds=ds)
            if g9:
                raise AirflowFailException(f"G9 {ds}: AGG != FCT {g9}")
            checks = runner.fetch("checks/recon_daily_checks.sql", ds=ds)
            for name, actual, expected, status in checks:
                print(f"G11 {ds} {name:<10} actual={actual} expected={expected} {status}")
            failed = [c[0] for c in checks if c[3] != "PASS"]
            if len(checks) != 5 or failed:
                raise AirflowFailException(f"G11 {ds}: {len(checks)} checks, failed {failed}")

        @task
        def assert_primary_keys(ds: str) -> None:
            """PK checker: every gates/pk_*.sql returns the duplicated keys of one Gold table; any row fails."""
            bad = {p.stem: rows for p in sorted((SQL_DIR / "gates").glob("pk_*.sql"))
                   if (rows := runner.fetch(f"gates/{p.name}", ds=ds))}
            if bad:
                raise AirflowFailException(f"PK duplicates {ds}: {bad}")

        chain(
            sql_task("load/gold/agg_daily_item_locn_sales.sql"),
            sql_task("load/audit/recon_daily_checks.sql"),
            assert_reconciliation(),
            assert_primary_keys(),
        )

    chain(
        setup_platform(),
        sql_task("load/audit/c_batch_scripts.sql"),
        sql_task("load/audit/c_param.sql"),
        generate_landing_day(),
        wait_for_files,
        bronze,
        silver,
        gold_dims,
        gold_facts,
        reconcile,
        sql_task("load/audit/c_load_dates.sql"),
    )


retail_medallion_daily()

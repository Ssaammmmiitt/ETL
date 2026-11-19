"""Lesson 11: AGG + VW, gate G9, G11 recon with return-flag branch and SMTP alert.

Expects FCT_SALES loaded for the days you want in the recon table (run 09_gold_facts).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.sdk import dag, task
from airflow.exceptions import AirflowFailException

from include.medallion.config import (
    CONFIG_VARIABLE,
    SNOWFLAKE_CONN_ID,
    SMTP_CONN_ID,
    SQL_DIR,
)


@dag(
    dag_id="11_agg_recon",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "11"],
)
def agg_recon():
    create_agg = SQLExecuteQueryOperator(
        task_id="create_agg_daily_item_locn_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="ddl/gold/agg_daily_item_locn_sales.sql",
    )
    create_vw = SQLExecuteQueryOperator(
        task_id="create_vw_daily_item_locn_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="ddl/gold/vw_daily_item_locn_sales.sql",
    )
    create_recon = SQLExecuteQueryOperator(
        task_id="create_recon_daily_net_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="ddl/audit/recon_daily_net_sales.sql",
    )

    load_agg = SQLExecuteQueryOperator(
        task_id="load_agg_daily_item_locn_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/agg_daily_item_locn_sales.sql",
        split_statements=True,
    )

    @task(task_id="g09_agg_equals_fct")
    def g09_agg_equals_fct(ds: str) -> dict:
        """Gate G9: fail if AGG totals ≠ FCT totals for day D."""
        from pathlib import Path

        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        sql = Path(SQL_DIR, "gates/g09_agg_equals_fct.sql").read_text(encoding="utf-8")
        sql = (
            sql.replace("{{ var.json.medallion_config.database }}", cfg["database"])
            .replace("{{ var.json.medallion_config.schemas.gold }}", cfg["schemas"]["gold"])
            .replace("{{ ds }}", ds)
        )
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(sql)
        if rows:
            print(f"G9 FAIL: {rows}")
            raise AirflowFailException(f"G9 failed: AGG ≠ FCT for {ds}: {rows}")
        print(f"G9 OK: AGG matches FCT for {ds}")
        return {"ds": ds, "ok": True}

    write_recon = SQLExecuteQueryOperator(
        task_id="write_recon_daily_net_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/audit/recon_daily_net_sales.sql",
        split_statements=True,
    )

    @task(task_id="reconcile")
    def reconcile() -> bool:
        """G11 return flag: True if every VARIANCE_AMT is 0 (within 0.01)."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db, audit = cfg["database"], cfg["schemas"]["audit"]
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(
            f"""
            SELECT DAY_KY, SOURCE_NET_AMT, QUARANTINE_NET_AMT, FCT_NET_AMT, VARIANCE_AMT
            FROM {db}.{audit}.RECON_DAILY_NET_SALES
            ORDER BY DAY_KY
            """
        )
        print(f"G11 recon rows: {rows}")
        if not rows:
            raise AirflowFailException("G11: RECON_DAILY_NET_SALES is empty")
        bad = [r for r in rows if abs(float(r[4])) > 0.01]
        ok = len(bad) == 0
        print(f"G11 {'OK' if ok else 'FAIL'}: bad_rows={bad}")
        return ok

    @task.branch(task_id="branch_recon")
    def branch_recon(ok: bool) -> str:
        return "recon_passed" if ok else "send_alert"

    @task(task_id="recon_passed")
    def recon_passed() -> str:
        print("G11 passed — all daily variances are 0")
        return "ok"

    send_alert = EmailOperator(
        task_id="send_alert",
        conn_id=SMTP_CONN_ID,
        to="{{ var.json.medallion_config.alert_email }}",
        from_email="airflow@medallion.local",
        subject="[medallion] G11 reconciliation failed",
        html_content=(
            "<p>Gate G11 found non-zero VARIANCE_AMT in "
            "AUDIT.RECON_DAILY_NET_SALES. Check Snowsight.</p>"
        ),
    )

    @task(task_id="fail_run")
    def fail_run() -> None:
        raise AirflowFailException("G11 reconciliation failed after alert")

    g09 = g09_agg_equals_fct()
    ok = reconcile()
    branch = branch_recon(ok)
    passed = recon_passed()
    fail = fail_run()

    # Creates → AGG for day D → G9 → G11 recon across all STG days → return-flag branch
    [create_agg, create_vw, create_recon] >> load_agg >> g09 >> write_recon >> ok >> branch
    branch >> passed
    branch >> send_alert >> fail


agg_recon()

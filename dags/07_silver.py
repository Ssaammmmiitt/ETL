"""Lesson 7: Silver — typed TMP tables, quarantine gates G3–G6 + G12, identity check.

Expects Bronze already loaded for {{ ds }} (run 05_bronze_load first with complex files).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task

from include.medallion.config import SNOWFLAKE_CONN_ID, SQL_DIR

TMP_DDL = [
    "ddl/silver/tmp_quarantine.sql",
    "ddl/silver/tmp_sales_line.sql",
    "ddl/silver/tmp_customer.sql",
    "ddl/silver/tmp_item_loc_snapshot.sql",
    "ddl/silver/tmp_item_hier.sql",
    "ddl/silver/tmp_locn_hier.sql",
]


@dag(
    dag_id="07_silver",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "07"],
)
def silver():
    prev = None
    for path in TMP_DDL:
        name = path.rsplit("/", 1)[-1].replace(".sql", "")
        create = SQLExecuteQueryOperator(
            task_id=f"create_{name}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=path,
        )
        if prev is not None:
            prev >> create
        prev = create

    load_sales = SQLExecuteQueryOperator(
        task_id="load_tmp_sales_line",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_sales_line.sql",
        split_statements=True,
    )
    load_customer = SQLExecuteQueryOperator(
        task_id="load_tmp_customer",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_customer.sql",
        split_statements=True,
    )
    load_snapshot = SQLExecuteQueryOperator(
        task_id="load_tmp_item_loc_snapshot",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_item_loc_snapshot.sql",
        split_statements=True,
    )
    load_item_hier = SQLExecuteQueryOperator(
        task_id="load_tmp_item_hier",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_item_hier.sql",
    )
    load_locn_hier = SQLExecuteQueryOperator(
        task_id="load_tmp_locn_hier",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_locn_hier.sql",
    )

    @task(task_id="g06b_alert_reserved")
    def g06b_alert_reserved(ds: str) -> int:
        """Gate G6b: alert only when RESERVED > ON_HAND. Does not fail or quarantine."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        from include.medallion.config import CONFIG_VARIABLE

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db, silver = cfg["database"], cfg["schemas"]["silver"]
        sql = f"""
            SELECT SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, AVAILABLE_QTY
            FROM {db}.{silver}.TMP_ITEM_LOC_SNAPSHOT
            WHERE _INGEST_DT = %s AND RESERVED_QTY > ON_HAND_QTY
            ORDER BY ITEM_ID, LOCN_ID
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(sql, parameters=(ds,))
        if rows:
            print(f"G6b ALERT: {len(rows)} row(s) with RESERVED_QTY > ON_HAND_QTY")
            for r in rows[:10]:
                print(f"  {r}")
        else:
            print("G6b OK: no RESERVED > ON_HAND rows")
        return len(rows)

    @task(task_id="assert_sales_identity")
    def assert_sales_identity(ds: str) -> dict:
        """STG sales rows for D must equal TMP sales + quarantined sales rows."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        from include.medallion.config import CONFIG_VARIABLE

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db = cfg["database"]
        bronze, silver = cfg["schemas"]["bronze"], cfg["schemas"]["silver"]
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM {db}.{bronze}.STG_SALES WHERE _INGEST_DT = %s),
                (SELECT COUNT(*) FROM {db}.{silver}.TMP_SALES_LINE WHERE _INGEST_DT = %s),
                (SELECT COUNT(*) FROM {db}.{silver}.TMP_QUARANTINE
                  WHERE INGEST_DT = %s AND OBJECT_NM = 'TMP_SALES_LINE')
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        stg_cnt, tmp_cnt, quar_cnt = hook.get_first(sql, parameters=(ds, ds, ds))
        print(f"identity: STG={stg_cnt} TMP={tmp_cnt} QUAR={quar_cnt}")
        if stg_cnt != tmp_cnt + quar_cnt:
            raise ValueError(
                f"identity failed for {ds}: STG {stg_cnt} != TMP {tmp_cnt} + QUAR {quar_cnt}"
            )
        return {"stg": stg_cnt, "tmp": tmp_cnt, "quarantine": quar_cnt}

    @task(task_id="quarantine_summary")
    def quarantine_summary(ds: str) -> list[dict]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        from include.medallion.config import CONFIG_VARIABLE

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db, silver = cfg["database"], cfg["schemas"]["silver"]
        sql = f"""
            SELECT GATE_ID, REASON_CD, OBJECT_NM, COUNT(*) AS CNT
            FROM {db}.{silver}.TMP_QUARANTINE
            WHERE INGEST_DT = %s
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(sql, parameters=(ds,))
        out = [
            {"gate": g, "reason": r, "object": o, "count": c}
            for g, r, o, c in rows
        ]
        print(f"quarantine summary for {ds}: {out}")
        return out

    g06b = g06b_alert_reserved()
    identity = assert_sales_identity()
    summary = quarantine_summary()

    prev >> load_sales >> load_customer
    load_sales >> load_snapshot >> g06b
    [load_customer, g06b] >> load_item_hier >> load_locn_hier >> identity >> summary


silver()

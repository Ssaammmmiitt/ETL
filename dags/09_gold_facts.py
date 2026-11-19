"""Lesson 9: Gold facts — FCT_SALES MERGE, FCT_INVENTORY soft-close, FCT_PRICE periods, G7/G8.

Expects dims loaded (08_gold_dims) and Silver TMP for {{ ds }} (07_silver).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task

from include.medallion.config import SNOWFLAKE_CONN_ID, SQL_DIR

DDL_FILES = [
    "ddl/gold/sequences.sql",  # ensures SEQ_*_KY for facts (dims already created them)
    "ddl/gold/fct_sales.sql",
    "ddl/gold/fct_inventory.sql",
    "ddl/gold/fct_price.sql",
]


@dag(
    dag_id="09_gold_facts",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "09"],
)
def gold_facts():
    prev = None
    for path in DDL_FILES:
        name = path.rsplit("/", 1)[-1].replace(".sql", "")
        create = SQLExecuteQueryOperator(
            task_id=f"create_{name}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=path,
        )
        if prev is not None:
            prev >> create
        prev = create

    @task(task_id="g08_inventory_count")
    def g08_inventory_count(ds: str) -> dict:
        """Gate G8: fail if today's inventory row count drifts >±20% vs prior loaded day."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        from include.medallion.config import CONFIG_VARIABLE

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db = cfg["database"]
        bronze, silver = cfg["schemas"]["bronze"], cfg["schemas"]["silver"]
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM {db}.{silver}.TMP_ITEM_LOC_SNAPSHOT
                  WHERE _INGEST_DT = %s) AS TODAY_CNT,
                (SELECT COUNT(*) FROM {db}.{bronze}.STG_INVENTORY
                  WHERE _INGEST_DT = (
                      SELECT MAX(_INGEST_DT) FROM {db}.{bronze}.STG_INVENTORY
                       WHERE _INGEST_DT < %s::DATE
                  )) AS PRIOR_CNT,
                (SELECT MAX(_INGEST_DT) FROM {db}.{bronze}.STG_INVENTORY
                  WHERE _INGEST_DT < %s::DATE) AS PRIOR_DT
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        today_cnt, prior_cnt, prior_dt = hook.get_first(sql, parameters=(ds, ds, ds))
        today_cnt = int(today_cnt or 0)
        prior_cnt = int(prior_cnt or 0)
        if prior_dt is None or prior_cnt == 0:
            print(f"G8 SKIP: no prior inventory day before {ds} (today={today_cnt})")
            return {"today": today_cnt, "prior": prior_cnt, "prior_dt": None, "skipped": True}

        ratio = abs(today_cnt - prior_cnt) / prior_cnt
        print(
            f"G8: today={today_cnt} prior={prior_cnt} ({prior_dt}) "
            f"drift={ratio:.1%} (limit 20%)"
        )
        if ratio > 0.20:
            raise ValueError(
                f"G8 failed: inventory count {today_cnt} vs prior {prior_cnt} "
                f"({prior_dt}) drift {ratio:.1%} > 20%"
            )
        return {
            "today": today_cnt,
            "prior": prior_cnt,
            "prior_dt": str(prior_dt),
            "drift": ratio,
            "skipped": False,
        }

    g07 = SQLExecuteQueryOperator(
        task_id="g07_keys_resolve",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="gates/g07_keys_resolve.sql",
        split_statements=True,
    )

    @task(task_id="g07_alert")
    def g07_alert(ds: str) -> int:
        """Gate G7 alert: print orphan count (quarantine already written). Does not fail."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from airflow.sdk import Variable

        from include.medallion.config import CONFIG_VARIABLE

        cfg = Variable.get(CONFIG_VARIABLE, deserialize_json=True)
        db, silver = cfg["database"], cfg["schemas"]["silver"]
        sql = f"""
            SELECT REASON_CD, COUNT(*) AS CNT
            FROM {db}.{silver}.TMP_QUARANTINE
            WHERE INGEST_DT = %s AND GATE_ID = 'G7' AND OBJECT_NM = 'FCT_SALES'
            GROUP BY 1 ORDER BY 1
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        rows = hook.get_records(sql, parameters=(ds,))
        total = sum(int(c) for _, c in rows) if rows else 0
        if total:
            print(f"G7 ALERT: {total} orphan sales line(s) quarantined")
            for reason, cnt in rows:
                print(f"  {reason}: {cnt}")
        else:
            print("G7 OK: all sales keys resolve")
        return total

    load_sales = SQLExecuteQueryOperator(
        task_id="load_fct_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_sales.sql",
    )
    load_inv = SQLExecuteQueryOperator(
        task_id="load_fct_inventory",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_inventory.sql",
        split_statements=True,
    )
    load_price = SQLExecuteQueryOperator(
        task_id="load_fct_price",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_price.sql",
        split_statements=True,
    )

    g08 = g08_inventory_count()
    alert = g07_alert()

    # create_* → G7 → alert → sales
    #         → G8 → inventory ‖ price
    prev >> g07 >> alert >> load_sales
    prev >> g08 >> [load_inv, load_price]


gold_facts()

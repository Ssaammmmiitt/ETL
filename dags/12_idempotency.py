"""Lesson 12: Prove idempotency — second apply of day D leaves counts and nets unchanged.

Captures a BEFORE fingerprint, re-applies Silver → dims → facts → AGG for {{ ds }},
captures AFTER, and fails if any metric moved.

Does not re-run Bronze (landing files are usually archived). See lesson for the full
reload-of-day-D policy including Bronze.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from airflow.exceptions import AirflowFailException

from include.medallion.config import CONFIG_VARIABLE, SNOWFLAKE_CONN_ID, SQL_DIR

METRIC_KEYS = [
    "DAY_KY",
    "STG_SALES_CNT",
    "STG_SALES_NET",
    "TMP_SALES_CNT",
    "TMP_SALES_NET",
    "QUAR_SALES_CNT",
    "FCT_SALES_CNT",
    "FCT_SALES_NET",
    "FCT_INV_CURRENT_CNT",
    "FCT_PRICE_CURRENT_CNT",
    "AGG_CNT",
    "AGG_NET",
    "DIM_LOCN_CURRENT_CNT",
    "DIM_ITEM_CURRENT_CNT",
]


def _cfg():
    from airflow.sdk import Variable

    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def _hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def _render(rel: str, cfg: dict, ds: str) -> str:
    text = Path(SQL_DIR, rel).read_text(encoding="utf-8")
    text = text.replace("{{ var.json.medallion_config.database }}", cfg["database"])
    for layer, key in (
        ("bronze", "bronze"),
        ("silver", "silver"),
        ("gold", "gold"),
        ("audit", "audit"),
    ):
        text = text.replace(
            f"{{{{ var.json.medallion_config.schemas.{layer} }}}}",
            cfg["schemas"][key],
        )
    return text.replace("{{ ds }}", ds)


def _fingerprint(ds: str) -> dict:
    cfg = _cfg()
    sql = _render("checks/idem_day_fingerprint.sql", cfg, ds)
    row = _hook().get_first(sql)
    if not row:
        raise AirflowFailException(f"Fingerprint query returned no row for {ds}")
    out = {}
    for i, key in enumerate(METRIC_KEYS):
        val = row[i]
        if val is None:
            out[key] = 0
        elif hasattr(val, "as_tuple"):  # Decimal
            out[key] = float(val)
        else:
            out[key] = int(val) if key.endswith("_CNT") or key == "DAY_KY" else float(val)
    return out


def _store_snapshot(ds: str, phase: str, metrics: dict) -> None:
    import json

    cfg = _cfg()
    db, audit = cfg["database"], cfg["schemas"]["audit"]
    day_ky = int(metrics["DAY_KY"])
    # json.dumps uses double quotes — safe inside single-quoted Snowflake string
    payload = json.dumps(metrics).replace("\\", "\\\\").replace("'", "\\'")
    _hook().run(
        f"""
        INSERT INTO {db}.{audit}.IDEM_DAY_SNAPSHOT (DAY_KY, PHASE, METRICS, SNAPSHOT_TS)
        SELECT {day_ky}, '{phase}', PARSE_JSON('{payload}'), CURRENT_TIMESTAMP()
        """
    )


@dag(
    dag_id="12_idempotency",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "12"],
)
def idempotency():
    create_snap = SQLExecuteQueryOperator(
        task_id="create_idem_day_snapshot",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="ddl/audit/idem_day_snapshot.sql",
    )

    @task(task_id="snapshot_before")
    def snapshot_before(ds: str) -> dict:
        metrics = _fingerprint(ds)
        print(f"BEFORE {ds}: {metrics}")
        _store_snapshot(ds, "BEFORE", metrics)
        return metrics

    # --- re-apply Silver day D (DELETE+INSERT / OVERWRITE — already idempotent) ---
    reload_tmp_sales = SQLExecuteQueryOperator(
        task_id="reload_tmp_sales_line",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_sales_line.sql",
        split_statements=True,
    )
    reload_tmp_customer = SQLExecuteQueryOperator(
        task_id="reload_tmp_customer",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_customer.sql",
        split_statements=True,
    )
    reload_tmp_snapshot = SQLExecuteQueryOperator(
        task_id="reload_tmp_item_loc_snapshot",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_item_loc_snapshot.sql",
        split_statements=True,
    )
    reload_tmp_item_hier = SQLExecuteQueryOperator(
        task_id="reload_tmp_item_hier",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_item_hier.sql",
    )
    reload_tmp_locn_hier = SQLExecuteQueryOperator(
        task_id="reload_tmp_locn_hier",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/silver/tmp_locn_hier.sql",
    )

    # --- re-apply Gold dims (NOT EXISTS / MERGE / SCD2 hash) ---
    reload_dept = SQLExecuteQueryOperator(
        task_id="reload_dim_department",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_department.sql",
        split_statements=True,
    )
    reload_cat = SQLExecuteQueryOperator(
        task_id="reload_dim_category",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_category.sql",
        split_statements=True,
    )
    reload_sub = SQLExecuteQueryOperator(
        task_id="reload_dim_subcategory",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_subcategory.sql",
        split_statements=True,
    )
    reload_item = SQLExecuteQueryOperator(
        task_id="reload_dim_item",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_item.sql",
        split_statements=True,
    )
    reload_div = SQLExecuteQueryOperator(
        task_id="reload_dim_division",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_division.sql",
        split_statements=True,
    )
    reload_reg = SQLExecuteQueryOperator(
        task_id="reload_dim_region",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_region.sql",
        split_statements=True,
    )
    reload_city = SQLExecuteQueryOperator(
        task_id="reload_dim_city",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_city.sql",
        split_statements=True,
    )
    reload_locn = SQLExecuteQueryOperator(
        task_id="reload_dim_locn",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_locn.sql",
        split_statements=True,
    )
    reload_cust = SQLExecuteQueryOperator(
        task_id="reload_dim_customer",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/dim_customer.sql",
    )

    # --- re-apply facts + AGG ---
    reload_g07 = SQLExecuteQueryOperator(
        task_id="reload_g07_keys_resolve",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="gates/g07_keys_resolve.sql",
        split_statements=True,
    )
    reload_fct_sales = SQLExecuteQueryOperator(
        task_id="reload_fct_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_sales.sql",
    )
    reload_fct_inv = SQLExecuteQueryOperator(
        task_id="reload_fct_inventory",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_inventory.sql",
        split_statements=True,
    )
    reload_fct_price = SQLExecuteQueryOperator(
        task_id="reload_fct_price",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/fct_price.sql",
        split_statements=True,
    )
    reload_agg = SQLExecuteQueryOperator(
        task_id="reload_agg_daily_item_locn_sales",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="load/gold/agg_daily_item_locn_sales.sql",
        split_statements=True,
    )

    @task(task_id="snapshot_after")
    def snapshot_after(ds: str) -> dict:
        metrics = _fingerprint(ds)
        print(f"AFTER {ds}: {metrics}")
        _store_snapshot(ds, "AFTER", metrics)
        return metrics

    @task(task_id="assert_idempotent")
    def assert_idempotent(before: dict, after: dict) -> dict:
        """Fail if any fingerprint metric changed (acceptance criterion 1)."""
        diffs = {}
        for key in METRIC_KEYS:
            b, a = before.get(key), after.get(key)
            if key.endswith("_NET") or key.endswith("_AMT"):
                same = abs(float(b) - float(a)) <= 0.01
            else:
                same = b == a
            if not same:
                diffs[key] = {"before": b, "after": a}
        if diffs:
            print(f"IDEM FAIL: {diffs}")
            raise AirflowFailException(f"Idempotency failed — metrics moved: {diffs}")
        print("IDEM OK: BEFORE == AFTER for all fingerprint metrics")
        return {"ok": True, "metrics": after}

    before = snapshot_before()
    after = snapshot_after()
    check = assert_idempotent(before, after)

    silver = [
        reload_tmp_sales,
        reload_tmp_customer,
        reload_tmp_snapshot,
        reload_tmp_item_hier,
        reload_tmp_locn_hier,
    ]

    create_snap >> before >> silver
    silver >> reload_dept >> reload_cat >> reload_sub >> reload_item
    silver >> reload_div >> reload_reg >> reload_city >> reload_locn
    [reload_item, reload_locn] >> reload_cust
    [reload_tmp_sales, reload_item, reload_locn] >> reload_g07 >> reload_fct_sales
    [reload_tmp_snapshot, reload_item, reload_locn] >> [reload_fct_inv, reload_fct_price]
    [reload_fct_sales, reload_fct_inv, reload_fct_price, reload_cust] >> reload_agg >> after >> check


idempotency()

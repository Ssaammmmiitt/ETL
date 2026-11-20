"""Helpers for retail_medallion_daily, evidence/run_queries.py and tests.

render_sql substitutes the same templates Airflow would ({{ ds }}, the medallion_config
Variable, params) so a SQL file can be run outside an operator, in one Snowflake session.
"""
from __future__ import annotations

import csv
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from include.medallion.config import (
    CONFIG_VARIABLE,
    SOURCE_CONTRACTS,
    SNOWFLAKE_CONN_ID,
    SQL_DIR,
)

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def _cfg() -> dict:
    from airflow.sdk import Variable

    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def _hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def render_sql(
    rel: str,
    *,
    ds: str,
    run_id: str = "medallion_daily",
    params: dict | None = None,
    cfg: dict | None = None,
) -> str:
    cfg = cfg or _cfg()
    text = Path(SQL_DIR, rel).read_text(encoding="utf-8")
    params = params or {}
    for key in ("database", "landing_path"):
        text = text.replace(f"{{{{ var.json.medallion_config.{key} }}}}", cfg[key])
    text = text.replace("{{ var.json.medallion_config.warehouse }}", cfg.get("warehouse", ""))
    text = text.replace("{{ var.json.medallion_config.seed_path }}", cfg.get("seed_path", ""))
    for name, schema in cfg["schemas"].items():
        text = text.replace(f"{{{{ var.json.medallion_config.schemas.{name} }}}}", schema)
    for key, val in (cfg.get("outlier_rules") or {}).items():
        text = text.replace(f"{{{{ var.json.medallion_config.outlier_rules.{key} }}}}", str(val))
    text = text.replace("{{ ds }}", ds).replace("{{ ds_nodash }}", ds.replace("-", ""))
    text = text.replace("{{ run_id }}", run_id)
    for key, val in params.items():
        text = text.replace(f"{{{{ params.{key} }}}}", str(val))
    text = text.replace("{{ params.get('overwrite', 'FALSE') }}", params.get("overwrite", "FALSE"))
    return text


def _split_statements(sql: str) -> list[str]:
    out = []
    for part in re.split(r";\s*\n", sql.strip()):
        s = part.strip().rstrip(";").strip()
        if any(ln.strip() and not ln.strip().startswith("--") for ln in s.splitlines()):
            out.append(s)
    return out


def run_sql_files(rels: list[str], *, ds: str, run_id: str = "medallion_daily", params: dict | None = None) -> None:
    """Run every statement of every file in ONE Snowflake session (TEMP tables survive)."""
    conn = _hook().get_conn()
    cur = conn.cursor()
    try:
        for rel in rels:
            for stmt in _split_statements(render_sql(rel, ds=ds, run_id=run_id, params=params)):
                print(f"SQL {rel}: {stmt[:120].replace(chr(10), ' ')}...")
                cur.execute(stmt)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def run_sql_file(rel: str, **kwargs) -> None:
    run_sql_files([rel], **kwargs)


def fetch(rel: str, *, ds: str, first: bool = False):
    """Records (or the first row) of a single-SELECT SQL file; gate files return rows only on failure."""
    sql = "\n".join(_split_statements(render_sql(rel, ds=ds)))
    return _hook().get_first(sql) if first else _hook().get_records(sql)


def ddl_files() -> list[str]:
    """Every CREATE ... IF NOT EXISTS file in dependency order: schemas, formats, stage, sequences first, view last."""
    root = Path(SQL_DIR)
    layers = ["bronze", "silver", "gold", "audit"]

    def order(p: Path) -> tuple:
        return (layers.index(p.parent.name), p.stem != "sequences", p.stem.startswith("vw_"), p.stem)

    ddl = sorted((p for layer in layers for p in (root / "ddl" / layer).glob("*.sql")), key=order)
    return ["setup/00_schemas.sql", "setup/01_file_formats.sql", "setup/02_stages.sql"] + [
        p.relative_to(root).as_posix() for p in ddl
    ]


def generate_day(ds: str) -> list[dict]:
    from generate_complex import generate_pipeline_days

    return generate_pipeline_days([date.fromisoformat(ds)], landing_dir=Path(_cfg()["landing_path"]))


def g2_check(source: str, ds: str) -> int:
    """G2: contracted field count on every row; G1 row count: at least one data row. Returns data rows."""
    contract = SOURCE_CONTRACTS[source]
    path = Path(_cfg()["landing_path"]) / f"{source}_{ds.replace('-', '')}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"G2: missing {path}")
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise ValueError(f"G2: empty {path.name}")
    expected = contract["columns"]
    data = rows[1:] if contract["has_header"] else rows
    bad = [i for i, r in enumerate(rows if contract["has_header"] else data, 1) if len(r) != expected]
    if bad:
        raise ValueError(f"G2: {path.name} rows {bad[:5]} != {expected} columns")
    if not data:
        raise ValueError(f"G1: {path.name} has no data rows")
    return len(data)


def rewind_gold_if_past_day(ds: str, run_id: str) -> str | None:
    """Reload of a past day: if Gold already holds a date after ds, rewind Gold to ds - 1. Returns the old high-water."""
    (high_water,) = fetch("checks/gold_high_water.sql", ds=ds, first=True)
    if high_water and ds < high_water:
        run_sql_file("load/gold/rewind_from_ds.sql", ds=ds, run_id=run_id)
        return high_water
    return None


def _rows_affected(value) -> int | None:
    """Row count from a task's return value: an int, or the first result row of a DML statement."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (list, tuple)) and value and isinstance(value[0], (list, tuple)):
        nums = [v for v in value[0] if isinstance(v, int) and not isinstance(v, bool)]
        return sum(nums) if nums else None
    return None


def log_task(context: dict, status: str) -> None:
    """Task callback: one C_BATCH_LOG row per attempt. Never raises, so logging can't fail the pipeline."""
    try:
        ti = context["ti"]
        end = datetime.now(timezone.utc)
        start = getattr(ti, "start_date", None)
        rows = None
        if status == "SUCCESS":
            rows = _rows_affected(ti.xcom_pull(task_ids=ti.task_id, map_indexes=ti.map_index))
        error = context.get("exception")
        _hook().run(
            render_sql("load/audit/c_batch_log.sql", ds=context["ds"]),
            parameters={
                "batch_id": ti.run_id,
                "business_dt": context["ds"],
                "task_id": ti.task_id,
                "map_index": ti.map_index if ti.map_index is not None else -1,
                "try_number": ti.try_number,
                "status": status,
                "start_ts": start.astimezone(timezone.utc).replace(tzinfo=None).isoformat() if start else None,
                "end_ts": end.replace(tzinfo=None).isoformat(),
                "elapsed_sec": round((end - start).total_seconds(), 1) if start else None,
                "rows_affected": rows,
                "failure_reason": str(error)[:2000] if error else None,
            },
            split_statements=False,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"C_BATCH_LOG write skipped: {exc}")


def log_success(context: dict) -> None:
    log_task(context, "SUCCESS")


def log_failure(context: dict) -> None:
    log_task(context, "FAILED")


def log_retry(context: dict) -> None:
    log_task(context, "UP_FOR_RETRY")


def list_stage(ds: str) -> list[tuple]:
    """G1: (name, size) of every file PUT to the stage folder for ds."""
    conn = _hook().get_conn()
    cur = conn.cursor()
    try:
        cur.execute(render_sql("checks/list_stage.sql", ds=ds).strip().rstrip(";"))
        cur.execute('SELECT "name", "size" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY 1')
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

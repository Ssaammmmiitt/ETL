"""Evidence collector — runs the evidence/sql/r*.sql queries and writes ONE Markdown report.

Run inside the Airflow container (it reads the medallion_config Variable and the
snowflake_medallion connection from Airflow, so nothing is hard-coded here):

    docker compose run --rm airflow-cli python /opt/airflow/evidence/run_queries.py report --from 2026-11-20 --to 2026-11-20
    docker compose run --rm airflow-cli python /opt/airflow/evidence/run_queries.py report --from 2026-11-10 --to 2026-11-14
    docker compose run --rm airflow-cli python /opt/airflow/evidence/run_queries.py snapshot --ds 2026-11-14 --label before
    docker compose run --rm airflow-cli python /opt/airflow/evidence/run_queries.py snapshot --ds 2026-11-14 --label after

The rerun proof must use the LATEST loaded day: SCD2 and the inventory/price soft-close are
order-dependent, so re-running an older day after newer ones changes "current" rows.
    docker compose run --rm airflow-cli python /opt/airflow/evidence/run_queries.py reset --yes

report    query-results/day_<ds>.md (one day) or backfill_<from>_to_<to>.md (range):
          verdict table, defects planted -> caught, layer flow, quarantine, SCD, final view.
snapshot  idempotency fingerprint for one day; "after" writes query-results/rerun_<ds>.md.
reset     truncates every data table (evidence/sql/00_reset.sql) and clears landing/archive CSVs.
"""
from __future__ import annotations

import json
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from include.medallion.config import CONFIG_VARIABLE, SNOWFLAKE_CONN_ID, SQL_DIR
from include.medallion.runner import render_sql

EVIDENCE_DIR = Path(__file__).resolve().parent
QUERY_DIR = EVIDENCE_DIR / "sql"
RESULTS_DIR = EVIDENCE_DIR / "query-results"

sys.path.insert(0, str(EVIDENCE_DIR.parent / "scripts"))
from generate_complex import SCD1_FROM, SCD2_FROM, SCD2_LOCN_ID, defect_bucket  # noqa: E402

SCD1_ITEM_ID = "100011"
PREMIUM_PRICE = "2499.99"
BUCKET_NAMES = {
    0: "Outliers kept + anomalies",
    1: "D1 null ITEM_ID x2",
    2: "D2 negative qty",
    3: "D3 orphan item 199999",
    4: "D4 duplicate line",
}


def load_cfg() -> dict:
    from airflow.models import Variable

    return json.loads(Variable.get(CONFIG_VARIABLE))


def connect():
    """Snowflake connection built from the Airflow connection (outside a task)."""
    from airflow.models.connection import Connection
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    hook.get_connection = lambda conn_id: Connection.get_connection_from_secrets(conn_id)
    return hook.get_conn()


def query(conn, sql: str) -> tuple[list[str], list[tuple]]:
    cur = conn.cursor()
    try:
        cur.execute(sql.strip().rstrip(";"))
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
        return cols, rows
    finally:
        cur.close()


def md_table(cols: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n\n_(0 rows)_\n"
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out) + "\n"


def days(from_ds: str, to_ds: str) -> list[date]:
    d, end, out = date.fromisoformat(from_ds), date.fromisoformat(to_ds), []
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def run_file(conn, cfg: dict, name: str, from_ds: str, to_ds: str) -> tuple[list[str], list[tuple]]:
    params = {
        "from_ds": from_ds,
        "to_ds": to_ds,
        "scd2_locn_id": SCD2_LOCN_ID,
        "scd1_item_id": SCD1_ITEM_ID,
    }
    return query(conn, render_sql(str(QUERY_DIR / name), ds=to_ds, params=params, cfg=cfg))


def defect_trace(conn, cfg: dict, from_ds: str, to_ds: str) -> list[tuple]:
    """Per day: planted defect, what Bronze holds, which gate caught it, PASS/CHECK."""
    db, s = cfg["database"], cfg["schemas"]
    stg = f"{db}.{s['bronze']}.STG_SALES"
    quar = f"{db}.{s['audit']}.C_REJ_TABLE"
    fct = f"{db}.{s['gold']}.FCT_SALES"

    def one(sql: str) -> int:
        return int(query(conn, sql)[1][0][0] or 0)

    def q_cnt(d: str, gate: str, reason: str | None = None, obj: str = "TMP_SALES_LINE") -> int:
        extra = f"AND REASON_CD = '{reason}'" if reason else ""
        return one(f"SELECT COUNT(*) FROM {quar} WHERE INGEST_DT = '{d}' AND OBJECT_NM = '{obj}' AND GATE_ID = '{gate}' {extra}")

    rows = []
    for day in days(from_ds, to_ds):
        d = day.isoformat()
        if one(f"SELECT COUNT(*) FROM {stg} WHERE _INGEST_DT = '{d}'") == 0:
            continue
        b = defect_bucket(day)
        if b == 0:
            caught = q_cnt(d, "G12")
            kept = one(f"SELECT COUNT(*) FROM {fct} WHERE SALE_DT = '{d}' AND UNIT_PRICE_AMT = {PREMIUM_PRICE}")
            gate = "G12 quarantines anomalies; valid outliers stay in Gold"
            ok = caught >= 3 and kept >= 2
            found = f"G12 quarantined={caught}, premium lines in Gold={kept}"
        elif b == 1:
            in_bronze = one(f"SELECT COUNT(*) FROM {stg} WHERE _INGEST_DT = '{d}' AND NULLIF(TRIM(ITEM_ID),'') IS NULL")
            caught = q_cnt(d, "G3", "NULL_ITEM_ID")
            gate, ok = "G3 NULL_ITEM_ID", in_bronze == 2 and caught == 2
            found = f"bronze={in_bronze}, quarantined={caught}"
        elif b == 2:
            in_bronze = one(f"SELECT COUNT(*) FROM {stg} WHERE _INGEST_DT = '{d}' AND TRY_TO_NUMBER(TRIM(SOLD_QTY),18,3) < 0")
            caught = q_cnt(d, "G6", "BAD_OR_NONPOSITIVE_QTY")
            gate, ok = "G6 BAD_OR_NONPOSITIVE_QTY", in_bronze == 1 and caught == 1
            found = f"bronze={in_bronze}, quarantined={caught}"
        elif b == 3:
            in_bronze = one(f"SELECT COUNT(*) FROM {stg} WHERE _INGEST_DT = '{d}' AND TRIM(ITEM_ID) = '199999'")
            caught = q_cnt(d, "G7", "ORPHAN_ITEM", obj="FCT_SALES")
            gate, ok = "G7 ORPHAN_ITEM (before FCT_SALES)", in_bronze == 1 and caught == 1
            found = f"bronze={in_bronze}, quarantined={caught}"
        else:
            in_bronze = one(f"SELECT COUNT(*) FROM (SELECT 1 FROM {stg} WHERE _INGEST_DT = '{d}' GROUP BY TRIM(TRANSACTION_ID), TRIM(LINE_NUM) HAVING COUNT(*) > 1)")
            caught = q_cnt(d, "G4", "DUPLICATE_TXN_LINE")
            gate, ok = "G4 DUPLICATE_TXN_LINE (latest kept)", in_bronze == 1 and caught == 1
            found = f"duplicated keys={in_bronze}, quarantined={caught}"
        rows.append((d, b, BUCKET_NAMES[b], gate, found, "PASS" if ok else "CHECK"))
    return rows


def pk_checks(conn, cfg: dict, to_ds: str) -> list[tuple]:
    rows = []
    for path in sorted(Path(SQL_DIR, "gates").glob("pk_*.sql")):
        _, bad = query(conn, render_sql(f"gates/{path.name}", ds=to_ds, cfg=cfg))
        rows.append((path.stem, len(bad)))
    return rows


def scd_verdicts(scd_rows: list[tuple], from_ds: str, to_ds: str) -> list[tuple]:
    locn = [r for r in scd_rows if r[0].startswith("DIM_LOCN")]
    item = [r for r in scd_rows if r[0].startswith("DIM_ITEM")]
    start, end = date.fromisoformat(from_ds), date.fromisoformat(to_ds)
    current_city = next((r[2] for r in locn if r[5]), None)
    if start < SCD2_FROM <= end:
        ok = len(locn) == 2 and sum(1 for r in locn if r[5]) == 1
        scd2 = ("SCD2 store move (DIM_LOCN)", f"{len(locn)} versions, current city {current_city}",
                "2 versions, 1 current", "PASS" if ok else "FAIL")
    else:
        scd2 = ("SCD2 store move (DIM_LOCN)", f"{len(locn)} version(s), current city {current_city}",
                f"history needs a range spanning {SCD2_FROM}", "INFO")
    want_rev = end >= SCD1_FROM
    has_rev = any("(rev Nov)" in str(r[2]) for r in item)
    ok = len(item) == 1 and has_rev == want_rev
    scd1 = ("SCD1 item description (DIM_ITEM)", f"{len(item)} row, desc revised={has_rev}",
            f"1 row, revised={want_rev}", "PASS" if ok else "FAIL")
    return [scd2, scd1]


def report(from_ds: str, to_ds: str, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    cfg = load_cfg()
    conn = connect()
    try:
        trace = defect_trace(conn, cfg, from_ds, to_ds)
        flow_cols, flow = run_file(conn, cfg, "r1_layer_flow.sql", from_ds, to_ds)
        quar_cols, quar = run_file(conn, cfg, "r2_quarantine_by_gate.sql", from_ds, to_ds)
        scd_cols, scd = run_file(conn, cfg, "r3_scd_history.sql", from_ds, to_ds)
        view_cols, view = run_file(conn, cfg, "r4_final_view_by_region.sql", from_ds, to_ds)
        batch_cols, batches = run_file(conn, cfg, "r5_control_batches.sql", from_ds, to_ds)
        load_cols, loads = run_file(conn, cfg, "r6_load_dates.sql", from_ds, to_ds)
        pks = pk_checks(conn, cfg, to_ds)
    finally:
        conn.close()

    expected_days = len(days(from_ds, to_ds))
    f = {c: i for i, c in enumerate(flow_cols)}
    checks = [
        ("Days loaded", f"{len(flow)} of {expected_days}", f"{expected_days}",
         "PASS" if len(flow) == expected_days else "FAIL"),
        ("Planted defects caught", f"{sum(r[5] == 'PASS' for r in trace)} of {len(trace)}", "all",
         "PASS" if trace and all(r[5] == "PASS" for r in trace) else "FAIL"),
        ("Rows: source = Gold + quarantine", f"max abs diff {max((abs(r[f['ROW_DIFF']]) for r in flow), default=0)}", "0",
         "PASS" if flow and all(r[f["ROW_DIFF"]] == 0 for r in flow) else "FAIL"),
        ("G11 reconciliation variance", f"max abs variance {max((abs(r[f['G11_VARIANCE']] or 0) for r in flow), default=0)}", "0.00",
         "PASS" if flow and all((r[f["G11_VARIANCE"]] or 0) == 0 for r in flow) else "FAIL"),
        ("G9 aggregate = fact", f"max abs delta {max((abs(r[f['G9_DELTA']]) for r in flow), default=0)}", "0.00",
         "PASS" if flow and all(r[f["G9_DELTA"]] == 0 for r in flow) else "FAIL"),
        ("Primary keys unique", f"{sum(1 for _, n in pks if n == 0)} of {len(pks)} tables clean", "all",
         "PASS" if all(n == 0 for _, n in pks) else "FAIL"),
        *scd_verdicts(scd, from_ds, to_ds),
    ]
    b = {c: i for i, c in enumerate(batch_cols)}
    logged_days = {str(r[b["BUSINESS_DT"]]) for r in batches if r[b["PARAMS"]]}
    gold_dt = max((str(r[2]) for r in loads if r[1] == "GOLD"), default="none")
    checks.append(("Control tables written", f"{len(logged_days)} day(s) in C_BATCH_LOG + C_PARAM; Gold LAST_LOAD_DT {gold_dt}",
                   f"{expected_days} day(s); >= {to_ds}",
                   "PASS" if len(logged_days) == expected_days and gold_dt >= to_ds else "FAIL"))

    single = from_ds == to_ds
    title = f"Evidence — single day {from_ds}" if single else f"Evidence — backfill {from_ds} to {to_ds}"
    name = f"day_{from_ds}.md" if single else f"backfill_{from_ds}_to_{to_ds}.md"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    pk_bad = [p for p, n in pks if n]

    text = [
        f"# {title}\n\n",
        f"Generated {ts} by `evidence/run_queries.py report`. Queries: `evidence/sql/r1`–`r6` "
        f"and `include/sql/gates/pk_*.sql`.\n\n",
        "## Verdict\n\n",
        md_table(["CHECK", "FOUND", "EXPECTED", "STATUS"], checks),
        "\n## 1. Planted defects → where they were caught\n\n",
        "Bucket = `date.toordinal() % 5` (`scripts/generate_complex.py`). "
        "Full log: `data/store/defect_log.txt`.\n\n",
        md_table(["DATE", "BUCKET", "PLANTED", "CAUGHT BY", "FOUND", "STATUS"], trace),
        "\n## 2. Layer flow and reconciliation (`r1_layer_flow.sql`)\n\n",
        "`RUN_ID` is the Airflow run that loaded the day. "
        "`ROW_DIFF` = Bronze − Gold − quarantine.\n\n",
        md_table(flow_cols, flow),
        "\n## 3. Rejected rows by rule (`C_REJ_TABLE`, `r2_quarantine_by_gate.sql`)\n\n",
        md_table(quar_cols, quar),
        "\n## 4. Slowly changing dimensions (`r3_scd_history.sql`)\n\n",
        md_table(scd_cols, scd),
        "\n## 5. Final view by region (`r4_final_view_by_region.sql`)\n\n",
        md_table(view_cols, view),
        "\n## 6. Control tables (`r5_control_batches.sql`, `r6_load_dates.sql`)\n\n",
        "One row per batch from `C_BATCH_LOG` (with `C_PARAM` and `C_REJ_TABLE` counts); "
        "per-task detail is in `C_BATCH_LOG` itself.\n\n",
        md_table(batch_cols, batches),
        "\n",
        md_table(load_cols, loads),
        f"\nPK checks: {len(pks)} `pk_*.sql` gates, "
        + ("all returned 0 duplicate keys.\n" if not pk_bad else f"duplicates in {', '.join(pk_bad)}.\n"),
    ]
    path = out / name
    path.write_text("".join(text), encoding="utf-8")
    print(f"wrote {path}")
    for c in checks:
        print(f"  {c[3]:5} {c[0]}: {c[1]}")
    if not flow:
        print("  No Bronze rows for this range: check the run's logical date in the UI (it must be <date>T00:00:00+00:00).")


def snapshot(ds: str, label: str, out: Path) -> None:
    snaps = out / "_snapshots"
    snaps.mkdir(parents=True, exist_ok=True)
    cfg = load_cfg()
    conn = connect()
    try:
        cols, rows = query(conn, render_sql("checks/idem_day_fingerprint.sql", ds=ds, cfg=cfg))
    finally:
        conn.close()
    snap = {c: (None if v is None else str(v)) for c, v in zip(cols, rows[0])}
    (snaps / f"rerun_{ds}_{label}.json").write_text(json.dumps(snap, indent=2), encoding="utf-8")
    print(f"saved {label} snapshot for {ds}")
    before_path = snaps / f"rerun_{ds}_before.json"
    if label == "after" and before_path.is_file():
        before = json.loads(before_path.read_text(encoding="utf-8"))
        table = [(k, before.get(k), snap.get(k), "yes" if before.get(k) == snap.get(k) else "NO") for k in snap]
        same = all(r[3] == "yes" for r in table)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
        text = f"# Evidence — rerun of {ds} (idempotency)\n\nGenerated {ts}. "
        text += "Fingerprint: `include/sql/checks/idem_day_fingerprint.sql`, captured before and after a second run of the same date.\n\n"
        text += f"**Result: {'IDENTICAL — rerun changed nothing' if same else 'DIFFERENT — investigate'}**\n\n"
        text += md_table(["METRIC", "BEFORE", "AFTER", "MATCH"], table)
        path = out / f"rerun_{ds}.md"
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path} ({'identical' if same else 'DIFFERENT'})")


def reset() -> None:
    cfg = load_cfg()
    conn = connect()
    try:
        from include.medallion.runner import _split_statements

        sql = render_sql(str(QUERY_DIR / "00_reset.sql"), ds="2026-01-01", cfg=cfg)
        for stmt in _split_statements(sql):
            print(f"RESET: {stmt.splitlines()[0][:110]}")
            query(conn, stmt)
    finally:
        conn.close()
    for key in ("landing_path", "archive_path"):
        folder = Path(cfg[key])
        for p in folder.glob("*.csv"):
            p.unlink()
        for p in folder.iterdir() if folder.is_dir() else []:
            if p.is_dir():
                shutil.rmtree(p)
        print(f"cleared CSVs under {folder}")


def main(argv: list[str]) -> None:
    def arg(name: str, default: str | None = None) -> str | None:
        return argv[argv.index(name) + 1] if name in argv else default

    cmd = argv[0] if argv else ""
    out = Path(arg("--out", str(RESULTS_DIR)))
    if cmd == "report":
        report(arg("--from"), arg("--to"), out)
    elif cmd == "snapshot":
        snapshot(arg("--ds"), arg("--label"), out)
    elif cmd == "reset" and "--yes" in argv:
        reset()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])

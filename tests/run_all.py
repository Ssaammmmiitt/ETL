"""All project tests in one command (plain asserts, no pytest).

    docker compose run --rm airflow-cli python /opt/airflow/tests/run_all.py

DAG tests:   imports cleanly (no cycles), retries >= 1, sensor + mapping, backfill settings,
             every referenced SQL file exists, one load file per table, C_BATCH_SCRIPTS
             matches the DAG, no hard-coding.
SQL tests:   no Jinja inside SQL comments.
Data tests:  generator is deterministic per date; each planted defect lands on its day;
             files meet the G2 column contracts.

Output is printed and saved to evidence/query-results/tests.txt. Exit code 1 if any test fails.
None of these tests touch Snowflake; the data checks on loaded tables live in evidence/sql.
"""
from __future__ import annotations

import csv
import hashlib
import re
import sys
import tempfile
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from include.medallion.config import SOURCE_CONTRACTS, SOURCES, SQL_DIR  # noqa: E402

SUBMISSION_DAGS = ["retail_medallion_daily"]
HARDCODE_PATTERNS = [
    "snowflakecomputing", "password", "PRIVATE KEY", "/opt/airflow",
    r"MEDALLION_(DB|WH|ROLE)", r"(BRONZE|SILVER|GOLD|AUDIT)\.",
    r"(STG|TMP|DIM|FCT|AGG|VW)_", "MY_INT_STAGE",
]
WINDOW = [date(2026, 11, 10) + timedelta(days=i) for i in range(5)]

results: list[tuple[str, str, str]] = []
_dagbag = None


def test(fn):
    try:
        detail = fn() or ""
        results.append(("PASS", fn.__name__, detail))
    except AssertionError as e:
        results.append(("FAIL", fn.__name__, str(e)))
    except Exception:
        results.append(("FAIL", fn.__name__, traceback.format_exc(limit=2).strip().splitlines()[-1]))
    return fn


def dagbag():
    global _dagbag
    if _dagbag is None:
        try:
            from airflow.dag_processing.dagbag import DagBag
        except ImportError:
            from airflow.models.dagbag import DagBag
        _dagbag = DagBag(dag_folder=str(ROOT / "dags"))
    return _dagbag


# ── DAG tests ──────────────────────────────────────────────────────────

@test
def dag_imports_without_errors():
    bag = dagbag()
    assert not bag.import_errors, f"import errors: {list(bag.import_errors)}"
    missing = [d for d in SUBMISSION_DAGS if d not in bag.dags]
    assert not missing, f"missing DAGs: {missing}"
    return f"{len(bag.dags)} DAGs parsed, 0 import errors"


@test
def dag_has_no_cycles():
    for dag_id in SUBMISSION_DAGS:
        dag = dagbag().dags[dag_id]
        state: dict[str, int] = {}

        def visit(tid: str) -> None:
            assert state.get(tid) != 1, f"{dag_id}: cycle through {tid}"
            if state.get(tid) == 2:
                return
            state[tid] = 1
            for child in dag.task_dict[tid].downstream_task_ids:
                visit(child)
            state[tid] = 2

        for tid in dag.task_dict:
            visit(tid)
    return "depth-first walk found no back edges"


@test
def every_task_has_retries():
    bad = []
    for dag_id in SUBMISSION_DAGS:
        for task in dagbag().dags[dag_id].tasks:
            retries = getattr(task, "retries", None)
            if retries is None and hasattr(task, "partial_kwargs"):
                retries = task.partial_kwargs.get("retries")
            if retries is None or int(retries) < 1:
                bad.append(f"{dag_id}.{task.task_id}={retries}")
    assert not bad, f"retries < 1: {bad}"
    return "all tasks retries >= 1"


@test
def sensor_and_mapped_ingest():
    dag = dagbag().dags["retail_medallion_daily"]
    for tid in ("wait_for_files", "bronze.ingest_one"):
        task = dag.task_dict[tid]
        assert type(task).__name__.endswith("MappedOperator"), f"{tid} is not mapped ({type(task).__name__})"
    sensor = dag.task_dict["wait_for_files"]
    kw = getattr(sensor, "partial_kwargs", {})
    assert kw.get("mode") == "reschedule", f"sensor mode={kw.get('mode')}"
    assert kw.get("timeout"), "sensor has no timeout"
    return f"wait_for_files and bronze.ingest_one mapped; sensor mode=reschedule timeout={kw.get('timeout')}"


@test
def backfill_settings():
    dag = dagbag().dags["retail_medallion_daily"]
    assert dag.max_active_runs == 1, f"max_active_runs={dag.max_active_runs}"
    assert dag.catchup is False, "catchup should be False"
    assert str(getattr(dag, "timetable", "")) and "NullTimetable" not in type(dag.timetable).__name__, \
        "schedule=None cannot be backfilled in Airflow 3"
    return f"max_active_runs=1, catchup=False, timetable={type(dag.timetable).__name__}"


def referenced_sql() -> set[str]:
    refs: set[str] = set()
    sources = [ROOT / "dags" / f"{d}.py" for d in SUBMISSION_DAGS] + [ROOT / "include" / "medallion" / "runner.py"]
    for path in sources:
        for m in re.finditer(r"""["']([\w/{}.]+\.sql)["']""", path.read_text(encoding="utf-8")):
            rel = m.group(1)
            if "{source}" in rel:
                refs.update(rel.replace("{source}", s) for s in SOURCES)
            elif "{" not in rel:
                refs.add(rel)
    return refs


@test
def referenced_sql_files_exist():
    refs = referenced_sql()
    missing = sorted(r for r in refs if not Path(SQL_DIR, r).is_file())
    assert not missing, f"missing SQL files: {missing}"
    return f"{len(refs)} referenced SQL files all exist"


@test
def every_sql_file_is_used():
    """No dead SQL: each file is referenced by the DAG/runner/evidence, or is DDL, a pk_* check or the manual bootstrap."""
    refs = referenced_sql()
    text = (ROOT / "evidence" / "run_queries.py").read_text(encoding="utf-8")
    refs |= set(re.findall(r"""["']([\w/]+\.sql)["']""", text))
    unused = []
    for path in sorted(Path(SQL_DIR).rglob("*.sql")):
        rel = path.relative_to(SQL_DIR).as_posix()
        if rel.startswith(("ddl/", "setup/", "gates/pk_")) or rel in refs:
            continue
        unused.append(rel)
    assert not unused, f"unused SQL files: {unused}"
    return f"{sum(1 for _ in Path(SQL_DIR).rglob('*.sql'))} SQL files, all used"


@test
def one_load_file_per_table():
    no_load = {"sequences", "vw_daily_item_locn_sales", "c_rej_table"}
    missing = []
    for ddl in sorted(Path(SQL_DIR, "ddl").glob("*/*.sql")):
        if ddl.stem in no_load:
            continue
        if not Path(SQL_DIR, "load", ddl.parent.name, ddl.name).is_file():
            missing.append(f"{ddl.parent.name}/{ddl.name}")
    assert not missing, f"tables without a load file: {missing}"
    return "every table DDL has a matching load/<layer>/<table>.sql"


@test
def batch_scripts_registry_matches_dag():
    """C_BATCH_SCRIPTS seed lists exactly the DAG's tasks, and RUN_SEQ follows the dependencies."""
    seed = Path(SQL_DIR, "load", "audit", "c_batch_scripts.sql").read_text(encoding="utf-8")
    seq = {tid: int(n) for n, tid in re.findall(r"\(\s*(\d+),\s*'([\w.]+)'", seed)}
    dag = dagbag().dags["retail_medallion_daily"]
    missing, extra = sorted(set(dag.task_dict) - set(seq)), sorted(set(seq) - set(dag.task_dict))
    assert not missing and not extra, f"not in seed: {missing}; not in DAG: {extra}"
    out_of_order = [f"{t.task_id}>{d}" for t in dag.tasks for d in t.downstream_task_ids if seq[t.task_id] >= seq[d]]
    assert not out_of_order, f"RUN_SEQ against dependencies: {out_of_order}"
    return f"{len(seq)} tasks registered, RUN_SEQ consistent with the graph"


@test
def no_hardcoding_in_submission_dags():
    hits = []
    files = [ROOT / "dags" / f"{d}.py" for d in SUBMISSION_DAGS] + [ROOT / "include" / "medallion" / "runner.py"]
    for path in files:
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if any(re.search(pat, line) for pat in HARDCODE_PATTERNS):
                hits.append(f"{path.name}:{n}: {line.strip()}")
    assert not hits, "\n".join(hits)
    return f"0 hits in {[p.name for p in files]}"


# ── SQL tests ──────────────────────────────────────────────────────────

@test
def no_jinja_in_sql_comments():
    bad = []
    for path in Path(SQL_DIR).rglob("*.sql"):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if line.strip().startswith("--") and "{{" in line:
                bad.append(f"{path.relative_to(SQL_DIR)}:{n}")
    assert not bad, f"Jinja inside comments: {bad}"
    return "no {{ }} inside -- comments"


# ── Data tests (generator, no Snowflake) ───────────────────────────────

def generate(days: list[date], out: Path) -> None:
    import contextlib
    import io

    import generate_complex as g

    with contextlib.redirect_stdout(io.StringIO()):
        g.generate_pipeline_days(days, landing_dir=out, defect_log_path=out / "defect_log.txt")


def read_csv(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


@test
def generator_is_deterministic_per_date():
    a, b = Path(tempfile.mkdtemp()), Path(tempfile.mkdtemp())
    generate(WINDOW, a)
    for d in WINDOW:
        generate([d], b)
    diff = [
        f.name for f in sorted(a.glob("*.csv"))
        if hashlib.md5(f.read_bytes()).digest() != hashlib.md5((b / f.name).read_bytes()).digest()
    ]
    assert not diff, f"single-day vs window differ: {diff}"
    return f"{len(list(a.glob('*.csv')))} files identical (one-day vs five-day generate)"


@test
def five_consecutive_days_cover_every_defect():
    import generate_complex as g

    for start in range(0, 60):
        days = [date(2026, 11, 1) + timedelta(days=start + i) for i in range(5)]
        assert {g.defect_bucket(d) for d in days} == {0, 1, 2, 3, 4}, f"window from {days[0]} misses a bucket"
    return "any 5 consecutive days contain buckets 0-4"


@test
def planted_defects_land_on_their_day():
    import generate_complex as g

    out = Path(tempfile.mkdtemp())
    generate(WINDOW, out)
    notes = []
    for d in WINDOW:
        rows = read_csv(out / f"sales_{d:%Y%m%d}.csv")[1:]
        nulls = sum(1 for r in rows if r[4] == "")
        neg = sum(1 for r in rows if float(r[10]) < 0)
        orphan = sum(1 for r in rows if r[4] == g.ORPHAN_ITEM_ID)
        keys: dict[tuple, int] = {}
        for r in rows:
            keys[(r[0], r[1])] = keys.get((r[0], r[1]), 0) + 1
        dups = sum(1 for c in keys.values() if c > 1)
        b = g.defect_bucket(d)
        expect = {"nulls": 2 if b == 1 else 0, "neg": 1 if b == 2 else 0,
                  "orphan": 1 if b == 3 else 0, "dups": 1 if b == 4 else 0}
        got = {"nulls": nulls, "neg": neg, "orphan": orphan, "dups": dups}
        assert got == expect, f"{d} bucket {b}: expected {expect}, got {got}"
        notes.append(f"{d:%m-%d}:b{b}")

    def city(day: date) -> str:
        rows = read_csv(out / f"store_master_{day:%Y%m%d}.csv")[1:]
        return next(r[5] for r in rows if r[0] == g.SCD2_LOCN_ID)

    assert city(date(2026, 11, 12)) != city(date(2026, 11, 13)), "SCD2 city move missing on 2026-11-13"
    assert city(date(2026, 11, 13)) == city(date(2026, 11, 14)), "SCD2 move must persist"

    def desc(day: date) -> str:
        return read_csv(out / f"item_master_{day:%Y%m%d}.csv")[11][2]

    assert desc(date(2026, 11, 13)) != desc(date(2026, 11, 14)), "SCD1 description change missing on 2026-11-14"
    return "defects per day OK (" + ", ".join(notes) + "); SCD2 move 11-13; SCD1 change 11-14"


@test
def files_meet_g2_contracts():
    out = Path(tempfile.mkdtemp())
    generate(WINDOW, out)
    for d in WINDOW:
        for source in SOURCES:
            contract = SOURCE_CONTRACTS[source]
            rows = read_csv(out / f"{source}_{d:%Y%m%d}.csv")
            bad = [i for i, r in enumerate(rows, 1) if len(r) != contract["columns"]]
            assert not bad, f"{source} {d}: rows {bad[:5]} != {contract['columns']} columns"
    return f"{len(WINDOW) * len(SOURCES)} files match SOURCE_CONTRACTS"


# ── report ─────────────────────────────────────────────────────────────

def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = [f"tests/run_all.py · {ts}", ""]
    lines += [f"{status:4}  {name}: {detail}" for status, name, detail in results]
    failed = sum(1 for s, _, _ in results if s == "FAIL")
    lines += ["", f"{len(results) - failed} passed, {failed} failed"]
    text = "\n".join(lines)
    print(text)
    out = ROOT / "evidence" / "query-results" / "tests.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text + "\n", encoding="utf-8")
    print(f"\nsaved {out}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

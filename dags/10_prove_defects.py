"""Lesson 10: Prove planted defects D1–D5, PK checker on all Gold tables, gate G10.

Prerequisite: Bronze → Silver → dims → facts for 2026-11-10, 11, 12, 13 (in date order).
Silver TMP DDL is IF NOT EXISTS so quarantine rows accumulate across days.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

from include.medallion.config import CONFIG_VARIABLE, SNOWFLAKE_CONN_ID, SQL_DIR

PK_SQL_FILES = [
    "gates/pk_dim_day.sql",
    "gates/pk_dim_customer.sql",
    "gates/pk_dim_department.sql",
    "gates/pk_dim_category.sql",
    "gates/pk_dim_subcategory.sql",
    "gates/pk_dim_item.sql",
    "gates/pk_dim_division.sql",
    "gates/pk_dim_region.sql",
    "gates/pk_dim_city.sql",
    "gates/pk_dim_locn.sql",
    "gates/pk_fct_sales.sql",
    "gates/pk_fct_inventory.sql",
    "gates/pk_fct_price.sql",
]


def _cfg():
    from airflow.sdk import Variable

    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def _render(rel: str, cfg: dict, ds: str | None = None) -> str:
    text = Path(SQL_DIR, rel).read_text(encoding="utf-8")
    text = text.replace("{{ var.json.medallion_config.database }}", cfg["database"])
    text = text.replace(
        "{{ var.json.medallion_config.schemas.gold }}", cfg["schemas"]["gold"]
    )
    text = text.replace(
        "{{ var.json.medallion_config.schemas.silver }}", cfg["schemas"]["silver"]
    )
    text = text.replace(
        "{{ var.json.medallion_config.schemas.bronze }}", cfg["schemas"]["bronze"]
    )
    if ds is not None:
        text = text.replace("{{ ds }}", ds)
    return text


def _hook():
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


@dag(
    dag_id="10_prove_defects",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    default_args={"retries": 0},
    tags=["medallion", "10"],
)
def prove_defects():
    @task(task_id="prove_d1_d2_d4")
    def prove_d1_d2_d4() -> dict:
        """Nov 11: G3 null ITEM×2, G6 bad qty×1, G4 duplicate×1."""
        cfg = _cfg()
        db, silver = cfg["database"], cfg["schemas"]["silver"]
        hook = _hook()
        sql = f"""
            SELECT GATE_ID, REASON_CD, COUNT(*) AS CNT
            FROM {db}.{silver}.TMP_QUARANTINE
            WHERE INGEST_DT = '2026-11-11'
              AND OBJECT_NM = 'TMP_SALES_LINE'
              AND GATE_ID IN ('G3', 'G4', 'G6')
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        rows = {f"{g}:{r}": int(c) for g, r, c in hook.get_records(sql)}
        print(f"Nov 11 quarantine: {rows}")
        g3 = sum(c for k, c in rows.items() if k.startswith("G3:"))
        g4 = sum(c for k, c in rows.items() if k.startswith("G4:"))
        g6 = sum(c for k, c in rows.items() if k.startswith("G6:"))
        errors = []
        if g3 != 2:
            errors.append(f"D1/G3 expected 2 null-key rows, got {g3}")
        if g4 != 1:
            errors.append(f"D4/G4 expected 1 duplicate, got {g4}")
        if g6 < 1:
            errors.append(f"D2/G6 expected ≥1 bad qty, got {g6}")
        if errors:
            raise ValueError("; ".join(errors))
        return {"G3": g3, "G4": g4, "G6": g6}

    @task(task_id="prove_d3_g7")
    def prove_d3_g7() -> dict:
        """Nov 12: G7 orphan ITEM 199999 quarantined; not in FCT_SALES."""
        cfg = _cfg()
        db = cfg["database"]
        silver, gold = cfg["schemas"]["silver"], cfg["schemas"]["gold"]
        hook = _hook()
        g7 = hook.get_first(
            f"""
            SELECT COUNT(*) FROM {db}.{silver}.TMP_QUARANTINE
            WHERE INGEST_DT = '2026-11-12' AND GATE_ID = 'G7'
              AND OBJECT_NM = 'FCT_SALES'
            """
        )[0]
        in_fct = hook.get_first(
            f"""
            SELECT COUNT(*) FROM {db}.{gold}.FCT_SALES f
            INNER JOIN {db}.{gold}.DIM_ITEM i ON f.ITEM_KY = i.ITEM_KY
            WHERE f.DAY_KY = 20261112 AND i.ITEM_ID = '199999'
            """
        )[0]
        # orphan never in dim — also ensure no sales line still unresolved in TMP made it via bad join
        print(f"D3: G7 quarantine={g7}, FCT rows for 199999={in_fct}")
        if int(g7) != 1:
            raise ValueError(f"D3/G7 expected 1 orphan quarantine, got {g7}")
        if int(in_fct) != 0:
            raise ValueError(f"D3: orphan must not be in FCT_SALES, found {in_fct}")
        return {"g7": int(g7), "in_fct": int(in_fct)}

    @task(task_id="prove_d5_scd2")
    def prove_d5_scd2() -> dict:
        """LOCN 1008: closed Spokane version + current Portland Relocated."""
        cfg = _cfg()
        db, gold = cfg["database"], cfg["schemas"]["gold"]
        hook = _hook()
        rows = hook.get_records(
            f"""
            SELECT l.LOCN_NM, c.CITY_NM, l.EFFECTIVE_START_DT, l.EFFECTIVE_END_DT, l.CURRENT_FLG
            FROM {db}.{gold}.DIM_LOCN l
            INNER JOIN {db}.{gold}.DIM_CITY c
                ON l.CITY_KY = c.CITY_KY AND c.CURRENT_FLG
            WHERE l.LOCN_ID = '1008'
            ORDER BY l.EFFECTIVE_START_DT
            """
        )
        print(f"D5 DIM_LOCN 1008 versions: {rows}")
        if len(rows) < 2:
            raise ValueError(f"D5 expected ≥2 SCD2 versions for 1008, got {len(rows)}")
        current = [r for r in rows if r[4] is True or r[4] == True]
        closed = [r for r in rows if not (r[4] is True or r[4] == True)]
        if len(current) != 1:
            raise ValueError(f"D5 expected exactly 1 current row, got {len(current)}")
        if not closed:
            raise ValueError("D5 expected at least one closed version")
        cur_nm, cur_city = current[0][0], current[0][1]
        if cur_city != "Portland":
            raise ValueError(f"D5 current city expected Portland, got {cur_city}")
        if "Portland" not in (cur_nm or ""):
            raise ValueError(f"D5 current LOCN_NM expected Portland Relocated, got {cur_nm}")
        closed_cities = {r[1] for r in closed}
        if "Spokane" not in closed_cities:
            raise ValueError(f"D5 expected a closed Spokane version, got cities {closed_cities}")
        return {"versions": len(rows), "current_city": cur_city}

    @task(task_id="pk_checker")
    def pk_checker() -> dict:
        """Any row from a pk_*.sql file = fail (Snowflake PKs are not enforced)."""
        cfg = _cfg()
        hook = _hook()
        failures = {}
        for rel in PK_SQL_FILES:
            name = Path(rel).stem
            rows = hook.get_records(_render(rel, cfg))
            if rows:
                failures[name] = rows[:5]
                print(f"PK FAIL {name}: {rows[:5]}")
            else:
                print(f"PK OK {name}")
        if failures:
            raise ValueError(f"PK checker failed: {list(failures)}")
        return {"checked": len(PK_SQL_FILES), "failures": 0}

    @task(task_id="g10_price_overlap")
    def g10_price_overlap() -> int:
        """Gate G10: fail if any overlapping FCT_PRICE periods."""
        cfg = _cfg()
        hook = _hook()
        rows = hook.get_records(_render("gates/g10_price_overlap.sql", cfg))
        if rows:
            print(f"G10 FAIL: {len(rows)} overlap(s), sample={rows[:3]}")
            raise ValueError(f"G10 failed: {len(rows)} overlapping price period(s)")
        print("G10 OK: no price period overlaps")
        return 0

    d112 = prove_d1_d2_d4()
    d3 = prove_d3_g7()
    d5 = prove_d5_scd2()
    pk = pk_checker()
    g10 = g10_price_overlap()

    [d112, d3, d5] >> pk >> g10


prove_defects()

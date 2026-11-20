---
name: airflow-medallion-patterns
description: Rules and patterns for the airflow-medallion project (Airflow 3.3 + Snowflake, Bronze/Silver/Gold). Use when writing or reviewing DAGs, SQL loads, quality gates, reconciliation, backfills or lesson docs in this repo.
---

# Airflow Medallion Patterns

One page of working rules for this project. It sits beside `AGENT_CONTEXT.md`. If the two disagree, **AGENT_CONTEXT wins** (it holds the decisions and the learner's preferences). Keep everything simple: plain `for` loops, SQL in files, short DAGs, no pytest, no Dockerfile, no helper layers.

## 1. Design principles

| Principle | What it means here |
|---|---|
| **Idempotent** | A second run for the same `ds` changes no counts and no totals. Bronze: `DELETE WHERE _INGEST_DT = ds` then `COPY ... FORCE = TRUE`. Silver: delete-then-insert by `ds`. Dims: MERGE or `NOT EXISTS`. Facts: `NOT EXISTS` anti-join |
| **Atomic** | A task either finishes or fails whole. One table load per task, one SQL file per table |
| **Incremental** | A run loads only its own day. Time is a run dimension, never a graph dimension |
| **Observable** | Row counts in and out, quarantine counts and recon results are written to AUDIT; logs say what was checked |
| **Deterministic** | Dates come from `ds` (or `conf` for manual runs), never `CURRENT_DATE`. Generator output is reproducible for a given date |

## 2. DAG shape

```
generate_landing_day -> wait_for_files (x4) -> bronze (mapped x4) -> silver
  -> gold_dims -> gold_facts -> agg -> reconcile (G11)
```

- One **single-day, `ds`-driven DAG** (`14_medallion_daily`). Five days = five runs via Backfill/Trigger or `airflow backfill create`. Never five TaskGroups in one run.
- `schedule="@daily"` (a `None` schedule cannot backfill in Airflow 3), `catchup=False`, `max_active_runs=1`, `is_paused_upon_creation=True`.
- Backfill in **chronological order, never reverse**: SCD2 and soft-closed inventory depend on order.
- Dims in parallel branches, then facts. Gates sit between layers.
- Use `max_active_runs=1` for ordering. Avoid `depends_on_past=True` (it makes one failed day block every later task in odd ways).

## 3. Airflow 3.3 conventions

```python
from airflow.sdk import dag, task, Asset, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

- Every DAG: `template_searchpath=[str(SQL_DIR)]`, `retries >= 1` (via `default_args`), `tags=["medallion", "NN"]`, lowercase task ids, `dag_id = file stem`.
- No `Variable.get()` at module top level. Read config inside tasks or in templates.
- Heavy logic goes in `include/medallion/`, not the DAG file.
- Sensors: `mode="reschedule"`, explicit `timeout` and `poke_interval`. Mapped x4 with `.partial().expand()`.
- Manual triggers may have no `logical_date`: always pass an explicit date (logical date or `conf`).
- Names: `execution_date` is now `logical_date`/`ds`; `schedule_interval` is `schedule`; `airflow dags backfill` is `airflow backfill create`.

## 4. Snowflake and SQL rules

- One SQL file per table: `ddl/<layer>/<table>.sql`, `load/<layer>/<table>.sql`, `gates/gNN_<rule>.sql`, `checks/`.
- Names always come from config: `{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES`.
- Never put `{{ }}` in a SQL comment (Jinja renders comments). `PUT` / `GET` SQL must start with the command.
- Pick the right write pattern:

| Pattern | Use for |
|---|---|
| `DELETE` by `ds` + insert | Bronze and Silver day partitions |
| `MERGE` | SCD1 attributes, SCD2 close-then-insert |
| `NOT EXISTS` insert | Gold facts and incremental dims |
| Soft close (`EFFECTIVE_END_DT`, `CURRENT_FLG`) | `FCT_INVENTORY` and SCD2 history. Never `DELETE` |

- Surrogate keys from sequences (create with `IF NOT EXISTS`); effective dates from `ds`.
- NULL handling explicit: `COALESCE`, `NULLIF`, typed casts (`TRY_TO_*` in Silver so bad values become quarantine rows, not errors).
- Cost: XSMALL warehouse, 60 s auto-suspend, filter by `_INGEST_DT`. No broadcast-join, cache or pooling tuning is needed at ~800 rows a day.

## 5. Data quality: gates and quarantine

| Dimension | Where it lives |
|---|---|
| Completeness | G1 files present (sensor + `LIST @stage`); G2 column-count contract |
| Validity | G3-G6 types, nulls, negative qty, amount rules; G12 outlier limits from `variables.json` |
| Consistency | G6 `SALES = QTY x PRICE`, `NET = SALES - DISCOUNT`, `RESERVED <= ON_HAND` |
| Uniqueness | Dedupe in Silver (D4); `pk_*.sql` PK checker; G10 |
| Referential integrity | G7 orphan item, G8 dimension keys before facts |
| Anomaly detection | G12 configurable price ratio, qty, line amount, on-hand limits |
| Accuracy / tie-out | G11 reconciliation, G9 aggregate check |
| Timeliness | Sensor timeout; `ds`-partitioned loads |

**Two kinds of gate. Do not mix them up:**

- **Quarantine gates** (G3-G7, G12): move the bad row to `TMP_QUARANTINE` with a **reason code**, count it, and **do not raise**. The run stays green.
- **Fail gates** (G1, G2, G8-G11): raise and stop the run.

**Reconciliation equation** (quarantine must be inside it):

```
source net sales = Gold net sales + quarantined net sales
rows in          = rows in Gold + rows quarantined (+ deduped)
```

`reconcile` is the only task meant to fail the DAG on a mismatch. On mismatch: alert, then fail (return-flag pattern: bool, branch, `send_alert`, `fail_run`).

Valid outliers (premium price, bulk buy, 100% discount, zero stock) **must reach Gold**. A gate that catches them is wrong.

## 6. Planted defects (known answers)

Calendar buckets in `generate_complex.py`, `date.toordinal() % 6`: `0` clean, `1` null ITEM_ID, `2` negative qty, `3` orphan `199999`, `4` duplicate, `5` outliers. SCD2: `LOCN 1008` CITY_ID Spokane to Portland from `2026-11-13`. Every planted line is appended to `data/store/defect_log.txt`; a proof task (`10_prove_defects`) asserts each defect lands where expected.

## 7. Checks before calling a task done

Run these (the learner runs them; the agent says what each result means):

1. Parse: `docker compose run --rm airflow-cli airflow dags list-import-errors` gives `No data found`.
2. No hard-coding in DAGs (no output = pass):
   `Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'`
3. Idempotency: run one date twice, compare counts and sums per table.
4. Backfill: five runs (10-14 Nov), every task green, recon table of 5 dates with variance 0.

## 8. Excellence checklist (kept short)

- [ ] Idempotent at every layer, proven by a rerun
- [ ] Bad data quarantined with reason codes, never silently dropped
- [ ] Gold ties to source for all five days
- [ ] Config, credentials, paths and table names outside DAG files
- [ ] Retries set, timeouts on sensors, alerts on recon failure
- [ ] Data types chosen and explained for every table
- [ ] Lesson doc, `LESSONS.md`, `PROGRESS.md`, README and evidence updated after each lesson
- [ ] Known limitations written down honestly (for example no external stage, no long history)

## 9. Don'ts

- Don't hardcode dates; use `ds`.
- Don't use global state in tasks.
- Don't use `CREATE OR REPLACE` on tables that hold data; `ALTER` instead.
- Don't skip a requirement silently; keep the running checklist at the top of each response.
- Don't add pytest, Dockerfiles, argparse, or abstraction layers.
# Progress snapshot

Last updated: **18 Nov 2026**.

## Where we are

Lessons **1–5 are done**. Lesson **6 is in progress** (slim five-day complex generator).

Current lesson file: [`lessons/06_history_generator.md`](lessons/06_history_generator.md).

## Completed by lesson

| Lesson | What you can do now |
|---|---|
| **1** Environment | Airflow 3.3.2 Compose stack; Snowflake bootstrap; key-pair `AIRFLOW_SVC`; connections + `medallion_config` Variable; DAG `01_connection_check` |
| **2** Simple data + STG setup | Generators for 2026-11-10 + `seed_dim_day`; valid outliers + 4 anomalies; file formats; 4 empty STG tables; DAG `02_stg_setup` |
| **3** Stage + COPY | `MY_INT_STAGE`; PUT/LIST/COPY with lineage; positional inventory; external stage **dropped** (departure 9); DAG `03_stage_load` |
| **4** Waiting (G1) | Mapped `FileSensor` (`reschedule`, 10 min timeout); Assets taught; `assert_files_on_stage`; DAG `04_waiting` |
| **5** Bronze load | Wait → PUT overwrite → G1 → G2 → `DELETE`+`COPY FORCE` → archive; DAG `05_bronze_load` |
| **6** Complex data (in progress) | `generate_complex.py`: **5 days**, ~800 lines/day, sales ⊆ stocked, **D1–D5 defects planted**; history deferred |

## Facts to keep

| Item | Value |
|---|---|
| Business dates (pipeline) | 2026-11-10, 11, 12, 13, 16 |
| Complex daily volume | **810** sales lines (Nov 11 = **811**); **400 txns**/day cycling **12/14/16/18/20** per store; inventory **1501** |
| Complex Nov 10 net | **2,819,814.46** (anomaly-dominated) |
| Planted defects | D1–D2–D4 on 11 Nov; D3 orphan `199999` on 12 Nov; D5 `LOCN 1008` city move from 13 Nov |
| History | **Deferred** (no `data/history/` from generator) |
| DAG naming | `dags/NN_<topic>.py`, `dag_id="NN_<topic>"`, tags `["medallion","NN"]` |

## Still ahead (short)

7 Silver TMP + G3–G6 + G12 → 8 Gold dims → 9 Gold facts → 10 defects → 11 agg/recon/alert → 12 idempotency proof → 13 five-day backfill → 14 README.

Full checklist: [`LESSONS.md`](LESSONS.md). Agent handoff: [`AGENT_CONTEXT.md`](AGENT_CONTEXT.md).

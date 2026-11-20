# Progress snapshot

Last updated: **20 Nov 2026**.

## Where we are

Lessons **1–13 are done**. Lesson **14 is in progress** (ultimate DAG `14_medallion_daily`).

Current lesson file: [`lessons/14_medallion_daily.md`](lessons/14_medallion_daily.md).

## Completed by lesson

| Lesson | What you can do now |
|---|---|
| **1–12** | Environment through idempotency (see prior snapshots) |
| **13** Backfill | Five consecutive days 10–14; `13_backfill_evidence` |
| **14** Ultimate (in progress) | `generate_landing_day` (this `ds`) → wait → bronze…reconcile; calendar `% 5` defects + `data/store/defect_log.txt`; UI/CLI backfill 10–14; `evidence/run_queries.py` + `tests/run_all.py` (13/13 pass) |

## Facts to keep

| Item | Value |
|---|---|
| Demo dates | 2026-11-10 … 2026-11-14 (add 15 for orphan bucket) |
| Defects | `toordinal() % 5` exclusive (10–14 Nov = buckets 0–4); SCD2 from 2026-11-13; SCD1 from 2026-11-14; log `data/store/defect_log.txt` |
| Ultimate DAG | `14_medallion_daily` — load + generate follow logical date `ds` |
| Schedule | `@daily`, `catchup=False`, starts paused; `max_active_runs=1` |
| History | Deferred |
| DAG naming | `dags/NN_<topic>.py` |

## Still ahead (short)

Finish L14 checkpoint (green ultimate run + short README/acceptance if you want it in-repo).

Full checklist: [`LESSONS.md`](LESSONS.md). Agent handoff: [`AGENT_CONTEXT.md`](AGENT_CONTEXT.md).

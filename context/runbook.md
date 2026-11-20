# Runbook: how to run `retail_medallion_daily`

One DAG run = one business date `ds`. Days must load in order (`max_active_runs = 1`). Set the Airflow UI timezone to **UTC**. Default UI: http://localhost:8080 (`airflow` / `airflow`).

Run every command from the project root.

```powershell
cd C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion
docker compose up -d
```

Offline checks (no Snowflake writes):

```powershell
docker compose run --rm airflow-cli python /opt/airflow/tests/run_all.py
```

---

## When to use which method

| Goal | Method |
|---|---|
| Let the schedule pick up **today** | Unpause the DAG (scheduled run) |
| Load **one** specific date | Trigger single run, or `dags test` |
| Load a **date range** in order (history) | Backfill (UI or CLI) |
| Prove the **same day** is idempotent | Clear the latest green run and let it rerun |
| Fix / replay a **past** day after newer Gold exists | Backfill that day (and forward) with reprocess; DAG rewinds Gold first |
| Start over in Snowflake | Drop schemas, then any run recreates DDL |

---

## Fresh local wipe (optional)

Clears generated CSVs and defect log only. Does not touch Snowflake.

```powershell
Remove-Item data\archive\2026-* -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item data\landing\*.csv -Force -ErrorAction SilentlyContinue
Remove-Item data\store\defect_log.txt -Force -ErrorAction SilentlyContinue
```

Snowflake wipe (Snowsight): run `evidence/snowsight/00_reset_fresh_start.sql`. The DAG task `setup_platform` recreates objects on the next run.

If an old DAG id still appears in the UI:

```powershell
docker compose run --rm airflow-cli airflow dags delete 14_medallion_daily -y
docker compose run --rm airflow-cli airflow dags list
```

You should see only `retail_medallion_daily`.

---

## 1. Scheduled daily run

1. Unpause `retail_medallion_daily`.
2. Airflow creates a run for today (`scheduled__…`, UTC midnight logical date).
3. If you need history first, mark that auto-run **Failed** (or leave it) and backfill older days first; then **Clear** today’s run when ready.

Pause the DAG when you do not want tomorrow’s automatic run.

---

## 2. Single run (UI)

**Trigger → Trigger DAG** (or **Single Run**): set the logical date to the business day you want (`YYYY-MM-DD` at 00:00 **UTC**).

Use for one-off days after history is already loaded, or to fill a gap when you do not need a range.

---

## 3. Single run (CLI)

Parse + execute one logical date (good for a quick check):

```powershell
docker compose run --rm airflow-cli airflow dags test retail_medallion_daily 2026-11-20
```

Or create a real run in the metastore:

```powershell
docker compose run --rm airflow-cli airflow dags trigger retail_medallion_daily --logical-date 2026-11-20T00:00:00+00:00
```

---

## 4. Backfill a range (UI)

DAG must be **unpaused**.

**Trigger → Backfill** → From / To (UTC midnight) → reprocess **All Runs** → max active runs `1` → Run Backfill.

Use for the evidence history window, e.g. 2026-11-10 → 2026-11-14.

Pause or cancel from the DAG page banner (**Pause backfill** / **Cancel backfill**).

---

## 5. Backfill a range (CLI)

```powershell
docker compose run --rm airflow-cli airflow backfill create `
  --dag-id retail_medallion_daily `
  --from-date 2026-11-10 --to-date 2026-11-14 `
  --reprocess-behavior completed --max-active-runs 1
```

`--reprocess-behavior completed` re-runs days that already succeeded (needed for reload drills).

---

## 6. Rerun the latest day (idempotency)

Only clear/rerun the **newest** loaded day. Clearing an older day after newer Gold exists is a reload (section 7), not a simple rerun.

In **Grid**, open the latest run → **Clear run** (all tasks) → wait for green. Counts and SCD history for that day should match the previous attempt; `AUDIT.C_BATCH_LOG` shows `TRY_NUMBER` 1 and 2.

---

## 7. Reload one past day (Gold rewind + forward replay)

Example: 12 Nov was wrong, but Gold already has 13–20 Nov.

1. Backfill from the bad day through the last day you still want corrected, with reprocess:

```powershell
docker compose run --rm airflow-cli airflow backfill create `
  --dag-id retail_medallion_daily `
  --from-date 2026-11-12 --to-date 2026-11-14 `
  --reprocess-behavior completed --max-active-runs 1
```

2. On the first past day, `gold_dims.rewind_gold_for_reload` rewinds Gold to the day before `ds`, then the pipeline reloads forward. Later days in that backfill log `No rewind` if Gold is already at the right watermark.
3. **Clear** any newer scheduled day (e.g. 20 Nov) that still needs replay after the backfill ends short of it.

Confirm with `AUDIT.C_LOAD_DATES` (Gold `LAST_LOAD_DT`) and `AUDIT.C_PARAM` (`GOLD_REWOUND_FROM` on the rewind batch).

---

## Checks after a run

Copy-paste SQL: `evidence/snowsight/` (set `FROM_DT` / `TO_DT` at the top of each file).

| File | Use |
|---|---|
| `01_layer_row_flow.sql` | Bronze → Silver → rejects → Gold; `ROW_DIFF = 0` |
| `02_defects_by_layer.sql` | Planted defects solved |
| `03_rejects_by_rule.sql` | Quarantine by gate |
| `04_reconciliation.sql` | Five G11 checks PASS |
| `05_scd_history.sql` | SCD2 / SCD1 samples |
| `06_control_tables.sql` | Scripts, params, batch log, load dates |
| `07_final_view_by_region.sql` | Business view |

---

## If a run fails

1. Open the red task → **Logs**; the first `AirflowFailException` names the gate.
2. `06_control_tables.sql` query D for that day: `FAILURE_REASON`.
3. Fix, then **Clear** the failed run. Clearing the **latest** day is always safe; past days after newer Gold need the reload path above.

**UI date pitfall:** logical dates follow the UI timezone. Prefer UTC so `2026-11-20` means business day 20 Nov, not the previous UTC day.

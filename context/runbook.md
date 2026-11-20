# Runbook: fresh start → evidence (retail_medallion_daily)

Follow the phases in order. Each step says **what to do**, **what you should see**, and **which screenshot to save**.

Screenshots go in `evidence/screenshots/<phase folder>/` with the file name given. Crop or blur the Snowflake account identifier (Snowsight URL and top-left menu) and any credentials before saving.

**Before you start:**

- **PowerShell location:** run every command from the project folder: `cd C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion`
- **Airflow UI:** <http://localhost:8080>. Set the UI timezone to **UTC** (user menu, bottom left), so run dates read as business dates.
- **Snowsight:** open a worksheet and paste a file from `evidence/snowsight/`. Run a single query with **Ctrl+Enter** (cursor inside it), or everything with **Ctrl+Shift+Enter**. Each file sets its dates at the top (`SET FROM_DT / TO_DT`); edit them there.

| Folder | Phase |
|---|---|
| `evidence/screenshots/00_reset/` | 0 Fresh start |
| `evidence/screenshots/01_backfill/` | 1 History load 10–14 Nov |
| `evidence/screenshots/02_daily_run/` | 2 Today's scheduled run (20 Nov) |
| `evidence/screenshots/03_rerun_proof/` | 3 Same day twice → same result |
| `evidence/screenshots/04_reload_bad_day/` | 4 Reload 12 Nov (Gold rewind + replay) |

---

## Phase 0: Fresh start

**0.1 Make the screenshot folders and clear the old local outputs** (these are generated files; they come back on the next runs):

```powershell
"00_reset","01_backfill","02_daily_run","03_rerun_proof","04_reload_bad_day" | ForEach-Object { New-Item -ItemType Directory -Force "evidence\screenshots\$_" | Out-Null }
Remove-Item data\archive\2026-* -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item data\landing\*.csv -Force -ErrorAction SilentlyContinue
Remove-Item data\store\defect_log.txt -Force -ErrorAction SilentlyContinue
Remove-Item evidence\query-results\*.md, evidence\query-results\_snapshots -Recurse -Force -ErrorAction SilentlyContinue
```

**0.2 Remove the old DAG and its runs from Airflow.** Its file is already renamed, but the metadata is still there and the DAG is still unpaused:

```powershell
docker compose up -d
docker compose run --rm airflow-cli airflow dags delete 14_medallion_daily -y
docker compose run --rm airflow-cli airflow dags list
```

You should see only `retail_medallion_daily`, paused (`is_paused = True`).

**0.3 Drop the Snowflake objects.** In Snowsight, run `evidence/snowsight/00_reset_fresh_start.sql`. The last statement should list only `INFORMATION_SCHEMA` and `PUBLIC`. The DAG's first task recreates everything.

- Screenshot `00_reset/01_snowflake_schemas_dropped.png`

**0.4 Offline tests:**

```powershell
docker compose run --rm airflow-cli python /opt/airflow/tests/run_all.py
```

Expect `15 passed, 0 failed` (also saved to `evidence/query-results/tests.txt`).

- Screenshot `00_reset/02_tests_passed.png`

**0.5 The DAG:** in the Airflow UI open `retail_medallion_daily` → **Graph**, and expand the task groups.

- Screenshot `00_reset/03_dag_graph.png`

---

## Phase 1: Load history, 10–14 Nov (each planted defect once)

**1.1 Unpause, then stop the automatic run.**

1. Toggle the DAG **on**.
2. Airflow immediately creates a run for today, `scheduled__2026-11-20…` (that's `catchup=False` with a daily schedule).
3. Open that run in **Grid** and choose **Mark run as → Failed** straight away.

Why: the history days must load first, in date order. The 20 Nov run is re-run in Phase 2 with **Clear**. If it already finished, that's not a problem: the backfill will rewind Gold (Phase 4 logic). It just makes the evidence harder to read.

**1.2 Backfill five days, one at a time:**

```powershell
docker compose run --rm airflow-cli airflow backfill create --dag-id retail_medallion_daily --from-date 2026-11-10 --to-date 2026-11-14 --reprocess-behavior completed --max-active-runs 1
```

(UI alternative: **Trigger → Backfill**, 10 → 14 Nov, reprocess *All runs*.)

In **Grid**, check that the run IDs are `backfill__2026-11-10T00:00:00+00:00` … `…14…`. Wait until all five are green, a few minutes each.

- Screenshot `01_backfill/01_grid_five_green_runs.png`

**1.3 Task logs** (click the task → **Logs**) for the gates that only log:

| Day | Task | Shows | Screenshot |
|---|---|---|---|
| 13 Nov | `gold_facts.g07_alert` | `G7 alert 2026-11-13: [('ORPHAN_ITEM', 1)]` | `01_backfill/02_log_g07_orphan_13nov.png` |
| 14 Nov | `reconcile.assert_reconciliation` | five `G11 … PASS` lines | `01_backfill/03_log_recon_14nov.png` |
| 11 Nov | `gold_facts.g08_inventory_count` | `G8 2026-11-11: today=… prior(2026-11-10)=… drift=…%` (well under 20 %) | `01_backfill/04_log_g08_11nov.png` |

**1.4 Snowsight evidence** (dates in each file: FROM `2026-11-10`, TO `2026-11-14`):

| File | What it proves | Expected | Screenshot |
|---|---|---|---|
| `01_layer_row_flow.sql` | Bronze → Silver → rejects → Gold per day | `ROW_DIFF = 0` every day | `01_backfill/05_layer_row_flow.png` |
| `02_defects_by_layer.sql` | each planted defect: found in Bronze, gate that caught it, none in Gold | every `RESULT = SOLVED` | `01_backfill/06_defects_by_layer.png` |
| `03_rejects_by_rule.sql` (both queries) | what each gate rejected, plus one full rejected row per rule | G12 on 10th, G3 on 11th, G6 on 12th, G7 on 13th, G4 on 14th | `01_backfill/07_rejects_by_rule.png`, `08_reject_sample_rows.png` |
| `04_reconciliation.sql` (both queries) | five checks per day | all `PASS`; summary `5 / 5` per day | `01_backfill/09_recon_checks.png`, `10_recon_summary.png` |
| `05_scd_history.sql` | SCD2 store 1008 moved 13 Nov; SCD1 item 100011 revised 14 Nov | `DIM_LOCN` 2 versions (old ends 2026-11-12); `DIM_ITEM` 1 row "(rev Nov)" | `01_backfill/11_scd_history.png` |
| `06_control_tables.sql` A, B, E | the control tables (skip **C** and **D** on Phase 1) | 39 scripts; params per batch; `C_LOAD_DATES` Gold = 2026-11-14 | `01_backfill/12_c_batch_scripts.png`, `13_c_param.png`, `14_c_load_dates.png` |
| `07_final_view_by_region.sql` | the business output | net sales by region | `01_backfill/15_view_by_region.png` |

**Skip `06` C and D for Phase 1.** `C_BATCH_LOG` was not written during the first backfill (a bind bug in the callback; fixed after that run). Do not re-backfill for it. From Phase 2 onward (20 Nov Clear / rerun / reload), new runs write `C_BATCH_LOG`; take screenshots `02_daily_run/05_c_batch_log_summary.png` (query C) and `03_rerun_proof/03_batch_log_two_attempts.png` (query D) then.

---

## Phase 2: Today's scheduled run (20 Nov)

**2.1** In **Grid**, open the failed `scheduled__2026-11-20…` run → **Clear run** (all tasks) → confirm. Wait for green.

- Screenshot `02_daily_run/01_grid_20nov_green.png`

**2.2** Snowsight `04_reconciliation.sql` and `01_layer_row_flow.sql` with FROM = TO = `2026-11-20`: 5 PASS, `ROW_DIFF = 0`. Then `06` E: Gold `LAST_LOAD_DT = 2026-11-20`. Then `06` C: at least the 20 Nov batch should appear (this is the first proof that `C_BATCH_LOG` is writing).

- Screenshots `02_daily_run/02_recon_20nov.png`, `03_layer_flow_20nov.png`, `04_c_load_dates_20nov.png`, `05_c_batch_log_summary.png`

---

## Phase 3: Rerun proof (same day twice gives the same result)

Re-run only the **latest** day (20 Nov). Re-running an older day goes through the reload path in Phase 4 instead.

1. **Before:** `01_layer_row_flow.sql` (FROM = TO = `2026-11-20`) and `05_scd_history.sql`.
   - Screenshot `03_rerun_proof/01_before.png`
2. **Clear** the `scheduled__2026-11-20…` run again and wait for green.
3. **After:** run the same two queries. Every number is identical.
   - Screenshot `03_rerun_proof/02_after.png`
4. `06_control_tables.sql` D with the date set to `2026-11-20`: each task appears with `TRY_NUMBER` 1 and 2, so it ran twice.
   - Screenshot `03_rerun_proof/03_batch_log_two_attempts.png`

---

## Phase 4: Reload one bad day (12 Nov)

This simulates "12 Nov was wrong and is reloaded". The DAG notices 12 Nov is older than Gold's newest date (20 Nov), rewinds Gold to 11 Nov, then reloads forward.

**4.1** Backfill 12 → 14 with reprocess:

```powershell
docker compose run --rm airflow-cli airflow backfill create --dag-id retail_medallion_daily --from-date 2026-11-12 --to-date 2026-11-14 --reprocess-behavior completed --max-active-runs 1
```

**4.2** Log of `gold_dims.rewind_gold_for_reload` for 12 Nov: `RELOAD 2026-11-12: Gold rewound to the day before; replay up to 2026-11-20`. On 13 and 14 Nov the same task says `No rewind`.

- Screenshot `04_reload_bad_day/01_log_rewind_12nov.png`

**4.3** Snowsight:

- `06` B: the 12 Nov backfill batch has `GOLD_REWOUND_FROM = 2026-11-20`.
- `06` E: Gold `LAST_LOAD_DT = 2026-11-14`. Gold is now current only to 14 Nov, which is the truth until 20 Nov is replayed.
- Screenshots `04_reload_bad_day/02_c_param_rewound.png`, `03_c_load_dates_14nov.png`

**4.4** Finish the replay: **Clear** the `scheduled__2026-11-20…` run again → green.

**4.5** Prove nothing broke:

- `05_scd_history.sql`: still 2 versions for 1008 and 1 row for 100011, with no extra versions.
- `02_defects_by_layer.sql`: all `SOLVED`.
- `04_reconciliation.sql` (FROM `2026-11-10`, TO `2026-11-20`): all `PASS`.
- `06` E: Gold back at `2026-11-20`.
- Screenshots `04_reload_bad_day/04_scd_after_reload.png`, `05_defects_after_reload.png`, `06_recon_after_reload.png`, `07_c_load_dates_20nov.png`

---

## Phase 5: Wrap up

1. **Pause the DAG** before 05:45 local time tomorrow (00:00 UTC 21 Nov). Otherwise it runs 21 Nov by itself.
2. **Diagram:** open `context/architecture.drawio` → page **Simple view** → export as PNG (100–150 %) → save as `context/architecture-Simple view.png`.
3. **Documentation:** `context/final_state.md` explains every stage, task, SQL file and table; this runbook lists the evidence.

**If a run fails:**

- Open the red task's log; the first `AirflowFailException` line names the gate and the numbers.
- `06_control_tables.sql` D for that day shows `FAILURE_REASON`.
- Fix, then **Clear** the failed run. A run for the latest day is always safe to clear.

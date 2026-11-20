# Evidence - `retail_medallion_daily`

Proof pack for the retail medallion pipeline. One business date per run. Sequence covered: empty start, five-day history backfill (10–14 Nov), scheduled daily load (20 Nov), same-day rerun (idempotency), and reload of one past day (Gold rewind + forward replay).

Screenshot paths are relative to this file.

---

## 0. Fresh start

Empty Snowflake schemas, offline tests green, DAG graph ready.

![Schemas dropped](screenshots/00_reset/01_snowflake_schemas_dropped.png)

**Schemas dropped.** Only `INFORMATION_SCHEMA` and `PUBLIC` remain. The next DAG run recreates Bronze, Silver, Gold and Audit.

![Tests passed](screenshots/00_reset/02_tests_passed.png)

**Offline tests.** `tests/run_all.py` — 15 passed (DAG parse, SQL registry, planted defects, G2 contracts). No Snowflake write.

![DAG graph](screenshots/00_reset/03_dag_graph.png)

**DAG graph.** `setup_platform` → control → generate → sensor → bronze → silver → gold_dims → gold_facts → reconcile → `C_LOAD_DATES`.

---

## 1. History load (10–14 Nov)

Five backfill runs, one day at a time. Each calendar bucket plants one defect class.

### 1.1 Airflow

![Five green backfill runs](screenshots/01_backfill/01_grid_five_green_runs.png)

**Grid.** Five green runs: `backfill__2026-11-10` through `backfill__2026-11-14`.

![G7 orphan alert](screenshots/01_backfill/02_log_g07_orphan_13nov.png)

**G7 (13 Nov).** Orphan item `199999` quarantined before `FCT_SALES`; run stays green (alert only).

![G11 recon log](screenshots/01_backfill/03_log_recon_14nov.png)

**G11 (14 Nov).** All five reconciliation checks print `PASS` in the task log.

![G8 inventory drift](screenshots/01_backfill/04_log_g08_11nov.png)

**G8 (11 Nov).** Inventory row count vs prior day: `1501` / `1501`, drift `0.0%` (fail threshold 20%).

### 1.2 Layers and defects

![Layer row flow](screenshots/01_backfill/05_layer_row_flow.png)

**Row flow.** Per day: Bronze → Silver → rejects → Gold. `ROW_DIFF = 0` means every source row is either in Gold or in `C_REJ_TABLE`.

![Defects by layer](screenshots/01_backfill/06_defects_by_layer.png)

**Planted defects and the gates that caught them.** Every row ends `SOLVED`:

| Day | Defect | Gate | Outcome |
|---|---|---|---|
| 10 | Sales anomaly | G12 | All bad rows rejected; none in Gold |
| 10 | Valid outlier (premium) | none | Kept on purpose; present in Gold |
| 11 | Null `ITEM_ID` | G3 | Rejected; none in Gold |
| 12 | Negative qty | G6 | Rejected; none in Gold |
| 13 | Orphan `199999` | G7 | Rejected before fact load |
| 14 | Duplicate txn+line | G4 | One copy rejected; latest kept in Gold |

![Rejects by rule](screenshots/01_backfill/07_rejects_by_rule.png)

**Rejects by rule.** Counts and net amount held in `AUDIT.C_REJ_TABLE` per day / gate / reason.

![Sample rejected rows](screenshots/01_backfill/08_reject_sample_rows.png)

**One sample per rule.** `RAW_RECORD` keeps the full source line (G12 anomalies, G3 null, G4 duplicate, G6 negative qty, G7 orphan).

### 1.3 Reconciliation and SCD

![Five recon checks](screenshots/01_backfill/09_recon_checks.png)

**G11 detail.** Five checks per day — all `PASS`:

1. **ROW_COUNT** — staging = Gold + rejects
2. **AMOUNT** — source net = Gold net + reject net
3. **ORPHANS** — unknown-key rejects under 1%
4. **FRESHNESS** — latest fact date = run date
5. **DUPLICATES** — no repeated business keys at fact grain

![Recon summary](screenshots/01_backfill/10_recon_summary.png)

**G11 summary.** One line per day: `5 / 5` passed, no failed checks.

![SCD history](screenshots/01_backfill/11_scd_history.png)

**Slowly changing dimensions.**

- **SCD2** store `1008`: Spokane closed 12 Nov → Portland current from 13 Nov (two versions).  
- **SCD1** item `100011`: one row; description overwritten to `(rev Nov)`.

### 1.4 Control tables and output

![C_BATCH_SCRIPTS](screenshots/01_backfill/12_c_batch_scripts.png)

**`C_BATCH_SCRIPTS`.** Run order: task, layer, SQL path, target, gate, on-fail (`FAIL` / `REJECT` / `ALERT`).

![C_PARAM](screenshots/01_backfill/13_c_batch_scripts.png)

**`C_PARAM`.** Per-batch parameters (paths, outlier rules, `G8_MAX_DRIFT`, `ORPHAN_MAX_PCT`, `RUN_TYPE=BACKFILL`).

![C_LOAD_DATES](screenshots/01_backfill/14_c_load_dates.png)

**`C_LOAD_DATES`.** Last successful load date per target. After the history backfill, Gold high-water = **2026-11-14**.

![View by region](screenshots/01_backfill/15_view_by_region.png)

**Business view.** `VW_DAILY_ITEM_LOCN_SALES` rolled up to region (net sales, lines, qty).

---

## 2. Daily run (20 Nov)

Scheduled run cleared and completed after history.

![20 Nov green](screenshots/02_daily_run/01_grid_20nov_green.png)

**Grid.** `scheduled__2026-11-20` green end-to-end.

![Recon 20 Nov](screenshots/02_daily_run/02_recon_20nov.png)

**G11 for 20 Nov.** Five checks, all `PASS`.

![Layer flow 20 Nov](screenshots/02_daily_run/03_layer_flow_20nov.png)

**Row flow 20 Nov.** `ROW_DIFF = 0` (810 Bronze = 807 Gold + 3 rejects).

![Load dates 20 Nov](screenshots/02_daily_run/04_c_load_dates_20nov.png)

**`C_LOAD_DATES`.** Gold `LAST_LOAD_DT` advanced to **2026-11-20**.

![C_BATCH_LOG summary](screenshots/02_daily_run/05_c_batch_log_summary.png)

**`C_BATCH_LOG`.** One batch for 20 Nov: 39 tasks, 0 failures, ~160 s wall time, rows affected recorded.

---

## 3. Rerun proof (same day twice)

Clear 20 Nov again. Counts and SCD must match before vs after.

### Layer flow

![Layer flow before rerun](screenshots/03_rerun_proof/01_before.png)

**Before Clear.** 20 Nov: Bronze 810 / Silver 807 / rejects 3 / Gold 807 / `ROW_DIFF = 0`.

![Layer flow after rerun](screenshots/03_rerun_proof/02_after.png)

**After Clear.** Same numbers — idempotent reload of the latest day (no Gold rewind).

### SCD history

![SCD before rerun](screenshots/03_rerun_proof/001_before.png)

**SCD before.** Store 1008: Spokane → Portland; item 100011 revised. Two locn versions, one item row.

![SCD after rerun](screenshots/03_rerun_proof/002_after.png)

**SCD after.** Identical — no extra SCD2 versions from the second apply.

### Task attempts

![Two attempts in C_BATCH_LOG](screenshots/03_rerun_proof/03_batch_log_two_attempts.png)

**`C_BATCH_LOG` detail.** Same tasks appear for both attempts — the day ran twice; Snowflake state is unchanged.

---

## 4. Reload one bad day (12 Nov)

Simulate a corrected past day: backfill **12 → 14**, then replay 20 Nov. Gold rewinds when the run date is older than `C_LOAD_DATES`.

![Reload backfill grid](screenshots/04_reload_bad_day/One%20Day%20wrong%2C%20backfill.png)

**UI.** Backfill 12–14 in progress / complete on the grid (Silver and Gold facts expanded).

![Rewind on 12 Nov](screenshots/04_reload_bad_day/01_log_rewind_12nov.png)

**12 Nov — rewind.** Log: `RELOAD 2026-11-12: Gold rewound to the day before; replay up to 2026-11-20`.

![No rewind on 13 Nov](screenshots/04_reload_bad_day/01_log_rewind_13nov.png)

**13 Nov — no rewind.** Later days are past the new high-water mark, so they replay normally.

![GOLD_REWOUND_FROM](screenshots/04_reload_bad_day/02_c_param_rewound.png)

**Audit trail.** `C_PARAM` row `GOLD_REWOUND_FROM` on the 12 Nov batch — the rewind left a control-table fingerprint.

![Load dates after 12-14](screenshots/04_reload_bad_day/03_c_load_dates_14nov.png)

**After 12–14 replay.** Gold high-water sits at **14 Nov** until 20 Nov is cleared again.

![Recon after reload](screenshots/04_reload_bad_day/06_recon_after_reload.png)

**Recon after reload.** Checks still `PASS` across the range (no broken totals from the rewind).

![Load dates back at 20 Nov](screenshots/04_reload_bad_day/07_c_load_dates_20nov.png)

**Caught up.** After clearing 20 Nov, Gold `LAST_LOAD_DT` is **2026-11-20** again.

---

## What this pack shows

| Claim | Where |
|---|---|
| Pipeline builds from empty schemas | §0 |
| Defects caught at the right gate; Gold stays clean | §1.2 |
| Five recon rules pass every day | §1.3, §2, §4 |
| Control tables record order, params, load watermarks | §1.4, §2 |
| Same day twice → same Snowflake state | §3 |
| Past-day reload rewinds Gold, then replays forward | §4 |

Related: DAG `retail_medallion_daily`, Snowsight SQL in `evidence/snowsight/`, architecture in `context/architecture_simple.md`.

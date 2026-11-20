# Final state: what the DAG does, task by task

*Snapshot · 20 Nov 2026. One DAG: `retail_medallion_daily` (`dags/retail_medallion_daily.py`). All SQL lives under `include/sql/`.*

## The DAG in one paragraph

One run loads **one business date** `ds` (the run's logical date). It creates any missing objects, records the run in the control tables, generates that day's four CSV files, waits for them, then moves the data Bronze → Silver → Gold, reconciles, and finally stamps `C_LOAD_DATES`. Several days are loaded with a **backfill**: one run per day, oldest first (`max_active_runs=1`), because SCD2, inventory and price history only stay correct when days arrive in order.

```
setup_platform → load_c_batch_scripts → load_c_param → generate_landing_day → wait_for_files ×4
  → bronze → silver → gold_dims → gold_facts → reconcile → load_c_load_dates
```

**Every task**, on success, failure or retry, writes one row to `AUDIT.C_BATCH_LOG` through the callbacks in `default_args` (`runner.log_success`, `log_failure`, `log_retry`).

**Gate behaviour** (the "on bad data" column below):

- **FAIL**: the task fails and the run stops.
- **REJECT**: bad rows go to `AUDIT.C_REJ_TABLE` with a rule and reason, and the run continues.
- **ALERT**: the count is written to the task log only.

## Stage by stage

### 1. Control start

| Task | What it does | SQL / code | On bad data |
|---|---|---|---|
| `setup_platform` | Creates the four schemas, the file formats, the stage, and every Bronze, Silver, Gold, Audit and control object (`IF NOT EXISTS`) in one session. A fresh account needs only the one-time `setup/00_bootstrap.sql`. | `setup/00_schemas.sql`, `setup/01_file_formats.sql`, `setup/02_stages.sql`, `ddl/<layer>/*.sql` (via `runner.ddl_files`) | FAIL |
| `load_c_batch_scripts` | Upserts the run order: one row per task, with its SQL file, target table and gate. | `load/audit/c_batch_scripts.sql` | FAIL |
| `load_c_param` | Records this batch's parameters: business date, batch id, run type, paths, outlier rules, the G8 limit and the orphan threshold. | `load/audit/c_param.sql` | FAIL |

### 2. Source

| Task | What it does | SQL / code | On bad data |
|---|---|---|---|
| `generate_landing_day` | Writes `sales`, `inventory`, `item_master` and `store_master_<YYYYMMDD>.csv` for `ds` into `data/landing/`, with that day's planted defect. | `scripts/generate_complex.py` (via `runner.generate_day`) | FAIL |
| `wait_for_files` ×4 | One mapped `FileSensor` per file, in reschedule mode, with a 600 s timeout. This is G1 (arrival). | none | FAIL on timeout |

### 3. Bronze: raw copy, delete and reload of the day

| Task | What it does | SQL / code | On bad data |
|---|---|---|---|
| `ingest_one` ×4 | Runs PUT to the stage, then the G2 column-count and G1 has-rows check on the file, then DELETE that day plus COPY FORCE into `STG_<SOURCE>`. | `stage/put_file.sql`, `runner.g2_check`, `load/bronze/stg_<source>.sql` | FAIL (G1, G2) |
| `assert_stage_g1` | Lists the stage folder for `ds`: there must be 4 non-empty files. | `checks/list_stage.sql` | FAIL (G1) |
| `archive_landing` | Moves the 4 CSVs to `data/archive/<ds>/`. | Python | FAIL |

### 4. Silver: clean, typed, deduplicated

| Task | What it does | SQL | On bad data |
|---|---|---|---|
| `load_tmp_sales_line` | Loads `STG_SALES` into `TMP_SALES_LINE`. Gates: G3 null keys, G4 duplicate line (the latest is kept), G5 date window, G6 quantity ≤ 0, G12 anomalies. | `load/silver/tmp_sales_line.sql` | REJECT |
| `load_tmp_customer` | MERGEs the members from the day's clean sales lines into `TMP_CUSTOMER`. | `load/silver/tmp_customer.sql` | none |
| `load_tmp_item_loc_snapshot` | Loads `STG_INVENTORY` into `TMP_ITEM_LOC_SNAPSHOT`, which holds stock and price per item and store. Gates: G3, G12. | `load/silver/tmp_item_loc_snapshot.sql` | REJECT |
| `g06b_alert` | Counts rows where reserved > on hand (G6b); those rows still load. | `gates/g06b_reserved_vs_on_hand.sql` | ALERT |
| `load_tmp_item_hier` | Loads `STG_ITEM_MASTER` into `TMP_ITEM_HIER` (department > category > subcategory > item). | `load/silver/tmp_item_hier.sql` | none |
| `load_tmp_locn_hier` | Loads `STG_STORE_MASTER` into `TMP_LOCN_HIER` (division > region > city > store). | `load/silver/tmp_locn_hier.sql` | none |
| `assert_sales_identity` | Checks that every sales row is accounted for: STG rows = TMP rows + rejected rows. | `checks/silver_identity_sales.sql` | FAIL |

### 5. Gold dimensions

| Task | What it does | SQL | On bad data |
|---|---|---|---|
| `rewind_gold_for_reload` | Runs the "reload one bad day" step. If `ds` is older than Gold's `C_LOAD_DATES.LAST_LOAD_DT`, it rewinds Gold to `ds − 1` (details below); otherwise it does nothing. | `checks/gold_high_water.sql`, `load/gold/rewind_from_ds.sql` | FAIL |
| `put_seed_dim_day` → `load_dim_day` | PUTs the calendar seed to the stage and loads it straight into Gold (the planned departure from the reference). | `stage/put_seed_dim_day.sql`, `load/gold/dim_day.sql` | FAIL |
| `load_dim_department` → `category` → `subcategory` → `item` | Applies SCD2: if the HASH of a row's attributes changed, the current version is closed (`END = ds − 1`) and a new version opens (`START = ds`). `DIM_ITEM` also applies SCD1 to `ITEM_DESC` in place. | `load/gold/dim_<level>.sql` | FAIL |
| `load_dim_division` → `region` → `city` → `locn` | Same SCD2 pattern for the location hierarchy. Store 1008 moves city from 13 Nov. | `load/gold/dim_<level>.sql` | FAIL |
| `load_dim_customer` | Applies SCD1 with a MERGE that overwrites in place. | `load/gold/dim_customer.sql` | FAIL |

### 6. Gold facts

| Task | What it does | SQL | On bad data |
|---|---|---|---|
| `g07_keys_resolve` | Finds sales lines whose item, store or customer key does not resolve to a current dimension row and rejects them to `C_REJ_TABLE` (G7). | `gates/g07_keys_resolve.sql` | REJECT |
| `g07_alert` | Logs the orphan count. | `checks/g07_orphan_summary.sql` | ALERT |
| `load_fct_sales` | MERGEs on `(TRANSACTION_ID, LINE_NUM)` with surrogate keys from the current dimension rows. | `load/gold/fct_sales.sql` | FAIL |
| `g08_inventory_count` | Checks that today's inventory rows are within ±20 % of the previous day (G8); skipped on the first day. | `gates/g08_inventory_count.sql` | FAIL |
| `load_fct_inventory` | Soft-close: a changed or vanished row is closed, a new row opens, nothing is deleted. | `load/gold/fct_inventory.sql` | FAIL |
| `load_fct_price` | Price periods: on a price change the old period closes and a new one opens. | `load/gold/fct_price.sql` | FAIL |
| `g10_price_overlap` | Fails if two price periods overlap for the same item and store (G10). | `gates/g10_price_overlap.sql` | FAIL |

### 7. Reconcile

| Task | What it does | SQL | On bad data |
|---|---|---|---|
| `load_agg_daily_item_locn_sales` | DELETE plus INSERT of the day's rollup by day, item and store. | `load/gold/agg_daily_item_locn_sales.sql` | FAIL |
| `load_recon_daily_checks` | Writes the five G11 checks for the day into `RECON_DAILY_CHECKS` (table below). | `load/audit/recon_daily_checks.sql` | FAIL (via the next task) |
| `assert_reconciliation` | G9 requires AGG = FCT. G11 requires all five checks to be PASS. | `gates/g09_agg_equals_fct.sql`, `checks/recon_daily_checks.sql` | FAIL |
| `assert_primary_keys` | Every `pk_*.sql` must return 0 duplicate keys; there is one file per Gold table (13). | `gates/pk_*.sql` | FAIL |

**The five reconciliation checks (G11, `AUDIT.RECON_DAILY_CHECKS`, one row each per day):**

| Check | Rule | How it is measured |
|---|---|---|
| Row count | Staging rows equal target rows plus rejects | `STG_SALES` rows of D = `FCT_SALES` rows of D + sales rejects of D |
| Amount | Warehouse sales total equals the source file total | source net of D = `FCT_SALES` net + rejected net (variance 0.00) |
| Orphans | Facts on the unknown key stay under threshold | G7 rejects ÷ sales lines ≤ `ORPHAN_MAX_PCT` (1 %, `config.py`). Unknown keys never reach Gold: G7 rejects them first. |
| Freshness | Latest fact date equals the run date | `MAX(FCT_SALES.SALE_DT)` and `MAX(FCT_INVENTORY.SNAPSHOT_DT)` = D |
| Duplicates | No repeated business key at fact grain | 0 repeated `(TRANSACTION_ID, LINE_NUM)`, `(ITEM_KY, LOCN_KY, SNAPSHOT_DT)`, `(ITEM_KY, LOCN_KY, EFFECTIVE_START_DT)` |

### 8. Control end

| Task | What it does | SQL | On bad data |
|---|---|---|---|
| `load_c_load_dates` | Runs only if every gate above passed. Sets `LAST_LOAD_DT` (it never moves backwards here) and the batch id for every target listed in `C_BATCH_SCRIPTS`. | `load/audit/c_load_dates.sql` | FAIL |

## Tables

**Read (inputs):**

- 4 landing CSVs per day.
- `data/seed/seed_dim_day.csv`.
- The `medallion_config` Variable, which holds the database, schemas, paths and outlier rules.

**Generated in Snowflake (`MEDALLION_DB`):**

| Schema | Objects | Load style |
|---|---|---|
| BRONZE | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER`; stage `MY_INT_STAGE`; formats `FF_CSV_HEADER`, `FF_CSV_POSITIONAL` | DELETE day + COPY |
| SILVER | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER` | DELETE day + INSERT (`TMP_CUSTOMER`: MERGE) |
| GOLD dims | `DIM_DAY`, `DIM_DEPARTMENT`, `DIM_CATEGORY`, `DIM_SUBCATEGORY`, `DIM_ITEM`, `DIM_DIVISION`, `DIM_REGION`, `DIM_CITY`, `DIM_LOCN`, `DIM_CUSTOMER` + `SEQ_*_KY` sequences | SCD2 / SCD1 / seed |
| GOLD facts | `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE`, `AGG_DAILY_ITEM_LOCN_SALES`, view `VW_DAILY_ITEM_LOCN_SALES` | MERGE / soft-close / periods / DELETE+INSERT |
| AUDIT | `RECON_DAILY_CHECKS` + the five control tables below (`C_REJ_TABLE` is written by the Silver gates and G7) | per run |
| session only | `TMP_INV_D`, `TMP_PRICE_D` (TEMP tables inside the inventory and price loads) | dropped at session end |

## Control tables (AUDIT schema)

These live in the AUDIT schema, so the `medallion_config` Variable did not need a new schema key.

| Table | Holds | Written by | When |
|---|---|---|---|
| `C_BATCH_SCRIPTS` | Which scripts run, in which order (`RUN_SEQ`, `TASK_ID`, `SCRIPT_PATH`, `TARGET_TABLE`, `GATE_ID`, `ON_FAIL`) | `load_c_batch_scripts` | start of every run (MERGE) |
| `C_PARAM` | Run parameters as name/value rows per `BATCH_ID` (= Airflow `run_id`) | `load_c_param` | start of every run |
| `C_BATCH_LOG` | One row per task attempt: status, start/end, `ELAPSED_SEC`, `ROWS_AFFECTED`, `FAILURE_REASON`, script | task callbacks | after every task |
| `C_REJ_TABLE` | Rejected rows and the rule that rejected them (`OBJECT_NM` = table it was headed for, `GATE_ID`, `REASON_CD`, full `RAW_RECORD`) | Silver gates G3–G6, G12 and G7 | during Silver and before `FCT_SALES` |
| `C_LOAD_DATES` | Last successful load date per target table (`LAST_LOAD_DT`, `LAST_RUN_DT`, `LAST_BATCH_ID`) | `load_c_load_dates` | end of a green run |

A few notes on how these behave:

- **`C_REJ_TABLE` belongs to Silver, lives in AUDIT:** it replaced `TMP_QUARANTINE`. The Silver gates (and G7) write it; it sits with the other control tables, and the diagram draws it in the Silver lane.
- **Where file-level failures go:** G1 and G2 failures stop the task before any row exists, so their reason is in `C_BATCH_LOG.FAILURE_REASON`.
- **A reload leaves a trace:** the batch that rewound Gold gets a `GOLD_REWOUND_FROM` row in `C_PARAM`.
- **How `ROWS_AFFECTED` is counted:** it comes from the task's return value. For SQL tasks that's the last statement's count (MERGE = inserted + updated). For `ingest_one` it's the file's rows. Tasks that return nothing get `NULL`.
- **If logging fails:** a failed log write is printed in the task log and never fails the pipeline.

## Reload one bad day D (Gold)

**Why a rewind is needed.** SCD2, inventory and price are history tables: loading day X closes the old version (`END = X − 1`) and opens a new one (`START = X`). Re-running an old day on top of newer days would corrupt the current rows. So before loading D, Gold is put back to how it looked at the end of D − 1.

**How to do it:**

1. Fix or land the corrected files for D.
2. Backfill **D → latest day** with *All runs* (reprocess).

**What the DAG does on its own** (`rewind_from_ds.sql`, only when D < Gold `LAST_LOAD_DT`):

| Table | Rewind |
|---|---|
| 8 SCD2 dims, `FCT_PRICE` | delete versions with `EFFECTIVE_START_DT ≥ D`; reopen versions with `END ≥ D − 1` (set `END = 9999-12-31`, current) |
| `FCT_INVENTORY` | delete `SNAPSHOT_DT ≥ D`; reopen rows closed `≥ D − 1` |
| `FCT_SALES`, `AGG_DAILY_ITEM_LOCN_SALES` | delete `DAY_KY ≥ D` |
| `C_LOAD_DATES` (Gold rows) | `LAST_LOAD_DT = D − 1` |
| `DIM_DAY`, `DIM_CUSTOMER` | untouched (seed / SCD1, no history); the replay re-applies them |

After that, day D loads normally. D+1 … latest are then newer than the mark, so they replay without a rewind. When `C_LOAD_DATES` is back at the latest day, the reload is done.

Running the rewind twice changes nothing.

Re-running the **latest** day never rewinds; it is idempotent on its own (the rerun proof).

## Defects (deterministic)

| Rule | Meaning |
|---|---|
| `date.toordinal() % 5` | `0` outliers/anomalies · `1` null ITEM ×2 · `2` negative qty · `3` orphan `199999` · `4` duplicate line |
| SCD2 | Store `1008` city move from **2026-11-13** |
| SCD1 | Item `100011` description "(rev Nov)" from **2026-11-14** |

10–14 Nov plants each defect once.

## Evidence and tests

| Path | Purpose |
|---|---|
| `evidence/snowsight/00`–`07` | Copy-paste SQL for Snowsight screenshots (the documentation path; see `context/runbook.md`) |
| `evidence/run_queries.py report --from --to` | One `day_<ds>.md` or `backfill_<from>_to_<to>.md`: verdict, defect trace, layer flow, `C_REJ_TABLE` by rule, SCD, final view, control tables |
| `evidence/run_queries.py snapshot` | Rerun proof on the latest day (`rerun_<ds>.md`) |
| `evidence/run_queries.py reset --yes` | Truncates the data and control tables (keeps DDL and `C_BATCH_SCRIPTS`) |
| `tests/run_all.py` | 15 offline checks, including "`C_BATCH_SCRIPTS` matches the DAG" → `evidence/query-results/tests.txt` |

## Ideas to remember

1. **Run = one business date.** A backfill is many runs, in date order.
2. **Reject ≠ fail.** Bad rows are kept with a reason, and only structural or total problems fail the run.
3. **source = Gold + rejects.** G11 checks rows, amount, orphans, freshness and duplicates every day.
4. **History tables need order.** To fix a past day: rewind Gold to D − 1, then replay forward.
5. **Control tables answer the audit questions:** what ran, with what parameters, how it went, what was rejected, and how current each table is.

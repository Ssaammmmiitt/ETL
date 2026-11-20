# Agent handoff: airflow-medallion

Attach this file at the start of a new chat. Last updated **20 Nov 2026**.

**Status:** Lessons **1–13 done**. Lesson **14 in progress** (ultimate DAG `14_medallion_daily`).
Quick map: [`PROGRESS.md`](PROGRESS.md). DAG ids: `01_` … `14_` (no `lesson` prefix).

## 1. Start here

1. Look at [`architecture-Simple view.png`](architecture-Simple%20view.png): the target. Every name comes from it.
2. Read [`PROGRESS.md`](PROGRESS.md) → this file → [`LESSONS.md`](LESSONS.md) → [`ASSUMPTIONS.md`](ASSUMPTIONS.md) → [`THEORY.md`](THEORY.md) → latest lesson in [`lessons/`](lessons/) (currently [`14_medallion_daily.md`](lessons/14_medallion_daily.md)).
3. Lesson 14 is the stitch: **one ds-driven daily DAG** + Airflow 3 backfill/Trigger (not five TaskGroups in one run). Lesson format as usual.
   DAG naming: `dags/NN_<topic>.py`, `dag_id="NN_<topic>"`, tags `["medallion", "NN"]`.
4. After each lesson: write `context/lessons/NN_<topic>.md`, update `LESSONS.md` + `PROGRESS.md`, THEORY if needed, section 7 here, and `README.md`.

## 2. The learner

- **Sammit Poudyal**, bootcamp student learning Airflow and Snowflake by building. Windows and PowerShell.
- Wants step-by-step teaching, not code dumps. Data goes through three levels: simple → complex → very complex. Wants a large dataset.
- Earlier work in `Bootcamp/2.1`–`2.2` is a Costco star schema. Its diagram was copied into `context/` (`architecture.drawio`, 2 PNG pages, `architecture_simple.md`, `architecture_notes.md`). **These are the learner's notes: do not edit them.**

## 3. Rules for every response

| Rule | Detail |
|---|---|
| Checklist first | Compact running checklist at the top; the full one is in `LESSONS.md`. Never drop a requirement silently |
| Lesson format | (1) concept, (2) code/SQL with paths, (3) observe/verify, (4) exercise, then a **checkpoint**; then **stop** |
| Learner runs everything | *"ill do the manual running work, just make edits and guide me on the execution and meaning of these changes"*. Agent edits files and gives exact PowerShell or Snowsight steps plus what each result means. Local Python checks that never touch Snowflake are fine |
| Keep it simple | No pytest or `tests/`, no Dockerfile, no defensive extras (argparse, regex validation, helper layers). Plain `for` loops, SQL in files, short DAGs |
| Explain | The *why* of each decision and each DAG task; **data types and why** for every table ([THEORY 3.10](THEORY.md#310-data-types-and-why-we-pick-them)) |
| Diagram names | STG (Bronze) / TMP (Silver) / TGT and Aggregate (Gold); the diagram's columns and types; gates G1–G10, plus G11 and G12 (departures) |
| One SQL file per table | `include/sql/ddl/<layer>/<table>.sql`, `load/<layer>/<table>.sql`, `gates/gNN_<rule>.sql`, `checks/`. File names are the table name in lower case |
| No hard-coding in DAGs | No credentials, paths, Snowflake objects or table names. SQL uses `{{ var.json.medallion_config.* }}`. Check in section 6 |
| Docs | All in `context/`. Lessons link as `../THEORY.md#<id>`; THEORY headings are HTML `<h2/h3 id>` |
| PowerShell | Don't use `\"` inside `python -c "..."`; use a script file or single quotes |

## 4. Decisions

| Topic | Decision (why) |
|---|---|
| Airflow | Existing **3.3.2** official image + Docker Compose (CeleryExecutor). Teach the brief's 2.x names next to the 3.x ones |
| History | **Deferred.** L6 generates **five pipeline days only** (~800 sales lines/day). Long Jan–Nov history can be added later if Gold/statistical G12 needs it; L9 initial-load DAG only if/when history exists |
| Sales vs stock | Complex generator: clean sales `ITEM_ID` ∈ store stocked set; **one** planted orphan `199999` for G7 |
| Planted defects | **Calendar buckets** in `generate_complex.py`: `date.toordinal() % 5` exclusive — `0` outliers/anomalies · `1` null ITEM_ID ×2 (D1) · `2` neg qty (D2) · `3` orphan `199999` (D3) · `4` dup line (D4). **D5 SCD2** separate: `LOCN 1008` CITY_ID Spokane→Portland from **`SCD2_FROM` = 2026-11-13** onward. **SCD1**: item `100011` desc "(rev Nov)" from **`SCD1_FROM` = 2026-11-14**. Stock replayed from `STOCK_ANCHOR` = 2026-11-10 (deterministic per date). Each planted line appends to `data/store/defect_log.txt` (includes **store** id). Per-store txns still cycle 12/14/16/18/20 (400 total). Nov 10–14 map: outliers → null → neg → orphan → dup (buckets 0–4); 20 Nov = outliers |
| Evidence / tests | `evidence/sql/r1–r4*.sql` + `evidence/run_queries.py` (`report` → ONE file per run: `day_<ds>.md` / `backfill_<from>_to_<to>.md` with verdict table first; `snapshot` → `rerun_<ds>.md`; `reset --yes`) → `evidence/query-results/`; screenshots in `evidence/screenshots/`; `tests/run_all.py` (13 checks, no pytest) → `evidence/query-results/tests.txt`. **UI trigger pitfall:** logical date is entered in local time (UTC+5:45) → 20 Nov 00:00 local = 19 Nov UTC = `ds` 2026-11-19; set the UI timezone to UTC or use `dags test`. For today's date, **Clear** the auto-created `scheduled__<today>` run instead of triggering a new one. **Rerun proof = latest loaded day only** (14 Nov): re-running an older day after newer ones breaks SCD2/soft-close "current" rows (seen 20 Nov with 12 Nov rerun). Compose mounts `./evidence`, `./tests`. Both 14 DAGs `retries=1` |
| Dates / L14 runs | Demo window still **10–14 Nov 2026** (add 15 for orphan). DAG `14_medallion_daily`: `schedule="@daily"`, `catchup=False`, `is_paused_upon_creation=True`, `max_active_runs=1`. First task **`generate_landing_day`** writes **this `ds` only**. Drive dates via UI Backfill/Trigger or `airflow backfill create --dag-id 14_medallion_daily --from-date … --to-date …`. Do **not** leave logical date as “today” unless you intend that day |
| Stages | Internal stage `@my_int_stage` only. **External stage and storage integration dropped** by the learner on 18 Nov (no S3/Azure/GCS): departure 9. Don't teach or write them; list it in README limitations |
| Auth | Key pair, `AIRFLOW_SVC` `TYPE = SERVICE`; key file `secrets/rsa_key.p8` mounted read-only; connection extra `private_key_file` |
| Connections | Imported from `secrets/connections.json` (template `include/config/connections.example.json`) |
| Waiting | Teach FileSensor and Assets; **use FileSensor** (mapped ×4, `mode='reschedule'`) plus a `LIST @stage` check |
| Email | Mailpit container in L11, `smtp_default` |
| `@task.snowflake` | Doesn't exist: use `SQLExecuteQueryOperator` or `@task` + `SnowflakeHook` |
| Tests | Replaced by `airflow dags list-import-errors`, the `Select-String` check, and DQ/recon SQL run as DAG tasks |
| Schemas | `BRONZE`, `SILVER`, `GOLD`, `AUDIT`. `TMP_QUARANTINE` in SILVER; recon results in AUDIT |
| Departures ([THEORY 1.4](THEORY.md#14-departures-and-where-the-brief-and-the-diagram-differ)) | (1) DIM_DAY seed goes straight to Gold. (2) `ITEM_DESC` is SCD1 inside SCD2 `DIM_ITEM`. (3) `FCT_INVENTORY` soft-close (`EFFECTIVE_END_DT`, `CURRENT_FLG`). (4) Silver = transient tables, not Parquet. (5) A corrected file replaces day D in STG (bad file kept in archive). (6) G11 reconciliation. (7) Brief "district change" = store moves `CITY_ID`. (8) G12 configurable outlier limits. (9) No external stage (internal only) |

**Airflow 2.x → 3.3 names:**

| Brief (2.x) | 3.3 |
|---|---|
| `airflow.decorators` | `airflow.sdk` (`dag`, `task`, `Asset`, `Variable`) |
| `SnowflakeOperator` | `SQLExecuteQueryOperator` (`airflow.providers.common.sql.operators.sql`) |
| `airflow.sensors.filesystem.FileSensor` | `airflow.providers.standard.sensors.filesystem.FileSensor` |
| `airflow.operators.email.EmailOperator` | `airflow.providers.smtp.operators.smtp.EmailOperator` |
| `execution_date`, `schedule_interval`, `airflow dags backfill` | `logical_date`/`ds`, `schedule`, `airflow backfill create` |

## 5. Environment

- **Machine:** Windows build 26200, PowerShell, Python 3.14 locally (no Airflow locally), Docker Desktop (start it first), Git for Windows (`openssl` in `%LOCALAPPDATA%\Programs\Git\usr\bin`). No SnowSQL or Astro CLI: SQL runs in **Snowsight**.
- **Other stack:** `Bootcamp\day3-airflow` also uses port 8080. Stop it with `docker compose -f ..\day3-airflow\docker-compose.yaml down`.
- **Project:** `C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion`.
  - Compose `name: airflow-medallion` with `PYTHONPATH: /opt/airflow`.
  - Mounts: `./include`, `./data`, `./secrets:ro`.
  - UI: <http://localhost:8080> (`airflow` / `airflow`).
  - CLI: `docker compose run --rm airflow-cli airflow <cmd>`.
  - Parse check: `airflow dags list-import-errors` → `No data found`.
- **Snowflake** (`include/sql/setup/00_bootstrap.sql`): `MEDALLION_WH` (XSMALL, 60 s suspend), `MEDALLION_DB`, schemas owned by `MEDALLION_ROLE`, user `AIRFLOW_SVC`, account id `ORG-ACCOUNT`, ACCOUNTADMIN available.

## 6. Code conventions and files

- **`include/medallion/config.py`:** `SNOWFLAKE_CONN_ID="snowflake_medallion"`, `LANDING_FS_CONN_ID="fs_landing"`, `CONFIG_VARIABLE="medallion_config"`, `SQL_DIR`, and `SOURCES = ["sales","inventory","item_master","store_master"]`. Source `<source>_YYYYMMDD.csv` maps to `STG_<SOURCE>`.
- **Variable `medallion_config`** (`include/config/variables.json`) has: `database`, `warehouse`, `schemas`, `landing_path`, `archive_path`, `seed_path`, `alert_email`, and `outlier_rules`:
  `{price_ratio_max 10, price_ratio_min 0.1, max_sold_qty 1000, max_line_net_amt 50000, max_on_hand_qty 100000}`.
- **Every DAG:** `template_searchpath=[str(SQL_DIR)]`, `retries >= 1`, `tags`, no top-level `Variable.get()`, lowercase task ids.
- **SQL:** `{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES`. **Never put `{{ }}` in a SQL comment** (Jinja renders comments).
- **Hard-coding check** (no output = pass):
  `Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'`

```
airflow-medallion/
├── README.md  docker-compose.yaml  .env  .env.example  .gitignore
├── context/      AGENT_CONTEXT, LESSONS, ASSUMPTIONS, THEORY, architecture-*.png/.drawio/.md, lessons/01_environment.md, 02_stg_tables.md, 03_stages_copy.md
├── dags/         01_connection_check.py … 05_bronze_load.py (dag_id = file stem; no "lesson" prefix)
├── include/medallion/config.py   include/config/variables.json, connections.example.json
├── include/sql/  setup/00_bootstrap.sql, 01_file_formats.sql, 02_stages.sql
│                 ddl/bronze/stg_*.sql (4) · load/bronze/stg_*.sql (4 COPY) · stage/put_file.sql (params.source)
│                 checks/whoami.sql, list_stage.sql
├── scripts/      generate_simple.py (L2), generate_complex.py (calendar defects), generate_seed_dim_day.py
├── data/         landing/ · archive/ · store/defect_log.txt (append-only; gitignored *.txt) · seed/seed_dim_day.csv
└── secrets/  plugins/
```

**STG tables:**
- Every file column is `VARCHAR`, in file order.
- Plus 5 lineage columns, all `NOT NULL`:
  - `_INGEST_DT DATE` (= `ds`, the partition)
  - `_SOURCE_FILE VARCHAR`
  - `_SOURCE_ROW_NUM NUMBER`
  - `_INGEST_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()`
  - `_BATCH_ID VARCHAR` (= `run_id`)
- Column counts (file columns + 5): SALES 16 → 21, INVENTORY 8 → 13, ITEM_MASTER 15 → 20, STORE_MASTER 14 → 19.
- `CREATE OR REPLACE` only while empty; use `ALTER` once loaded (they hold data from L3).

**Stage and COPY (L3):**
- `MY_INT_STAGE` in BRONZE (`CREATE STAGE IF NOT EXISTS`), one folder per date: `MY_INT_STAGE/2026-11-10/<source>_20261110.csv.gz`.
- PUT: `'file://{landing_path}/{params.source}_{ds_nodash}.csv'`, `AUTO_COMPRESS = TRUE`, `OVERWRITE` from `params.overwrite` (`FALSE` in L3/L4, `TRUE` in L5).
- **All 4 COPYs are transforming** (`SELECT $1..$n` + lineage). L5 load SQL is idempotent: `DELETE WHERE _INGEST_DT = ds` then `COPY ... FORCE = TRUE` (`split_statements=True`).
- G2: `SOURCE_CONTRACTS` in `config.py`; `g2_check_file` uses Python `csv` on the landing file (transforming COPY ignores column-count mismatch).
- Archive: after successful loads, `data/landing` → `data/archive/<ds>/`.

**THEORY anchors:** THEORY now uses plain Markdown headings (no HTML ids). Link with GitHub-style slugs, e.g. `../THEORY.md#34-file-formats-and-copy-into`, `#310-data-types-and-why-we-pick-them`, `#56-outliers-vs-anomalies-and-the-configurable-gate-g12`.

**TMP/TGT types (diagram):**

| Kind | Type |
|---|---|
| ids | `VARCHAR(50)` |
| names | `VARCHAR(100–200)` |
| money | `NUMBER(18,2)` |
| qty | `NUMBER(18,3)` (weighed items) |
| keys | `NUMBER(38,0)` |
| `DAY_KY` | `NUMBER(8,0)` |
| `LINE_NUM` | `NUMBER(10,0)` |
| flags | `BOOLEAN` (files send Y/N) |
| `RAW_RECORD` | `VARIANT` |

**Source layouts** (headers in upper case, diagram names):

| File | Columns | Notes |
|---|---|---|
| `sales_` | `TRANSACTION_ID, LINE_NUM, SALE_DT, LOCN_ID, ITEM_ID, CUSTOMER_ID, CUSTOMER_NM, CUSTOMER_TYPE, EMAIL_TXT, PHONE_NUM, SOLD_QTY, UNIT_PRICE_AMT, DISCOUNT_AMT, SALES_AMT, TAX_AMT, NET_SALES_AMT` | header, delta |
| `inventory_` | `SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY, UNIT_PRICE_AMT, PROMO_PRICE_AMT` | **no header** → positional COPY |
| `item_master_` | `ITEM_ID, ITEM_NM, ITEM_DESC, BRAND_NM, ITEM_TYPE, SUBCATEGORY_ID/NM/DESC, CATEGORY_ID/NM/DESC, DEPARTMENT_ID/NM/DESC, ACTIVE_FLG` | header, full |
| `store_master_` | `LOCN_ID, LOCN_NM, LOCN_TYPE, ADDRESS_TXT, POSTAL_CD, CITY_ID, CITY_NM, STATE_PROVINCE_NM, COUNTRY_NM, REGION_ID/NM, DIVISION_ID/NM, ACTIVE_FLG` | header, full |
| `seed_dim_day.csv` | `DAY_KY, CALENDAR_DT, DAY_OF_MONTH, DAY_OF_WEEK_NUM, DAY_OF_WEEK_NM, WEEK_OF_YEAR_NUM, MONTH_NUM, MONTH_NM, QUARTER_NUM, YEAR_NUM, WEEKEND_FLG, HOLIDAY_FLG` | 730 days 2026–2027 |

## 7. Data facts and current status

### Lesson 2 data (`generate_simple.py`, 2026-11-10, deterministic)

**Row counts:**
- 5,000 sales lines: 3,975 member, 1,025 non-member (every 5th transaction, customer columns empty).
- 1,502 inventory rows.
- 101 items: D01 FOOD, D02 NON_FOOD, D03 PERISHABLE (weighed, qty like 0.750); promo on every 10th item at price × 0.9.
- 25 stores: `LOCN_ID` 1001–1025, cities CT01–CT25, regions R01–R12, divisions DV1–DV5.

**Rules every row obeys:**
- `SALES = QTY × PRICE`.
- `NET = SALES − DISCOUNT`; the discount is capped so net is never below 0.
- Tax is 8% of net.
- `RESERVED ≤ ON_HAND`.

**Totals:**

| Measure | Amount |
|---|---|
| `SALES_AMT` | 2,844,752.96 |
| `DISCOUNT_AMT` | 964.45 |
| `NET_SALES_AMT` (file) | **2,843,788.51** |
| Clean net, expected in Gold | 204,298.00 |
| Net of the anomalies | 2,639,490.51 |

G6: 0 violations; no negative net.

**Valid outliers (must reach Gold):**
- Premium item 100101 at 2,499.99: 2 real lines, stocked at 1001 and 1013.
- Item 100045 at 0.05; 12 lines have zero net.
- 480-unit bulk buy (`T20261110-1001-0050` line 3).
- 100% discount (`T20261110-1002-0001` line 1).
- 0.005 kg sale (`T20261110-1002-0026` line 2).
- Stock 0 (100011@1001), stock 25,000 (100021@1001), and reserved = on hand (100031@1001).

**Planted anomalies (gate G12 solves them in L7; reason codes in [THEORY 5.6](THEORY.md#56-outliers-vs-anomalies-and-the-configurable-gate-g12)):**

| # | Row | Problem | Reason code |
|---|---|---|---|
| A1 | `T20261110-1006-0001` line 1, item 100101 | Price 2,499,990.00 (×1000) | `PRICE_TOO_HIGH` + `LINE_AMT_TOO_HIGH` |
| A2 | `T20261110-1011-0001` line 1, item 100016 | 10,000 units | `QTY_TOO_HIGH` + `LINE_AMT_TOO_HIGH` |
| A3 | `T20261110-1016-0001` line 1, item 100021 | 0.17 instead of 17.50 (÷100) | `PRICE_TOO_LOW` |
| A4 | Inventory row for 100041@1001 | On hand 9,999,999 | `ON_HAND_TOO_HIGH` |

- The reference price for the ratio rule is the item's `UNIT_PRICE_AMT` in the same day's inventory. Every sold item has one.
- A local simulation of G12 flagged exactly A1–A4 and no valid outlier.

### Status

| Lesson | Status (18 Nov 2026) |
|---|---|
| 1 Environment | Done |
| 2 Simple data + STG DDL | Done |
| 3 Stage PUT/LIST/COPY | Done |
| 4 FileSensor + G1 | Done |
| 5 Bronze DELETE+FORCE, G2, archive | Done (checkpoint passed) |
| 6 History / complex generator | Done (19 Nov 2026) |
| 7 Silver TMP + G3–G6 + G12 | Done (19 Nov 2026) |
| 8 Gold dimensions | Done (19 Nov 2026) |
| 9 Gold facts + G7/G8 | Done (19 Nov 2026) |
| 10 Prove defects + PK/G10 | Done (19 Nov 2026) |
| 11 Aggregate + recon + alert | Done (19 Nov 2026) |
| 12 Idempotency + reload | Done (19 Nov 2026; reload drill deferred) |
| 13 Five-day backfill | Done (19 Nov 2026) |
| 14 Ultimate medallion DAG | **In progress** |

**Lesson 13 closed:** five consecutive days 10–14; evidence green.

**Lesson 14 artifacts (Option 1 — single-day + backfill):**  
- `dags/14_medallion_daily.py` is the **only DAG** (20 Nov: lesson DAGs 01–13, `14_generate_source`, `scripts/run_pipeline_days.ps1` deleted; 01–13 recoverable with `git restore dags/<file>`). Shape: `setup_platform` (all DDL IF NOT EXISTS via `runner.ddl_files()`, one session) → `generate_landing_day` → `wait_for_files` → bronze (mapped) → silver → gold_dims → gold_facts → reconcile; `max_active_runs=1`; starts paused. SQL tasks built by `sql_task(path)` (task_id = `load_<table>` / file stem); Snowflake helpers in `include/medallion/runner.py` (`run_sql_files`, `fetch`, `g2_check`, `list_stage`, `generate_day`). Gate tasks (20 Nov): G1 `wait_for_files` + `bronze.assert_stage_g1` + no-data-row check in `g2_check`; G2 `bronze.ingest_one`; G3–G6/G12 inside `load_tmp_sales_line` (G12 on-hand inside `load_tmp_item_loc_snapshot`); `silver.assert_sales_identity`; `silver.g06b_alert`; `gold_facts.g07_keys_resolve` + `g07_alert`; `gold_facts.g08_inventory_count` (`G8_MAX_DRIFT` in config.py); `gold_facts.g10_price_overlap`; `reconcile.assert_recon_day` (G9 + G11); `reconcile.assert_primary_keys` (13 `pk_*.sql`). Alerts are task log lines (no email yet). Unused SQL deleted (whoami, g01_list_stage, g02_column_count, g01_files_have_rows, idem_day_snapshot DDL); test `every_sql_file_is_used` guards it.  
- **Fresh-start rework (20 Nov, afternoon):** DAG renamed `retail_medallion_daily` (file `dags/retail_medallion_daily.py`; old `14_medallion_daily` metadata to delete). `TMP_QUARANTINE` replaced by physical `AUDIT.C_REJ_TABLE` (same columns; written by Silver gates + G7). `RECON_DAILY_NET_SALES` replaced by `AUDIT.RECON_DAILY_CHECKS` (5 rows/day: ROW_COUNT, AMOUNT, ORPHANS (`ORPHAN_MAX_PCT` 1 % in config.py, DAG param), FRESHNESS, DUPLICATES); task `reconcile.assert_reconciliation` fails on any non-PASS. Bronze DDL fixed from `CREATE OR REPLACE` to `IF NOT EXISTS` (it wiped STG history every run). `setup/00_schemas.sql` lets `setup_platform` recreate dropped schemas. Rewind writes `C_PARAM` `GOLD_REWOUND_FROM`. Copy-paste Snowsight SQL in `evidence/snowsight/00–07`; run order and screenshot names in `context/runbook.md`. Only the Simple view page of the drawio was updated; page 1 still shows TMP_QUARANTINE / Parquet / manifest.  
- **Control tables (20 Nov, AUDIT schema):** `C_BATCH_SCRIPTS` (seed `load/audit/c_batch_scripts.sql`, task `load_c_batch_scripts`; test `batch_scripts_registry_matches_dag` keeps it equal to the DAG), `C_PARAM` (`load_c_param`), `C_BATCH_LOG` (default_args callbacks `runner.log_success/log_failure/log_retry` → `load/audit/c_batch_log.sql`, never raise), `C_REJ_TABLE` (view `ddl/audit/vw_c_rej_table.sql` over `TMP_QUARANTINE`), `C_LOAD_DATES` (final task `load_c_load_dates`). **Reload one bad day:** `gold_dims.rewind_gold_for_reload` → `runner.rewind_gold_if_past_day` runs `load/gold/rewind_from_ds.sql` when ds < Gold `LAST_LOAD_DT`; then replay D → latest. Task-by-task notes: `final_state.md`.  
- `scripts/generate_complex.py`: `defect_bucket` / `append_defect_log`; `generate_pipeline_days([ds], …)`.  
- `data/store/defect_log.txt`: append-only defect audit (store-tagged).  
- Lesson [`14_medallion_daily.md`](lessons/14_medallion_daily.md). Multi-day via UI Backfill / `airflow backfill create` / serial Trigger or `dags test` — **not** five TaskGroups in one run.  
- Airflow 3: `schedule=None` **cannot** backfill (`DagNonPeriodicScheduleException`); need `@daily` (or cron). Wipe history: `airflow dags delete 14_medallion_daily -y`.

**To close Lesson 14**, learner replies with checkpoint from [`lessons/14_medallion_daily.md`](lessons/14_medallion_daily.md) §14.9.

## 8. Plan for the remaining lessons

| # | Lesson | Key outputs |
|---|---|---|
| 3 | Stages (**written**) | See "Stage and COPY (L3)" in section 6 |
| 4 | Waiting (G1) (**written**) | `dags/04_waiting.py`: FileSensor.partial.expand ×4; PUT; `assert_files_on_stage` (LIST + size); Assets taught in notes only |
| 5 | Bronze (**written**) | `dags/05_bronze_load.py`: wait → PUT overwrite → G1 assert → G2 csv contract → DELETE+FORCE load → archive |
| 6 | Generator v2 (**written**; defects updated 20 Nov) | `scripts/generate_complex.py`: landing CSVs; ~800 lines/day; sales⊆stocked; defects = **`toordinal()%5`** + SCD2 from 13 Nov + SCD1 from 14 Nov; `data/store/defect_log.txt`; history deferred; lesson [`06_history_generator.md`](lessons/06_history_generator.md) (classic window notes may lag — L14 is source of truth for buckets) |
| 7 | Silver TMP (**written**, closed) | DDL + loads + `07_silver`: G3–G6 + G12; G6b alert; identity; lesson [`07_silver.md`](lessons/07_silver.md) |
| 8 | Gold dimensions (**written**, closed) | Sequences; IF NOT EXISTS dims; SCD2/SCD1/seed; DAG `08_gold_dims`; lesson [`08_gold_dims.md`](lessons/08_gold_dims.md) |
| 9 | Gold facts (**written**, closed) | `FCT_*` + G7/G8; DAG `09_gold_facts`; lesson [`09_gold_facts.md`](lessons/09_gold_facts.md) |
| 10 | Prove defects + PK/G10 (**written**, closed) | `10_prove_defects`: D1–D5 asserts; `pk_*.sql`; G10; lesson [`10_prove_defects.md`](lessons/10_prove_defects.md) |
| 11 | Aggregate + recon (**written**, closed) | AGG + VW; G9; G11 recon; return-flag branch; Mailpit optional; lesson [`11_agg_recon.md`](lessons/11_agg_recon.md) |
| 12 | Idempotency + reload (**written**, closed) | Fingerprint + `12_idempotency`; reload drill deferred; [`12_idempotency.md`](lessons/12_idempotency.md) |
| 13 | Five-day backfill (**written**, closed) | Script + `13_backfill_evidence`; 10–14 Nov; [`13_backfill.md`](lessons/13_backfill.md) |
| 14 | Ultimate DAG (**written**, Option 1; gen=ds 20 Nov) | `generate_landing_day` + mapped bronze…reconcile; calendar defects + store log; UI/CLI backfill; [`14_medallion_daily.md`](lessons/14_medallion_daily.md) |

**Target DAG** (full ASCII in `LESSONS.md`):

```
wait_for_file[x4] (G1) → put_to_stage → assert_files_on_stage → load_stg.expand(x4) (G2) → archive_files
  → tmp_* (G3–G6, G12) → quarantine_summary
  → dims: department→category→subcategory→item ‖ division→region→city→locn ‖ customer ‖ day
  → G7, G8 → fct_sales ‖ fct_inventory ‖ fct_price → pk_checker, G10
  → agg → G9 → reconcile G11 → branch → recon_passed | send_alert → fail_run
```

## 9. Gotchas

- Airflow 3 parses DAGs in `airflow-dag-processor`; new DAGs appear after about 30 s.
- Don't change `FERNET_KEY` after importing connections.
- COPY load history skips files already loaded (64 days): use `DELETE WHERE _INGEST_DT` + `FORCE = TRUE`.
- Encode the space in links: `architecture-Simple%20view.png`.
- Snowsight bootstrap:
  - `IDENTIFIER($my_user)` needs the user name double-quoted.
  - `DESC USER` must run as `SECURITYADMIN`.
  - `RSA_PUBLIC_KEY` takes the one-line body only, without the `-----BEGIN` lines.
- Outlier limits are data, not code: change them in `variables.json`, then re-import the Variable.
- `PUT` / `GET` SQL must start with the command (no leading `--` comments). Otherwise snowflake-connector returns `Unsupported feature 'unsupported_requested_format:snowflake'` because it fails to intercept the upload client-side (hit in L3, 18 Nov).
- Jinja still renders SQL comments: never put `{{ }}` inside a `--` comment.

## 10. Original brief (verbatim)

<details>
<summary>Click to expand the learner's original prompt</summary>

> **ROLE**
>
> You are a senior data engineer and instructor who teaches by building. I am learning Apache Airflow and Snowflake. I want to **learn while building** a production‑grade Airflow pipeline end‑to‑end. Do **not** just dump code — teach me step by step, explain *why* each decision is made, and let me build alongside you. You need to teach me step by step first using simple data and generate it then know the concepts then use complex data then learn then again using very complex data. I need all lessons well maintained.
>
> **TEACHING STYLE (IMPORTANT)**
>
> - Teach in **numbered, incremental lessons**. Each lesson: (1) concept explained simply, (2) the code/SQL, (3) what to observe/verify, (4) a small exercise for me.
> - At the end of each lesson, give me a **checkpoint** ("run this, you should see X").
> - Never skip a requirement. Maintain a **running checklist** at the top of every response showing what's covered and what's pending.
> - Prefer **Airflow 2.7+ TaskFlow API** with `@task`, `@task.snowflake` or `SnowflakeOperator`, and `Dynamic Task Mapping`.
> - Show **file paths** for every artifact: `dags/`, `include/sql/`, `tests/`, `README.md`.
> - Any SQL must be **one file per target table** under `include/sql/`.
> - **No credentials, file paths, or table names hard‑coded in DAG files** — use Airflow Connections, Variables, and Jinja templating (`{{ var.value.X }}`, `{{ conn.snowflake... }}`).
>
> ---
>
> **WHAT WE ARE BUILDING**
>
> An Airflow DAG that loads **five consecutive days of source files** (backfilled in one run, 10–14 Nov, 5 days) through a **Bronze → Silver → Gold** medallion architecture into Snowflake, with full reconciliation. The DAG must be **idempotent** — safe to run twice for the same date with identical row counts and totals.
>
> **STAGES:** `wait` → `bronze` → `silver` → `gold` → `reconcile`
>
> **TASKS:**
> 1. **Sensor** on all four files (with timeout)
> 2. **Mapped ingest** — one ingest task per file
> 3. **Clean, dedupe, validate** (Silver)
> 4. **Dimensions first, then facts** (Gold)
> 5. **Reconciliation task that FAILS the DAG** if Gold does not tie to source
>
> ---
>
> **SOURCE FILES (you must design realistic, complex curated data)**
>
> | File | Frequency | Feeds |
> |---|---|---|
> | `item_master_YYYYMMDD.csv` | Daily snapshot (weekly semantics) | DIM_ITEM, DIM_SUBCLASS |
> | `store_master_YYYYMMDD.csv` | Daily snapshot (weekly semantics) | DIM_LOCN, DIM_DISTRICT |
> | `pos_sales_YYYYMMDD.csv` | Daily transactions | FCT_SALES |
> | `inventory_snapshot_YYYYMMDD.csv` | Daily position | FCT_INVENTORY |
>
> Generate **rich, complex but consistent** sample data for 5 dates. Include realistic hierarchies (subclass → class → dept; store → district → region), plausible item/store keys, prices, quantities, and positions.
>
> **PLANT THESE FIVE DEFECTS (and show how the pipeline handles each):**
> 1. Two rows with **null item id** → quarantined, counted, run continues
> 2. One row with **negative sales quantity** → quarantined / flagged
> 3. One sale for an **item missing from item master** → orphan handling
> 4. One **duplicated transaction line** → deduped in Silver
> 5. One **store whose district changes** → triggers **SCD2** event on DIM_LOCN
>
> ---
>
> **SNOWFLAKE REQUIREMENTS (teach these explicitly)**
>
> - **STAGES**: use both **Internal stage** (`@my_int_stage`) and **External stage** (`@my_ext_stage` with storage integration). Explain the **`LIST @stage`** concept and use it in the sensor/validation logic (check files land before ingest).
> - **Waiting mechanism**: prefer **FileSensor** for the four files (with `timeout`, `poke_interval`, `mode='reschedule'`), OR **Airflow Datasets/Assets** — explain the trade‑off and pick one, teaching both.
> - **Dimensional load**:
>   - **DIM_ITEM** and **DIM_LOCN** with **surrogate key generation** (sequences/hash).
>   - **SCD2** for DIM_LOCN (district change), **SCD1** in‑place update for at least one DIM_ITEM attribute.
>   - **Positional load** demonstrated for at least one file (fixed column order → `COPY INTO` with positional mapping).
>   - **Hierarchy maintained correctly** (parent → child keys preserved).
> - **Parallel rolling loads** across different dimension tables (use Dynamic Task Mapping / task groups).
> - **Joining & rollup setup** clearly shown for the fact builds.
> - **Silver → Gold** use **`NOT EXISTS`** (anti‑join) for incremental inserts — explain idempotency rationale.
> - **PK checker** maintained (a DQ task asserting uniqueness on natural keys per table).
> - **FCT_INVENTORY rows are closed, not deleted** — soft‑close via `valid_to` / `is_current`.
> - **NULL handling** everywhere (COALESCE, `NULLIF`, explicit cast rules).
>
> ---
>
> **RECONCILIATION & ALERTING**
>
> - A **reconciliation task** that ties **Gold net sales** back to source files for **all five days**.
> - It must produce a **two‑column, five‑row** comparison table (e.g., `date | variance`) and **fail the DAG** if any variance ≠ 0.
> - On rejection / mismatch, emit an **alert** (email via `EmailOperator` or `on_failure_callback` with SMTP connection) — demonstrate the **return flag** pattern (task returns bool → downstream branch).
>
> ---
>
> **DELIVERABLES (create these files)**
>
> ```
> dags/            # DAG file(s), no cycles, retries set, parses cleanly
> include/sql/     # one SQL file per table load (bronze, silver, dims, facts, reconcile)
> tests/           # unit tests for the DAG + DQ checks
> README.md        # how to run, how to backfill, known limitations, evidence
> ```
>
> Evidence must include: backfill screenshot description + a **reconciliation output table (2 columns, 5 rows)**.
>
> ---
>
> **ACCEPTANCE CRITERIA (validate every one)**
>
> 1. A **second run** for the same date changes **no counts and no totals**
> 2. The **five‑day backfill (10–14 Nov)** completes with **every task green**
> 3. **Broken records are quarantined and counted** and do **not** stop the run
> 4. **No credentials, paths, or table names hard‑coded** in DAG files
> 5. **Gold net sales ties to source files for all five days**
>
> ---
>
> **AIRFLOW / SNOWFLAKE IDEMPOTENCY NOTES TO TEACH**
>
> - MERGE vs INSERT‑OVERWRITE vs `NOT EXISTS` — when to use which
> - Deterministic surrogate keys vs sequence keys (impact on re‑runs)
> - SCD2 `valid_from`/`valid_to` recompute logic on re‑run
> - Partition pruning by `load_date`
> - `COPY INTO` with `FORCE=TRUE` vs dedupe via staging
>
> ---
>
> **HOW TO START**
>
> 1. First response: confirm the full requirement checklist, propose the **folder structure**, and propose the **DAG graph** (ASCII). Ask me only the minimum questions needed (e.g., Airflow version, Snowflake edition if it affects stages).
> 2. Then teach **Lesson 1: environment + connections + variables** and stop for my checkpoint.
> 3. Continue lesson‑by‑lesson until everything on the checklist is built, tested, and reconciled.
> 4. Never move to the next lesson until I confirm the checkpoint passes.
>
> **Stay on the running checklist in every response. Do not skip, summarise away, or defer any requirement listed above.**
>
> ---
>
> CREATE A MD FILE IF NEEDED FOR THIS

Follow-up request: *"create a md file storing my prompt, your response and other relevant context... everything that would be needed when i start another chat with an agent or llm."*

</details>

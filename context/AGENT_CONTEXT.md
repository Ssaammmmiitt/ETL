# Agent handoff: airflow-medallion

Attach this file at the start of a new chat. Last updated **17 Nov 2026**: Lesson 1 done,
Lesson 2 written (with valid outliers + planted anomalies) and **waiting for the learner's checkpoint**.

## 1. Start here

1. Look at [`architecture-Simple view.png`](architecture-Simple%20view.png): the target. Every name comes from it.
2. Read this file → [`LESSONS.md`](LESSONS.md) (checklist, curriculum) → [`ASSUMPTIONS.md`](ASSUMPTIONS.md) → [`THEORY.md`](THEORY.md) → latest file in [`lessons/`](lessons/).
3. **Do not start Lesson 3** until the learner pastes the Lesson 2 checkpoint (section 7).
4. After each lesson: write `context/lessons/NN_<topic>.md`, update the `LESSONS.md` checklist and status, fix or add concepts in `THEORY.md`, update section 7 here, and keep `README.md` (project root) in sync.

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
| Explain | The *why* of each decision and each DAG task; **data types and why** for every table ([THEORY 3.10](THEORY.md#sec-3-10-data-types)) |
| Diagram names | STG (Bronze) / TMP (Silver) / TGT and Aggregate (Gold); the diagram's columns and types; gates G1–G10, plus G11 and G12 (departures) |
| One SQL file per table | `include/sql/ddl/<layer>/<table>.sql`, `load/<layer>/<table>.sql`, `gates/gNN_<rule>.sql`, `checks/`. File names are the table name in lower case |
| No hard-coding in DAGs | No credentials, paths, Snowflake objects or table names. SQL uses `{{ var.json.medallion_config.* }}`. Check in section 6 |
| Docs | All in `context/`. Lessons link as `../THEORY.md#<id>`; THEORY headings are HTML `<h2/h3 id>` |
| PowerShell | Don't use `\"` inside `python -c "..."`; use a script file or single quotes |

## 4. Decisions

| Topic | Decision (why) |
|---|---|
| Airflow | Existing **3.3.2** official image + Docker Compose (CeleryExecutor). Teach the brief's 2.x names next to the 3.x ones |
| Dates | Weekdays **10, 11, 12, 13, 16 Nov 2026**; schedule `0 6 * * 1-5`; `airflow backfill create --dag-id medallion_daily --from-date 2026-11-10 --to-date 2026-11-16`; `max_active_runs=1` (SCD2 and soft-close depend on day order) |
| Stages | `@my_int_stage` runs the pipeline. `@my_ext_stage` + storage integration are written and explained but not run (no cloud bucket) |
| Auth | Key pair, `AIRFLOW_SVC` `TYPE = SERVICE`; key file `secrets/rsa_key.p8` mounted read-only; connection extra `private_key_file` |
| Connections | Imported from `secrets/connections.json` (template `include/config/connections.example.json`) |
| Waiting | Teach FileSensor and Assets; **use FileSensor** (mapped ×4, `mode='reschedule'`) plus a `LIST @stage` check |
| Email | Mailpit container in L11, `smtp_default` |
| `@task.snowflake` | Doesn't exist: use `SQLExecuteQueryOperator` or `@task` + `SnowflakeHook` |
| Tests | Replaced by `airflow dags list-import-errors`, the `Select-String` check, and DQ/recon SQL run as DAG tasks |
| Schemas | `BRONZE`, `SILVER`, `GOLD`, `AUDIT`. `TMP_QUARANTINE` in SILVER; recon results in AUDIT |
| Departures ([THEORY 1.4](THEORY.md#sec-1-4-departures)) | (1) DIM_DAY seed goes straight to Gold. (2) `ITEM_DESC` is SCD1 inside SCD2 `DIM_ITEM`. (3) `FCT_INVENTORY` soft-close (`EFFECTIVE_END_DT`, `CURRENT_FLG`). (4) Silver = transient tables, not Parquet. (5) A corrected file replaces day D in STG (bad file kept in archive). (6) G11 reconciliation. (7) Brief "district change" = store moves `CITY_ID`. (8) G12 configurable outlier limits |

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
├── context/      AGENT_CONTEXT, LESSONS, ASSUMPTIONS, THEORY, architecture-*.png/.drawio/.md, lessons/01_environment.md, 02_stg_tables.md
├── dags/         lesson01_connection_check.py (whoami >> check_schemas)
│                 lesson02_stg_setup.py (create_file_formats >> 4 × create_stg_<source>)
├── include/medallion/config.py   include/config/variables.json, connections.example.json
├── include/sql/  setup/00_bootstrap.sql, 01_file_formats.sql · ddl/bronze/stg_*.sql (4) · checks/whoami.sql
├── scripts/      generate_simple.py (L2), generate_seed_dim_day.py
├── data/         landing/ (git-ignored) · archive/ · seed/seed_dim_day.csv (versioned)
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
- `CREATE OR REPLACE` only while empty; use `ALTER` after L5.

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

**Planted anomalies (gate G12 solves them in L7; reason codes in [THEORY 5.6](THEORY.md#sec-5-6-outliers)):**

| # | Row | Problem | Reason code |
|---|---|---|---|
| A1 | `T20261110-1006-0001` line 1, item 100101 | Price 2,499,990.00 (×1000) | `PRICE_TOO_HIGH` + `LINE_AMT_TOO_HIGH` |
| A2 | `T20261110-1011-0001` line 1, item 100016 | 10,000 units | `QTY_TOO_HIGH` + `LINE_AMT_TOO_HIGH` |
| A3 | `T20261110-1016-0001` line 1, item 100021 | 0.17 instead of 17.50 (÷100) | `PRICE_TOO_LOW` |
| A4 | Inventory row for 100041@1001 | On hand 9,999,999 | `ON_HAND_TOO_HIGH` |

- The reference price for the ratio rule is the item's `UNIT_PRICE_AMT` in the same day's inventory. Every sold item has one.
- A local simulation of G12 flagged exactly A1–A4 and no valid outlier.

### Status

- **Lesson 1 done.** Project simplified (no pytest), restructured to the diagram, docs moved to `context/`.
- **Lesson 2 checkpoint pending.** The learner runs steps 2.3–2.6, including dropping the old `BRONZE.ITEM_MASTER / STORE_MASTER / POS_SALES / INVENTORY_SNAPSHOT`.

**Expected checkpoint replies:**
1. Generator: 5000 / 1502 / 101 / 25 rows, net `2843788.51`, `extremes:` line; seed 730.
2. `lesson02_stg_setup`: 5 tasks green.
3. 2 file formats; 4 `STG_*` tables.
4. Hard-coding check: no output.
5. Exercise answers:

| Ex. | Expected answer |
|---|---|
| 2 | `ALTER TABLE ... ADD COLUMN` |
| 3 | STG text so a bad value can't reject a row; `NUMBER(18,3)` for weighed items; `LOCN_ID` is a label; `_INGEST_DT` is filtered by date → `DATE`; `_BATCH_ID` is a label |
| 4 | Net 2843788.51; 12 zero-net lines; Gold 204,298.00 + quarantined 2,639,490.51 |
| 5 | (a) outlier, passes; (b) defect, G6; (c) outlier, passes and counts in recon; (d) defect, G3; (e) G6b alert, still loads; (f) anomaly, G12 `QTY_TOO_HIGH`; (g) anomaly, G12 `PRICE_TOO_LOW` |
| 6 | Top line is A1. The ratio rule proves it: 1000 × the reference price. A fixed price limit would also reject the real 2,499.99 item. To allow bulk 1,500, change `max_sold_qty` in the Variable and re-import; **not** the SQL or the DAG |
| 7 | `STG_STORE_MASTER` → `TMP_LOCN_HIER` → `DIM_DIVISION`, `DIM_REGION`, `DIM_CITY`, `DIM_LOCN`; a column-count change is caught by G2 |

**Next:** Lesson 3 (stages, PUT/LIST/COPY, positional load). It is the first time data reaches Snowflake.

## 8. Plan for the remaining lessons

| # | Lesson | Key outputs |
|---|---|---|
| 3 | Stages | `setup/02_stages.sql`: `@my_int_stage` (PUT, LIST, REMOVE); `@my_ext_stage` + storage integration (explained, not run). COPY by name (`MATCH_BY_COLUMN_NAME`) vs **positional** `$1..$8` with `METADATA$FILENAME`, `METADATA$FILE_ROW_NUMBER` |
| 4 | Waiting (G1) | FileSensor mapped over `SOURCES` (`mode='reschedule'`, `poke_interval`, `timeout`); Assets and the trade-off; `LIST` assertion |
| 5 | Bronze DAG (G2) | `put_to_stage`, `load_stg.expand(source=SOURCES)`: `DELETE WHERE _INGEST_DT = ds` + `COPY ... FORCE = TRUE`; lineage columns; archive files |
| 6 | Generator v2 | `generate_complex.py`: 5 dates, returning customers, price and promo changes, changing stock, outliers kept |
| 7 | Silver TMP | Transient `TMP_SALES_LINE` (delta), `TMP_CUSTOMER` (members, `LAST_SEEN_DT`), `TMP_ITEM_LOC_SNAPSHOT` (`AVAILABLE_QTY`), `TMP_ITEM_HIER` and `TMP_LOCN_HIER` (overwrite); `TMP_QUARANTINE` (`GATE_ID`, `REASON_CD`, `RAW_RECORD = OBJECT_CONSTRUCT(*)`); gates G3–G6 and G6b. **G12 `gates/g12_outlier_limits.sql` reads `outlier_rules`, quarantines A1–A4, alerts.** Identity: STG = TMP + quarantined |
| 8 | Gold dimensions | `DIM_DEPARTMENT → CATEGORY → SUBCATEGORY → ITEM` ‖ `DIM_DIVISION → REGION → CITY → LOCN` (SCD2: `EFFECTIVE_START_DT/END_DT`, `CURRENT_FLG`, `INSERT_TS/UPDATE_TS`); `ITEM_DESC` SCD1; `DIM_CUSTOMER` SCD1 MERGE; `DIM_DAY` from the seed; sequence vs `HASH` keys; MERGE vs INSERT OVERWRITE vs `NOT EXISTS` |
| 9 | Gold facts + full DAG | `FCT_SALES` (MERGE on `TRANSACTION_ID, LINE_NUM`, `CLUSTER BY (DAY_KY)`), `FCT_INVENTORY` soft-close, `FCT_PRICE`; G7 (keys resolve), G8 (inventory ±20%); `medallion_daily` wait → bronze → silver → gold |
| 10 | Very complex data + defects | `generate_very_complex.py`: 2 null `ITEM_ID` (G3), 1 negative qty (G6), 1 orphan item (G7), 1 duplicate line (G4), 1 store changes `CITY_ID` (SCD2); PK checker `gates/pk_<table>.sql`; G10 |
| 11 | Aggregate + recon + alerting | `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES`; G9; G11 writes `AUDIT.RECON_DAILY_NET_SALES (DAY_KY, VARIANCE_AMT)` = STG net − quarantined net (incl. G12) − FCT net; `reconcile` returns bool → `@task.branch` → `recon_passed` or `send_alert` (Mailpit) → `fail_run`; `on_failure_callback` |
| 12 | Idempotency + reload of a bad day | Counts and sums before and after a second run must match; the diagram's reload policy for day D |
| 13 | Five-day backfill | 10, 11, 12, 13, 16 Nov, every task green |
| 14 | README + acceptance | Parse, hard-coding and DQ checks; 2 × 5 recon table; all 5 acceptance criteria |

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
> An Airflow DAG that loads **five consecutive days of source files** (backfilled in one run, 10–16 Nov, 5 days) through a **Bronze → Silver → Gold** medallion architecture into Snowflake, with full reconciliation. The DAG must be **idempotent** — safe to run twice for the same date with identical row counts and totals.
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
> 2. The **five‑day backfill (10–16 Nov)** completes with **every task green**
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

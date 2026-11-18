# Theory reference

Short, correct notes for every concept this project uses. Each lesson file links to the
sections it needs: read those sections first, then do the lesson.

Versions assumed: **Airflow 3.3** (2.x names noted where they differ) and **Snowflake** (any edition).
The project follows the architecture in [`architecture-Simple view.png`](architecture-Simple%20view.png)
(notes: [`architecture_simple.md`](architecture_simple.md), [`architecture_notes.md`](architecture_notes.md)).

## Which sections each lesson needs

| Lesson | Read first |
|---|---|
| 1 Environment, connections, variables | [1](#1-medallion-architecture-stg--tmp--tgt--aggregate), [2.1](#21-components), [2.3](#23-parse-time-vs-run-time), [2.6](#26-connections-variables-templating-and-the-fernet-key), [3.1](#31-architecture-warehouses-and-cost), [3.2](#32-access-control-service-users-and-key-pair-auth), [8](#8-docker-essentials) |
| 2 Simple data, STG tables, file formats | [1](#1-medallion-architecture-stg--tmp--tgt--aggregate), [3.4](#34-file-formats-and-copy-into), [3.7](#37-null-handling), [3.10](#310-data-types-and-why-we-pick-them), [5.6](#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |
| 3 Stages, positional load | [3.3](#33-stages), [3.4](#34-file-formats-and-copy-into) |
| 4 Waiting: sensors vs Assets, gate G1 | [2.7](#27-sensors), [2.8](#28-assets-datasets-and-the-sensor-trade-off), [3.3](#33-stages), [5.2](#52-quality-gates-red-hexagons-on-the-diagram) |
| 5 Bronze DAG: mapped STG load | [2.4](#24-scheduling-logical-date-catchup-and-backfill), [2.5](#25-taskflow-api-and-xcom), [2.9](#29-dynamic-task-mapping-and-task-groups), [3.4](#34-file-formats-and-copy-into), [3.5](#35-micro-partitions-pruning-and-the-diagrams-partitioning), [6](#6-idempotency-playbook) |
| 6 Complex data, hierarchies, customers, prices | [4.1](#41-grain-facts-and-dimensions), [4.3](#43-hierarchies) |
| 7 Silver: TMP tables, gates G2–G6, G12 outliers | [3.6](#36-sql-toolkit), [3.7](#37-null-handling), [3.10](#310-data-types-and-why-we-pick-them), [5.1](#51-quarantine-never-drop), [5.2](#52-quality-gates-red-hexagons-on-the-diagram), [5.6](#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |
| 8 Gold TGT dimensions | [4.2](#42-natural-keys-vs-surrogate-keys), [4.3](#43-hierarchies), [4.4](#44-slowly-changing-dimensions-scd), [3.6](#36-sql-toolkit), [2.9](#29-dynamic-task-mapping-and-task-groups), [6](#6-idempotency-playbook) |
| 9 Gold TGT facts, full DAG | [4.1](#41-grain-facts-and-dimensions), [4.5](#45-fact-types-sales-inventory-soft-close-price-periods), [3.6](#36-sql-toolkit), [5.2](#52-quality-gates-red-hexagons-on-the-diagram) |
| 10 Defects, SCD2, PK checker | [4.4](#44-slowly-changing-dimensions-scd), [4.6](#46-orphans-and-late-arriving-dimensions), [3.8](#38-constraints-are-not-enforced), [5](#5-data-quality-quarantine-and-reconciliation) |
| 11 Aggregate, reconciliation, alerting | [4.7](#47-aggregates-and-views), [5.4](#54-reconciliation-g11), [2.10](#210-branching-trigger-rules-and-the-return-flag-pattern), [2.11](#211-failure-handling-retries-callbacks-and-alerts) |
| 12 Idempotency proof | [6](#6-idempotency-playbook), [5.5](#55-retention-and-the-reload-of-one-bad-day-d) |
| 13 Backfill | [2.4](#24-scheduling-logical-date-catchup-and-backfill) |
| 14 Checks, README | [7](#7-checks) |

---

## 1. Medallion architecture (STG / TMP / TGT / Aggregate)

![Simple view of the architecture](architecture-Simple%20view.png)

Data flows left to right: **Source files → Bronze (STG) → Silver (TMP) → Gold TGT → Gold Aggregate → Consumers.**
Each layer makes a promise the next layer can rely on. Red hexagons on the diagram are
**quality gates** (G1–G10) on the layer boundaries (section [5.2](#52-quality-gates-red-hexagons-on-the-diagram)).

### 1.1 Naming convention

| Table type | Layer | Snowflake schema | Purpose | Tables in this project |
|---|---|---|---|---|
| **STG** | Bronze | `BRONZE` | Raw landing, no transformation | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | Silver | `SILVER` | Clean and compare against target | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER`, `TMP_QUARANTINE` |
| **TGT** | Gold | `GOLD` | The dimension or fact itself | 10 `DIM_`* + 3 `FCT_`* (below) |
| **Aggregate** | Gold | `GOLD` | Rolled-up marts on top of TGT | `AGG_DAILY_ITEM_LOCN_SALES`, view `VW_DAILY_ITEM_LOCN_SALES` |

Gold TGT, grouped as on the diagram:

- **Product hierarchy** (SCD2): `DIM_DEPARTMENT → DIM_CATEGORY → DIM_SUBCATEGORY → DIM_ITEM`
- **Location hierarchy** (SCD2): `DIM_DIVISION → DIM_REGION → DIM_CITY → DIM_LOCN`
- **Customer** (SCD1): `DIM_CUSTOMER`
- **Calendar** (seed): `DIM_DAY`
- **Facts**: `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE`

Other naming rules used everywhere:

| Thing | Rule | Example |
|---|---|---|
| Source file | `<source>_YYYYMMDD.csv`; files are not tables, so they keep their names | `sales_20261110.csv` |
| Surrogate key | `<ENTITY>_KY`, `NUMBER(38,0)` | `ITEM_KY`, `LOCN_KY` |
| Business (natural) key | `<ENTITY>_ID`, `VARCHAR` | `ITEM_ID`, `LOCN_ID` |
| Day key | `DAY_KY = YYYYMMDD`, `NUMBER(8,0)` | `20261110` |
| Suffixes | `_NM` name, `_DESC` description, `_DT` date, `_TS` timestamp, `_AMT` money, `_QTY` quantity, `_NUM` number, `_CD` code, `_TXT` free text, `_FLG` flag, `_CNT` count | `NET_SALES_AMT`, `ACTIVE_FLG` |
| Lineage columns | Leading underscore, so they never clash with source columns | `_INGEST_DT`, `_SOURCE_FILE` |
| SQL files | Lowercase table name | `include/sql/ddl/bronze/stg_sales.sql` |
| Airflow task ids | Lowercase | `create_stg_sales`, `load_dim_item` |

Every object on the diagram carries a badge **format · partitioning · load pattern**, for
example `CSV · ingest_dt · delta` (STG_SALES) or `table · DAY_KY · delta` (FCT_SALES).

### 1.2 What each layer guarantees

| Layer | Guarantees | Does **not** guarantee |
|---|---|---|
| **STG** (Bronze) | Byte-for-byte copy of the file; replayable by `_INGEST_DT`; arrival recorded even if a later gate fails | Types, non-null keys, uniqueness, deduplication |
| **TMP** (Silver) | Typed, trimmed, deduplicated on the business key; compared to TGT (new / changed / unchanged); bad rows go to `TMP_QUARANTINE` with a gate id and reason, never dropped | Surrogate keys, SCD2 history, finance-ready totals |
| **TGT** (Gold) | Star-schema grain; SCD1/SCD2 rules; every fact FK resolves (G7); facts idempotent on their natural keys | Real-time freshness (data is as of D+1); raw evidence |
| **Aggregate** (Gold) | Totals reconcile with `FCT_SALES` per `DAY_KY` (G9); rebuildable from TGT | Line-level detail |
| **Consumers** | Read Aggregate for reports, TGT for detail | Never read STG or TMP |

Why layers: a bug in Silver or Gold can be fixed and re-run from Bronze without asking
the source system to resend files.

### 1.3 The source drops and what they feed

| Drop | Grain | Load | Header | Lands in | Feeds (TMP → TGT) |
|---|---|---|---|---|---|
| `sales_YYYYMMDD.csv` | One POS line, with member attributes when present | delta | yes | `STG_SALES` | `TMP_SALES_LINE` → `FCT_SALES`; `TMP_CUSTOMER` → `DIM_CUSTOMER` |
| `inventory_YYYYMMDD.csv` | One item at one location for the day, with stock and prices | full | **no** (positional) | `STG_INVENTORY` | `TMP_ITEM_LOC_SNAPSHOT` → `FCT_INVENTORY`, `FCT_PRICE` |
| `item_master_YYYYMMDD.csv` | Flattened item > subcategory > category > department | full | yes | `STG_ITEM_MASTER` | `TMP_ITEM_HIER` → product hierarchy dims |
| `store_master_YYYYMMDD.csv` | Flattened location > city > region > division | full | yes | `STG_STORE_MASTER` | `TMP_LOCN_HIER` → location hierarchy dims |
| `seed_dim_day.csv` | One calendar day | seed | yes | (none) | `DIM_DAY` directly (the departure, 1.4) |

### 1.4 Departures, and where the brief and the diagram differ

| # | Departure | Why |
|---|---|---|
| 1 | `DIM_DAY` is seeded straight into Gold (green dashed line), skipping STG and TMP | The calendar is generated (`DAY_KY = YYYYMMDD`). No source system and nothing raw to keep, so extra copies add no audit value. Generator and seed file are version controlled |
| 2 | `DIM_ITEM.ITEM_DESC` is **SCD1** (overwritten); the other item attributes are SCD2 | The brief requires an SCD1 in-place update on a DIM_ITEM attribute; a description typo fix is the classic case |
| 3 | `FCT_INVENTORY` also carries `EFFECTIVE_END_DT` and `CURRENT_FLG` and is **soft-closed, never deleted** | The brief requires "closed, not deleted". A new row is written only when a position changes |
| 4 | Silver is Snowflake **transient tables**, not Parquet files | Everything runs inside Snowflake; Snowflake already stores tables in a compressed columnar format. "Partitioning" on the diagram becomes the column we filter and reload by |
| 5 | A reload of a corrected file **replaces** day D in STG; the bad file is kept in `data/archive/` | Keeps "run twice = same counts" simple; the archive is the evidence of what was first sent |
| 6 | Extra gate **G11**: Gold net sales tie to the source file per day | The brief's reconciliation requirement (2 columns × 5 rows) |
| 7 | Brief's "store whose district changes" = a store moving to a different `CITY_ID` (its direct parent) | Brief hierarchy store → district → region maps onto locn → city → region → division |
| 8 | Extra gate **G12**: configurable outlier limits | G6 only checks arithmetic; a price typed ×1000 still adds up. Limits live in the Variable so they change without code |
| 9 | **No external stage**: only the internal stage `@my_int_stage` | External stages need your own S3, Azure Blob or GCS bucket, and none is available. `LIST` and `COPY` work the same on both kinds |

---

## 2. Airflow fundamentals

### 2.1 Components

| Component | Role |
|---|---|
| API server | UI and REST API |
| DAG processor | Parses DAG files (separate process in Airflow 3) |
| Scheduler | Creates DAG runs, decides which tasks are ready |
| Executor + workers | Run the tasks (CeleryExecutor here: tasks go through Redis to workers) |
| Triggerer | Runs deferred tasks (async waits that don't hold a worker slot) |
| Metadata DB | Postgres: runs, task states, XComs, connections, variables |

### 2.2 DAGs, tasks, operators, hooks, sensors

- **DAG**: a directed acyclic graph of tasks. "Acyclic" means no loops; Airflow refuses cycles at parse time.
- **Task**: one node; an instance of an operator. A **task instance** is one task for one run (and one map index).
- **Operator**: a template for a unit of work (`SQLExecuteQueryOperator`, `EmailOperator`).
- **Hook**: a client for an external system, built from a connection (`SnowflakeHook`). Operators use hooks internally; inside `@task` you use hooks directly.
- **Sensor**: an operator that waits until a condition is true (`FileSensor`).

### 2.3 Parse time vs run time

- **Parse time**: the DAG processor imports every file in `dags/` about every 30 s. Top-level code runs each time, so it must be fast and must not call databases, APIs or `Variable.get()`.
- **Run time**: a worker executes one task. Task bodies run and templates render here.
- Rule: config is read inside tasks or through Jinja templates, never at module top level.

### 2.4 Scheduling, logical date, catchup and backfill

- **Logical date** (`{{ ds }}` = `YYYY-MM-DD`, `{{ ds_nodash }}` = `YYYYMMDD`): the date a run *represents*. Each run processes only its own date; that is what makes reruns and backfills safe. In this project `ds` is day **D**: it becomes `_INGEST_DT` in STG and `DAY_KY` in Gold.
- Airflow 3 cron schedules use `CronTriggerTimetable` by default: the run for `2026-11-10 06:00` starts at that time and its `ds` is `2026-11-10`. (Airflow 2 used data intervals: the run for day D started at the *end* of D.)
- `0 6 * * 1-5` = 06:00 Monday–Friday, so 10–16 Nov 2026 gives runs for 10, 11, 12, 13 and 16.
- **catchup** (default `False` in Airflow 3): whether the scheduler creates runs for past dates automatically.
- **Backfill** (Airflow 3): `airflow backfill create --dag-id X --from-date 2026-11-10 --to-date 2026-11-16`. Runs are executed by the scheduler and show in the UI. Options include `--reprocess-behavior` (`none`, `failed`, `completed`) and `--max-active-runs`.
- `max_active_runs=1`: one run at a time. Required when a run depends on the previous date's result (SCD2, inventory soft-close, price periods, gate G8).

### 2.5 TaskFlow API and XCom

```python
from airflow.sdk import dag, task

@task
def count_rows(table: str) -> int:
    return 42            # return value is pushed to XCom automatically

@task
def report(n: int):
    print(n)             # argument is pulled from XCom automatically

report(count_rows("x"))  # calling tasks wires the dependency
```

- **XCom** stores small values in the metadata DB. Pass counts, flags, file names; never datasets.
- Mix with classic operators: `op_task >> taskflow_task()`.

### 2.6 Connections, variables, templating and the Fernet key

| Item | Use for | Read with |
|---|---|---|
| Connection | Host, account, user, credentials | Hook (`SnowflakeHook(snowflake_conn_id=...)`), `{{ conn.<id>.login }}`, `{{ conn.<id>.extra_dejson.<key> }}` |
| Variable | Non-secret config | `{{ var.value.<key> }}` (string), `{{ var.json.<key>.<sub> }}` (JSON), `Variable.get(key, deserialize_json=True)` inside a task |

- **Templated fields**: only fields listed in an operator's `template_fields` are rendered (for SQL operators: `sql`). Files ending in `.sql` are read and rendered, searched in `template_searchpath`.
- Jinja renders the **whole** file, SQL comments included. `{{ var.json.* }}` or an empty `{{ }}` in a `--` comment raises `TemplateSyntaxError`; keep braces out of comments.
- Useful macros: `{{ ds }}`, `{{ ds_nodash }}`, `{{ logical_date }}`, `{{ macros.ds_add(ds, -1) }}`, `{{ run_id }}`.
- **Fernet key** encrypts connection passwords and extras in the metadata DB. Set it before creating connections; changing it later makes them unreadable.

### 2.7 Sensors

| Mode | Behaviour | Use when |
|---|---|---|
| `poke` (default) | Holds a worker slot and sleeps between checks | Waits of seconds |
| `reschedule` | Frees the slot between checks; task shows `up_for_reschedule` | Waits of minutes or hours (our case) |
| Deferrable (`deferrable=True`, where supported) | Hands the wait to the triggerer (async) | Many long waits at scale |

Key parameters:

- `poke_interval`: seconds between checks (use ≥ 60 with `reschedule`).
- `timeout`: total seconds before giving up. The sensor then fails (`AirflowSensorTimeout`), or is skipped if `soft_fail=True`. This is the "file arrives by SLA" half of gate **G1**.
- `exponential_backoff=True`: grow the interval between checks.
- `FileSensor(fs_conn_id=..., filepath=...)`: the path is relative to the `path` in the `fs` connection, so the DAG never contains a full path.

### 2.8 Assets (Datasets) and the sensor trade-off

- An **Asset** is a named piece of data (`Asset("s3://bucket/sales")`). A task that lists it in `outlets=[...]` emits an *asset event* when it succeeds. A DAG with `schedule=[asset_a, asset_b]` runs when all of them have been updated.
- Airflow 3 adds `AssetWatcher` to create asset events from external triggers (for example, message queues).

| | FileSensor | Assets |
|---|---|---|
| How it waits | Polls the filesystem | Event-driven, no polling |
| Knows about files produced outside Airflow | Yes | Only if something emits an event (a producer DAG or a watcher) |
| "File for date D exists" | Natural (`filepath` templated with `ds_nodash`) | Not directly; events are not tied to a logical date |
| Timeout / SLA on arrival | Built in (`timeout`) | Must be built separately |
| Cost | A worker slot per check (in `reschedule` mode, only during the check) | Nearly free |

**Choice for this project:** `FileSensor`, because files arrive from outside Airflow and each run
needs the files for *its* date, with a timeout. Assets are taught as the right tool when
Airflow itself produces the data.

### 2.9 Dynamic Task Mapping and task groups

```python
load_stg.partial(ingest_dt="{{ ds }}").expand(source=["sales", "inventory", "item_master", "store_master"])
```

- `.expand()` creates one task instance per element at run time (map indexes 0..n-1). `.partial()` passes constant arguments. `.expand_kwargs()` maps over a list of dicts.
- Mapped instances run in parallel, limited by `parallelism`, pools and `max_active_tis_per_dag`.
- Mapping over a list returned by an upstream task makes the fan-out data-driven.
- **Task groups** (`@task_group` or `TaskGroup`) group tasks visually and let you set dependencies on the whole group (`tgt_dims >> tgt_facts`). They don't change execution.

### 2.10 Branching, trigger rules and the return-flag pattern

```python
@task
def reconcile() -> bool:
    return variance_rows == 0          # the "return flag"

@task.branch
def route(ok: bool) -> str:
    return "recon_passed" if ok else "send_alert"
```

- A branch task returns the task id(s) to follow; the others are **skipped**.
- Skips propagate downstream. A task that joins branches needs `trigger_rule="none_failed_min_one_success"`.
- Other trigger rules: `all_success` (default), `all_done` (runs even after failures, good for cleanup and summaries), `one_failed`.

### 2.11 Failure handling: retries, callbacks and alerts

- `retries` and `retry_delay` in `default_args`: transient errors (network, warehouse resume) get retried.
- `raise AirflowFailException(...)`: fail immediately, **no retries**. Use for data problems such as a gate with action "fail", where retrying won't help.
- `on_failure_callback=fn`: called with the task context when a task fails. Use it to send email or chat alerts.
- `EmailOperator` (from `airflow.providers.smtp.operators.smtp`) sends mail through an SMTP connection (`smtp_default`). Locally, Mailpit receives it. Gates with action "alert" (G1, G6b, G7) use it.

---

## 3. Snowflake fundamentals

### 3.1 Architecture, warehouses and cost

- Storage and compute are separate. Data lives once in cloud storage; **virtual warehouses** are compute clusters that read it.
- Credits are charged per second while a warehouse runs (60 s minimum each time it resumes). `AUTO_SUSPEND = 60` and `AUTO_RESUME = TRUE` keep costs low.
- X-Small is enough for this project.

### 3.2 Access control, service users and key-pair auth

- Privileges are granted to **roles**, and roles to users (RBAC).
- System roles: `ACCOUNTADMIN` (everything; use sparingly), `SECURITYADMIN` (users, roles, grants), `SYSADMIN` (warehouses, databases).
- Best practice: a custom role per workload (`MEDALLION_ROLE`) granted up to `SYSADMIN`.
- `TYPE = SERVICE` users cannot use passwords or Snowsight and aren't subject to MFA. They authenticate with **key pairs**: Snowflake stores the public key, and the client signs a token with the private key.

### 3.3 Stages

A stage is a named location for files that Snowflake can read with `COPY INTO`.

| Stage | Reference | Notes |
|---|---|---|
| User stage | `@~` | Per user, cannot be shared |
| Table stage | `@%table_name` | Per table |
| Named internal | `@my_int_stage` | Storage managed by Snowflake; files uploaded with `PUT` |
| Named external | — | Points at your own cloud bucket. **Not used** here (departure 9) |

Commands:

```sql
PUT 'file:///opt/airflow/data/landing/sales_20261110.csv' @my_int_stage/2026-11-10/ AUTO_COMPRESS = TRUE;
LIST @my_int_stage/2026-11-10/;                       -- name, size, md5, last_modified
SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())); -- query LIST output with SQL
REMOVE @my_int_stage/2026-11-10/;
```

- `PUT` runs from a client (Python connector, SnowSQL), **not** from a Snowsight worksheet. Snowsight has an upload button instead.
- `PUT` / `GET` are intercepted **by the client**. The statement must start with `PUT` or `GET` (no leading `--` comments), or the connector sends it to the server and fails with `unsupported_requested_format:snowflake`.
- `PUT` skips a file that is already on the stage unchanged (status `SKIPPED`). `OVERWRITE = TRUE` replaces a changed file with the same name.
- `LIST` is how the pipeline proves files reached the stage before `COPY` (Lesson 4).

### 3.4 File formats and COPY INTO

**File format** (reusable parsing rules): `TYPE = CSV`, `FIELD_DELIMITER`, `SKIP_HEADER = 1` or `PARSE_HEADER = TRUE`,
`FIELD_OPTIONALLY_ENCLOSED_BY = '"'`, `NULL_IF = ('', 'NULL')`, `EMPTY_FIELD_AS_NULL = TRUE`, `TRIM_SPACE`.

- `CREATE OR REPLACE FILE FORMAT` is safe (no data inside). For tables, `CREATE OR REPLACE` wipes rows, so it is only used while a table is still empty; after that, columns change with `ALTER TABLE ... ADD COLUMN`.
- STG columns are `VARCHAR` so a malformed value lands as text instead of rejecting the row; Silver casts with `TRY_TO_*`.

**Two ways to map columns:**

```sql
-- By name: the header names decide the mapping. CSV needs PARSE_HEADER = TRUE (not SKIP_HEADER)
COPY INTO DEMO_TABLE FROM @my_int_stage/2026-11-10/ FILES = ('item_master_20261110.csv.gz')
  FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Positional: column order in the file decides the mapping ($1 = first field)
COPY INTO BRONZE.STG_INVENTORY
  (SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY,
   UNIT_PRICE_AMT, PROMO_PRICE_AMT,
   _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8,
             '2026-11-10', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, 'manual__2026-11-10'
      FROM @my_int_stage/2026-11-10/)
FILE_FORMAT = (FORMAT_NAME = FF_CSV_POSITIONAL)
PATTERN = '.*inventory_20261110.*';
```

Positional loads suit files with no header and a fixed, contracted column order. If the order
changes, the data silently goes into the wrong columns, so pair it with validation (gate G2).
`_INGEST_TS` is filled by the column default (`CURRENT_TIMESTAMP()`).

**Why this project loads every file with `SELECT $1, $2, ...`** (a "transforming" COPY):
- By-name loading can only add *file* metadata (`INCLUDE_METADATA`), not constants, so it can't
  fill the required `_INGEST_DT` and `_BATCH_ID` columns.
- The `SELECT` lets us add all lineage columns in the same statement.
- Trade-off: with a `SELECT`, Snowflake **ignores `ERROR_ON_COLUMN_COUNT_MISMATCH`** (a missing
  column just becomes `NULL`). So gate G2 needs its own explicit check (Lesson 5).
- `VALIDATION_MODE` and `VALIDATE()` don't work with a `SELECT` either; query the staged file
  directly instead: `SELECT $1, $2 FROM @stage/file (FILE_FORMAT => 'FF_NAME')`.

**Load history and re-runs:**

- Snowflake remembers which files were loaded into a table for **64 days** and skips them on later `COPY` runs (the result says `0 files processed`).
- `FORCE = TRUE` reloads files anyway, which **creates duplicates** unless the target rows are deleted first.
- `ON_ERROR`: `ABORT_STATEMENT` (default), `CONTINUE` (skip bad rows), `SKIP_FILE`.
- `VALIDATION_MODE = RETURN_ERRORS` checks files without loading (not allowed with a transforming `SELECT`).
- `METADATA$FILENAME` and `METADATA$FILE_ROW_NUMBER` give lineage per row.

### 3.5 Micro-partitions, pruning and the diagram's "partitioning"

- Snowflake stores tables in **micro-partitions** (about 50–500 MB uncompressed) and records min/max per column for each one.
- A filter like `WHERE _INGEST_DT = '2026-11-10'` skips partitions whose range can't match. This is **pruning**.
- There is no Hive-style `PARTITION BY` in Snowflake. The diagram's partition column becomes **the column every load and reload filters on**; loading one day at a time keeps data naturally clustered by it. On big Gold facts a **clustering key** keeps it that way.

| Layer | Diagram partition | Our filter / clustering column |
|---|---|---|
| STG | `ingest_dt` | `_INGEST_DT` |
| TMP | `sale_dt`, `snapshot_dt`, `ingest_dt` or none | `SALE_DT`, `SNAPSHOT_DT`, `_INGEST_DT` (masters are full refresh: none) |
| TGT | `DAY_KY` (FCT_SALES), `SNAPSHOT_DT` (FCT_INVENTORY), else none | `CLUSTER BY (DAY_KY)`, `CLUSTER BY (SNAPSHOT_DT)` |
| Aggregate | `DAY_KY` | `CLUSTER BY (DAY_KY)` |

`FCT_PRICE` and all dimensions are not partitioned by date, because a price period or an SCD2
version spans many days.

### 3.6 SQL toolkit

| Tool | What it does | Use for |
|---|---|---|
| `MERGE INTO t USING s ON key WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` | Upsert in one atomic statement | SCD1 (`DIM_CUSTOMER`), `FCT_SALES` on `(TRANSACTION_ID, LINE_NUM)` |
| `INSERT OVERWRITE INTO t SELECT ...` | Replaces **all** rows of the table atomically | Full-refresh TMP masters (`TMP_ITEM_HIER`, `TMP_LOCN_HIER`) |
| `INSERT INTO t SELECT ... WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.key = s.key)` | Insert only new keys (anti-join) | New dimension members, new SCD2 versions, new fact rows |
| `DELETE WHERE _INGEST_DT = D` + `INSERT` | Replace one day | Daily slices in STG and TMP |
| `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY ...) = 1` | Keep one row per key | Dedupe in TMP (gate G4: keep latest) |
| `TRY_TO_NUMBER`, `TRY_TO_DATE`, `TRY_TO_BOOLEAN` | Return NULL instead of failing | Safe casting from STG text |
| `CREATE SEQUENCE s; s.NEXTVAL` | Unique increasing numbers (gaps allowed) | Surrogate keys `*_KY` |
| `MD5(...)`, `HASH(...)`, `SHA2(...)` | Deterministic value from input | Deterministic keys, change detection |

**MERGE gotcha:** if two source rows match the same target row, Snowflake raises
"Duplicate row detected during DML action". Always dedupe the source first.

### 3.7 NULL handling

- Any comparison with NULL is unknown: `NULL = NULL` is not true. Use `IS NULL`, `IS DISTINCT FROM`, or `EQUAL_NULL(a, b)`.
- `NOT IN (subquery)` returns **no rows** if the subquery contains a NULL. Use `NOT EXISTS`, which also explains why the brief asks for it.
- Aggregates ignore NULLs: `SUM` of all-NULL is NULL, so wrap it with `COALESCE(SUM(x), 0)` in reconciliation.
- `NULLIF(TRIM(col), '')` turns empty strings into NULL; `COALESCE(col, default)` fills NULLs.
- Write the rule down per column:
  - may be NULL: `CUSTOMER_ID` (non-member sale), `PROMO_PRICE_AMT` (no promo);
  - gets a default: `DISCOUNT_AMT → 0`;
  - sends the row to quarantine (gate G3): `ITEM_ID`, `LOCN_ID`, `TRANSACTION_ID`, `LINE_NUM`.
- `HASH`/`MD5` over concatenated columns: wrap each one in `COALESCE(col, '~')`, otherwise one NULL makes the whole concatenation NULL.

### 3.8 Constraints are not enforced

On standard Snowflake tables, `PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` are **metadata only**;
only `NOT NULL` is enforced. Nothing stops duplicate keys, which is why the pipeline needs
a **PK checker** task:

```sql
SELECT ITEM_ID, COUNT(*) FROM GOLD.DIM_ITEM WHERE CURRENT_FLG GROUP BY ITEM_ID HAVING COUNT(*) > 1;
-- any row returned = fail the task
```

### 3.9 Transactions and Time Travel

- Each statement autocommits unless wrapped in `BEGIN ... COMMIT`. DDL commits implicitly.
- Use a transaction when a load is two steps (close old rows, insert new), so a failure doesn't leave half the change.
- **Time Travel** lets you query or restore earlier versions: `SELECT ... AT(OFFSET => -3600)`, `UNDROP TABLE`. The diagram's policy: 1 day for Bronze/Silver, 7 days for Gold (`DATA_RETENTION_TIME_IN_DAYS`; above 1 day needs Enterprise).

### 3.10 Data types and why we pick them

Rule of thumb: **STG stores text, TMP picks the real type, TGT keeps TMP's types.**
The types below are the ones on the diagram's *Medallion Architecture* page.

| Type                  | Use it for                                                                                                                                         | Why, and what to avoid                                                                                                                                                                                                                                     |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `VARCHAR` (no length) | Every STG business column                                                                                                                          | Text never fails to load: a bad value like `3.9O` still lands and is caught in TMP. A length in STG would only add a way to reject a row                                                                                                                   |
| `VARCHAR(n)`          | TMP and TGT text: ids `VARCHAR(50)`, names `VARCHAR(100–200)`, descriptions `VARCHAR(500–1000)`, `EMAIL_TXT VARCHAR(254)`, `PHONE_NUM VARCHAR(30)` | Lengths document the contract (254 is the email maximum). Snowflake stores `VARCHAR(50)` and `VARCHAR` the same way, so it costs nothing. Ids stay text even when they look numeric (`1001`, `100006`): you never add them, and leading zeros must survive |
| `NUMBER(18,2)`        | Money: `UNIT_PRICE_AMT`, `PROMO_PRICE_AMT`, `DISCOUNT_AMT`, `SALES_AMT`, `TAX_AMT`, `NET_SALES_AMT`                                                | Exact decimal with 2 places, room for large totals. **Never** `FLOAT` **for money**: `FLOAT` is approximate (0.1 + 0.2 = 0.30000000000000004) and reconciliation needs a variance of exactly 0                                                             |
| `NUMBER(18,3)`        | Quantities: `SOLD_QTY`, `ON_HAND_QTY`, `RESERVED_QTY`, `AVAILABLE_QTY`, `IN_TRANSIT_QTY`                                                           | Fresh items are sold by weight (`0.750` kg), so quantities need 3 decimals                                                                                                                                                                                 |
| `NUMBER(38,0)`        | Surrogate keys `*_KY`, counts `*_CNT`, `_SOURCE_ROW_NUM`                                                                                           | Whole numbers; the largest integer Snowflake has, so a sequence never runs out                                                                                                                                                                             |
| `NUMBER(10,0)`        | `LINE_NUM`                                                                                                                                         | A small whole number                                                                                                                                                                                                                                       |
| `NUMBER(8,0)`         | `DAY_KY` (`20261110`)                                                                                                                              | A readable date key that sorts and joins like a number                                                                                                                                                                                                     |
| `DATE`                | `SALE_DT`, `SNAPSHOT_DT`, `CALENDAR_DT`, `EFFECTIVE_START_DT` / `EFFECTIVE_END_DT`, `_INGEST_DT`                                                   | Calendar day with no time part. Allows date filters and date maths                                                                                                                                                                                         |
| `TIMESTAMP_NTZ`       | `_INGEST_TS`, `INSERT_TS`, `UPDATE_TS`, `QUARANTINED_TS`                                                                                           | Date + time, no time zone. Simplest to read when everything runs in one place                                                                                                                                                                              |
| `BOOLEAN`             | `ACTIVE_FLG`, `CURRENT_FLG`, `WEEKEND_FLG`, `HOLIDAY_FLG`                                                                                          | Only TRUE / FALSE / NULL, so no guessing what `'Y'`, `'1'` or `'yes'` mean. The files send `Y`/`N`; TMP converts with `TRY_TO_BOOLEAN`                                                                                                                     |
| `VARIANT`             | `TMP_QUARANTINE.RAW_RECORD`                                                                                                                        | Holds the whole rejected row as JSON (`OBJECT_CONSTRUCT(*)`), whatever table it came from                                                                                                                                                                  |

**Lineage columns on every STG table:**

| Column | Type | Why |
|---|---|---|
| `_INGEST_DT` | `DATE` | The business day D the file belongs to (`{{ ds }}`). The diagram's partition: delete or reload one day with `WHERE _INGEST_DT = ...` |
| `_SOURCE_FILE` | `VARCHAR` | Which file the row came from |
| `_SOURCE_ROW_NUM` | `NUMBER` | Line number in that file, so a quarantined row can be found in the original file |
| `_INGEST_TS` | `TIMESTAMP_NTZ` | When the row was loaded (default `CURRENT_TIMESTAMP()`) |
| `_BATCH_ID` | `VARCHAR` | The Airflow `run_id` that loaded it, so you can tell a first load from a rerun |

TMP tables keep `_INGEST_DT` (and `_SOURCE_FILE` on `TMP_SALES_LINE`). Gold tables carry audit
columns `INSERT_TS` and `UPDATE_TS` instead.

Converting STG text to a type in TMP: `TRY_TO_NUMBER(x, 18, 2)`, `TRY_TO_DATE(x)`,
`TRY_TO_BOOLEAN(x)`. The `TRY_` versions return NULL instead of failing, and a NULL where the
input was not NULL means "bad value": that row goes to `TMP_QUARANTINE`.

---

## 4. Dimensional modelling

### 4.1 Grain, facts and dimensions

- **Grain**: what one fact row means. Declare it before writing SQL.

| Fact | Grain | Natural key (idempotency) |
|---|---|---|
| `FCT_SALES` | One POS transaction line | `(TRANSACTION_ID, LINE_NUM)` |
| `FCT_INVENTORY` | One item at one location, from the snapshot day it changed | `(ITEM_KY, LOCN_KY, SNAPSHOT_DT)` |
| `FCT_PRICE` | One price period for an item at a location | `(ITEM_KY, LOCN_KY, EFFECTIVE_START_DT)` |
| `AGG_DAILY_ITEM_LOCN_SALES` | One item at one location per day | `(DAY_KY, ITEM_KY, LOCN_KY)` |

- **Facts**: numeric measures (`SOLD_QTY`, `NET_SALES_AMT`) plus foreign keys (`*_KY`) to dimensions.
- **Dimensions**: descriptive context (item, location, customer, day) used to filter and group.
- **Degenerate dimension**: an id kept on the fact with no dimension table (`TRANSACTION_ID`).
- **Rollup**: aggregate facts to a coarser grain by grouping on dimension attributes (net sales by region by day). Totals must equal the detail (gate G9).

### 4.2 Natural keys vs surrogate keys

- **Business (natural) key** `*_ID`: the source id (`ITEM_ID`, `LOCN_ID`). Used for matching and the PK checker.
- **Surrogate key** `*_KY`: a warehouse key (`ITEM_KY`) that facts join on. It lets one business key have several SCD2 versions, and isolates Gold from source id changes.

| | Sequence key | Deterministic (hash) key |
|---|---|---|
| How | `seq.NEXTVAL` on insert | `HASH(ITEM_ID, EFFECTIVE_START_DT)` (64-bit number) |
| Re-run of an existing row | Fine if inserts use `NOT EXISTS` (no new number taken) | Same key every time |
| Full rebuild | **New numbers**, so existing facts point at the wrong rows unless they're rebuilt too | Same keys, so facts remain valid |
| Fits `NUMBER(38,0)` | Yes | `HASH` yes (tiny collision risk); `MD5` is a 32-char string, so no |

### 4.3 Hierarchies

- Product: `DIM_DEPARTMENT → DIM_CATEGORY → DIM_SUBCATEGORY → DIM_ITEM`.
- Location: `DIM_DIVISION → DIM_REGION → DIM_CITY → DIM_LOCN`.
- **Snowflaked** design (ours, as on the diagram): each level has its own table, and the child holds the parent's surrogate key (`DIM_ITEM.SUBCATEGORY_KY → DIM_SUBCATEGORY`). **Flattened** design: parent attributes copied onto the child (that is what the master *files* and `TMP_*_HIER` look like).
- "Hierarchy maintained correctly" means:
  - load parents before children (department, then category, then subcategory, then item);
  - look up the parent's **current** key when inserting the child;
  - every child's parent key exists (no orphan children).

### 4.4 Slowly changing dimensions (SCD)

| Type | On change | Here |
|---|---|---|
| SCD0 | Never change | `DIM_DAY` (seed) |
| **SCD1** | Overwrite in place; history lost | `DIM_CUSTOMER` (all attributes), `DIM_ITEM.ITEM_DESC` (departure 2) |
| **SCD2** | Close the current row, insert a new version | All 8 hierarchy dims; the planted defect is a store moving to another city → new `DIM_LOCN` version |

SCD2 control columns (diagram names): `EFFECTIVE_START_DT`, `EFFECTIVE_END_DT` (open row =
`9999-12-31`), `CURRENT_FLG`. A hash of the tracked attributes detects change.

Daily SCD2 steps for day D:

1. Compute each TMP row's hash of tracked attributes (including the parent's business key).
2. **Close**: current rows whose hash differs → `EFFECTIVE_END_DT = D - 1`, `CURRENT_FLG = FALSE`, `UPDATE_TS = now`.
3. **Insert**: new business keys plus changed ones → `EFFECTIVE_START_DT = D`, `EFFECTIVE_END_DT = '9999-12-31'`, `CURRENT_FLG = TRUE`, using `NOT EXISTS` on (business key, `CURRENT_FLG`).

Re-run safety:

- On a second run for the same D, the current row already has the same hash, so nothing is closed and nothing is inserted.
- To *correct* day D (reload policy step 6): undo versions opened or closed on D, then apply again.
- Process dates in order (`max_active_runs=1`). Running D+1 before D breaks the version chain.
- Facts join to the version valid on the business date: `f.SALE_DT BETWEEN d.EFFECTIVE_START_DT AND d.EFFECTIVE_END_DT`.

### 4.5 Fact types: sales, inventory soft-close, price periods

| Type | Grain | Example |
|---|---|---|
| Transaction | One event | `FCT_SALES` |
| Periodic snapshot | One row per entity per period | Daily stock per item × location |
| Effective-dated (period) | One row per period during which values held | `FCT_PRICE`, and our `FCT_INVENTORY` |

`FCT_INVENTORY` **(soft-close, departure 3).** Each day D:

- a position that **changed** gets its open row closed (`EFFECTIVE_END_DT = D - 1`, `CURRENT_FLG = FALSE`) and a new row inserted with `SNAPSHOT_DT = D`;
- an **unchanged** position stays open;
- a position that **disappears** from the snapshot is closed.

Rows are never deleted, so history is auditable. Stock on date X is
`WHERE X BETWEEN SNAPSHOT_DT AND EFFECTIVE_END_DT`. `AVAILABLE_QTY = ON_HAND_QTY - RESERVED_QTY` is derived in TMP.

`FCT_PRICE`**.** Derived from the prices in the inventory snapshot: when `UNIT_PRICE_AMT` or
`PROMO_PRICE_AMT` changes for an item at a location, the open period is closed and a new one
starts. Gate G10 checks that periods never overlap.

### 4.6 Orphans and late-arriving dimensions

An **orphan** is a fact whose business key has no dimension row (a sale of an item missing from
the item master). Options:

| Option | Effect |
|---|---|
| **Quarantine the fact** (diagram gate G7: quarantine + alert) | Gold stays clean; reconciliation counts the quarantined amount |
| Unknown member (`ITEM_KY = -1`) | Fact kept and totals tie; fix the key later when the item arrives |
| Inferred member | Insert a placeholder dimension row from the fact, enrich it later |

This project follows the diagram: **G7 quarantines and alerts**. Reconciliation (G11) adds the
quarantined amount back so Gold still ties to source.

### 4.7 Aggregates and views

- `AGG_DAILY_ITEM_LOCN_SALES`: `FCT_SALES` grouped by `(DAY_KY, ITEM_KY, LOCN_KY)` with `SALES_LINE_CNT`, `SOLD_QTY`, `SALES_AMT`, `DISCOUNT_AMT`, `TAX_AMT`, `NET_SALES_AMT`.
- Load: delete and recompute only the `DAY_KY` values touched by today's fact load.
- `VW_DAILY_ITEM_LOCN_SALES`: a view over `FCT_SALES` at the same grain. Always live, but recomputed on every query; the table is the fast copy reports use.
- Gate G9 compares the two (or the table and `FCT_SALES`) per `DAY_KY`: any difference fails the run.

---

## 5. Data quality, quarantine and reconciliation

### 5.1 Quarantine, never drop

- A rejected row goes to `SILVER.TMP_QUARANTINE` **with the gate that rejected it**.
- Counts are reported **every run**, not discovered a month later.
- A row count that does not add up is a **failed load**.
- Accounting identity per day: **STG rows = TMP rows + quarantined rows** (+ duplicates removed, which are also quarantined by G4).

`TMP_QUARANTINE` columns (diagram): `INGEST_DT DATE`, `SOURCE_FILE VARCHAR(200)`,
`OBJECT_NM VARCHAR(100)` (which TMP table it was heading for), `GATE_ID VARCHAR(10)` (`G3`),
`REASON_CD VARCHAR(100)` (`NULL_ITEM_ID`), `RAW_RECORD VARIANT` (the whole STG row, including
`_SOURCE_ROW_NUM`), `QUARANTINED_TS TIMESTAMP_NTZ`. Kept 90 days.

| Planted defect (Lesson 10) | Gate | Action |
|---|---|---|
| Two sales rows with null `ITEM_ID` | G3 | quarantine row |
| One sale with negative `SOLD_QTY` | G6 | quarantine row |
| One sale for an item missing from the item master | G7 | quarantine + alert |
| One duplicated transaction line | G4 | keep latest, quarantine the duplicate |
| One store moves to another city | (not a defect) | SCD2 version on `DIM_LOCN` |

### 5.2 Quality gates (red hexagons on the diagram)

| Gate | Boundary | Rule | Action |
|---|---|---|---|
| G1 | Source → STG | File arrives by SLA (sensor timeout); row count > 0 and matches the manifest when one exists | alert, then fail |
| G2 | Source → STG | Header (or positional column count) matches the drop contract; explicit check on the staged file, because the transforming COPY ignores `ERROR_ON_COLUMN_COUNT_MISMATCH` | quarantine file |
| G3 | STG → TMP | Business keys not null (`ITEM_ID`, `LOCN_ID`, `TRANSACTION_ID` + `LINE_NUM`, ...) | quarantine row |
| G4 | STG → TMP | `(TRANSACTION_ID, LINE_NUM)` unique | keep latest, quarantine rest |
| G5 | STG → TMP | `SALE_DT` between D−7 and D | quarantine row |
| G6 | STG → TMP | `NET_SALES_AMT = SALES_AMT − DISCOUNT_AMT` (±0.01) and `SOLD_QTY > 0` | quarantine row |
| G6b | STG → TMP | `RESERVED_QTY ≤ ON_HAND_QTY` | alert only (still load) |
| G7 | TMP → TGT | `ITEM_ID` and `LOCN_ID` resolve to a current dimension key | quarantine + alert |
| G8 | TMP → TGT | Inventory snapshot row count within ±20% of the prior day | fail |
| G9 | TGT → Aggregate | `AGG` totals = `FCT_SALES` totals per `DAY_KY` | fail |
| G10 | TGT | No overlapping price periods per item and location | fail |
| G11 | TGT → Source (departure 6) | Gold net sales + quarantined net sales = file net sales, per day | fail + alert |
| G12 | STG → TMP (departure 8) | Values inside configurable outlier limits ([5.6](#56-outliers-vs-anomalies-and-the-configurable-gate-g12)) | quarantine row + alert |

Each gate is one SQL file under `include/sql/gates/` run by a DAG task. A "fail" gate raises
`AirflowFailException`; a "quarantine" gate writes rows and lets the run continue.

### 5.3 PK checker

One query per Gold table asserting that the natural key is unique. For SCD2 tables, that means
one `CURRENT_FLG = TRUE` row per business key and no overlapping effective ranges. Any
duplicate fails the task, because duplicates double-count facts.

### 5.4 Reconciliation (G11)

Tie Gold back to source per day:

```
expected_gold = source_net_sales − quarantined_net_sales   (from STG_SALES and TMP_QUARANTINE)
variance      = expected_gold − FCT_SALES net sales         (must be 0 for every day)
```

- `source_net_sales` is computed from `STG_SALES` (the file as landed), not from TMP, so cleaning mistakes can't hide.
- The output is a 2-column, 5-row table (`DAY_KY | VARIANCE_AMT`) in `AUDIT`. Any non-zero row means fail the DAG and alert.
- Use `COALESCE(SUM(...), 0)` on both sides, and compare amounts as `NUMBER(18,2)`, never floats.

### 5.5 Retention and the reload of one bad day D

| Layer | Retention | Why |
|---|---|---|
| STG (Bronze) | 400 days, then archive | Year-over-year compares and one-day reloads |
| `TMP_QUARANTINE` | 90 days | Long enough to investigate |
| TMP (Silver) | 3 years | Rebuild Gold without Bronze |
| TGT (Gold) | 7 years | Finance and audit horizon |
| Aggregate | 3 years | Rebuildable from TGT |
| Time Travel | 1 day Bronze/Silver, 7 days Gold | Undo a bad publish quickly |

Reload of day D (each step is idempotent, so a rerun is safe):

1. Land the corrected file for D: `DELETE FROM STG_* WHERE _INGEST_DT = D`, then `COPY` again. The bad file stays in `data/archive/` (departure 5).
2. Rebuild the TMP rows that came from D (`DELETE ... WHERE _INGEST_DT = D` / full refresh for masters).
3. `FCT_SALES`: `MERGE` on `(TRANSACTION_ID, LINE_NUM)`; recompute `AGG` only for the touched `DAY_KY` values.
4. `FCT_INVENTORY`: re-apply D's changes as new versions (rows are soft-closed, never deleted).
5. `FCT_PRICE`: re-derive periods from D onward for the changed item and location pairs.
6. SCD2 dims: undo versions opened or closed on D, then re-apply. `DIM_CUSTOMER`: SCD1 `MERGE`.
7. Re-run G7–G11, then publish.

### 5.6 Outliers vs anomalies, and the configurable gate G12

| | **Valid outlier** | **Anomaly** (a problem) |
|---|---|---|
| Meaning | Unusual but true | Looks like data but is wrong |
| Example | 480 units bought by a business member | 10,000 units: a key held down |
| Example | An item that really costs 2,499.99 | That item sold at 2,499,990.00: a decimal slip (×1000) |
| Action | Keep; must reach Gold | Quarantine (G12), alert, fix at source, reload the day |

Arithmetic rules (G6) cannot see anomalies: `2,499,990.00 × 1 − 0 = 2,499,990.00` adds up.
You need **range** rules. Three ways to detect, simplest first:

| Method | Rule | Good for | Weakness |
|---|---|---|---|
| Absolute limit | `SOLD_QTY > 1000` | Quantities, stock | One limit for every item |
| **Relative to a reference** | `UNIT_PRICE_AMT / reference price > 10` or `< 0.1` | Prices: a 0.05 item is fine, a 17.50 item at 0.17 is not | Needs a trusted reference (here: the item's price in today's inventory file) |
| Statistical | Value outside `median ± k × spread` of recent history (z-score, IQR) | Many days of data | Needs history; first days can't be scored |

**Configurable** means the limits live in the `medallion_config` Variable, not in SQL or the DAG:

```json
"outlier_rules": { "price_ratio_max": 10, "price_ratio_min": 0.1,
                   "max_sold_qty": 1000, "max_line_net_amt": 50000, "max_on_hand_qty": 100000 }
```

The gate SQL reads them as `{{ var.json.medallion_config.outlier_rules.max_sold_qty }}`, so
tightening a limit is a Variable import, not a deploy. Each hit goes to `TMP_QUARANTINE` with
`GATE_ID = 'G12'` and a `REASON_CD` (`PRICE_TOO_HIGH`, `PRICE_TOO_LOW`, `QTY_TOO_HIGH`,
`LINE_AMT_TOO_HIGH`, `ON_HAND_TOO_HIGH`). Reconciliation (G11) still ties, because quarantined
amounts are added back.

Choosing limits: too tight quarantines real sales (false alarms); too loose lets typos into
finance reports. Start wide, look at what gets caught, then tighten.

---

## 6. Idempotency playbook

**Idempotent** means running the same date twice gives identical row counts and totals.

| Technique | Use when | Here |
|---|---|---|
| `DELETE WHERE _INGEST_DT = D` then `INSERT` / `COPY FORCE` | Daily slices | STG tables, `TMP_SALES_LINE`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_QUARANTINE` |
| `INSERT OVERWRITE` | Small tables fully recomputed each run | `TMP_ITEM_HIER`, `TMP_LOCN_HIER` |
| `MERGE` | Rows can change | `DIM_CUSTOMER` (SCD1), `FCT_SALES` |
| `INSERT ... WHERE NOT EXISTS` | Rows never change once written | New dim members and SCD2 versions, `FCT_INVENTORY`, `FCT_PRICE` versions |
| Delete + recompute touched keys | Derived tables | `AGG_DAILY_ITEM_LOCN_SALES` by `DAY_KY` |

1. **MERGE vs INSERT OVERWRITE vs NOT EXISTS:** MERGE when existing rows can change; INSERT OVERWRITE when the table is cheap to rebuild completely; NOT EXISTS when rows never change once written.
2. **Deterministic vs sequence keys:** see [4.2](#42-natural-keys-vs-surrogate-keys). With sequences, inserts must be guarded by `NOT EXISTS` so a re-run doesn't take new numbers.
3. **SCD2 recompute on re-run:** see [4.4](#44-slowly-changing-dimensions-scd). Compare hashes before closing; undo day D before correcting it.
4. **Partition pruning by** `_INGEST_DT` **/** `DAY_KY`**:** see [3.5](#35-micro-partitions-pruning-and-the-diagrams-partitioning). Every reload is scoped to one day, so it stays cheap.
5. `COPY FORCE = TRUE` **vs dedupe via staging:** `FORCE` ignores load history and duplicates data unless the day is deleted first. The alternative is to load into a staging table and insert with `NOT EXISTS` on (`_SOURCE_FILE`, `_SOURCE_ROW_NUM`).

---

## 7. Checks

No test framework. Each check below is either one command or a SQL query that runs as a task
in the DAG, so a failure turns the task red.

| Check | How | When |
|---|---|---|
| DAG files parse (no syntax errors, missing imports or cycles) | `airflow dags list-import-errors` → `No data found`; or the red banner in the UI | After editing any DAG |
| No hard-coded secrets, paths or Snowflake names in DAGs | One `Select-String` line (Lesson 1, step 1.10) → no output | After editing any DAG |
| Quality gates G1–G10, PK checker | SQL in `include/sql/gates/`, run by a DAG task that fails or quarantines | Every DAG run (L4, L7, L9, L10, L11) |
| Gold ties to source (G11) | Reconciliation SQL + task that fails on variance ≠ 0 | Every DAG run (L11) |

---

## 8. Docker essentials

| Term | Meaning |
|---|---|
| Image | Read-only template. We use the official `apache/airflow:3.3.2` as-is; it already includes the Snowflake provider |
| Container | A running instance of an image |
| `docker compose up -d` | Starts all services in the background |
| Bind mount (`./dags:/opt/airflow/dags`) | A host folder visible inside the container; edits appear immediately |
| `.env` | Variables Compose substitutes into `docker-compose.yaml` (`FERNET_KEY`, `AIRFLOW_UID`) |
| `docker compose run --rm airflow-cli <cmd>` | One-off command in a fresh container, removed afterwards |

---

## 9. Glossary

| Term | Meaning |
|---|---|
| Aggregate (AGG) | Gold table rolled up from TGT facts for fast reporting |
| Anti-join | Rows in A with no match in B (`NOT EXISTS`) |
| Backfill | Running a DAG for past logical dates |
| Business key | The source system's identifier (`*_ID`) |
| Control total | A sum or count compared between two layers to prove nothing was lost |
| Gate | A quality rule on a layer boundary with an action: fail, quarantine or alert |
| Grain | What one row of a table represents |
| Idempotent | Same input, run twice, same result |
| Logical date | The date a DAG run represents (`ds`, day D) |
| Orphan | A fact whose dimension key cannot be found |
| Pruning | Skipping micro-partitions that can't match a filter |
| Quarantine | `TMP_QUARANTINE`: holding table for rows that failed a gate |
| STG / TMP / TGT | Staging (Bronze), temporary/compare (Silver), target (Gold) table types |
| Surrogate key | A warehouse-generated key used for joins (`*_KY`) |
| Variance | Expected minus actual in reconciliation; must be 0 |

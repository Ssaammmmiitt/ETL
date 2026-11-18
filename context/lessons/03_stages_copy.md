# Lesson 3: Stages, PUT, LIST and COPY INTO

| | |
|---|---|
| **Goal** | Move the 10 Nov files from your laptop into Snowflake: upload them to an **internal stage**, list them, and **COPY** them into the 4 STG tables |
| **You build** | 1 stage SQL, 1 PUT SQL, 1 LIST SQL, 4 COPY SQL files, 1 DAG |
| **Result** | `STG_SALES` 5,000 rows · `STG_INVENTORY` 1,502 · `STG_ITEM_MASTER` 101 · `STG_STORE_MASTER` 25, each row with lineage |
| **Not yet** | Waiting for files (L4), safe re-runs and the G2 check (L5) |
| **Theory (optional)** | [3.3 Stages](../THEORY.md#33-stages) · [3.4 File formats and COPY INTO](../THEORY.md#34-file-formats-and-copy-into) |

---

## 3.1 The idea in one picture

```
data/landing/sales_20261110.csv                         (your laptop, mounted in the container)
        │  PUT   (upload + gzip)
        ▼
@MY_INT_STAGE/2026-11-10/sales_20261110.csv.gz          (a file area inside Snowflake)
        │  LIST  (what's there?)
        │  COPY INTO ... FROM (SELECT $1, $2, ... )     (read the file, insert rows)
        ▼
BRONZE.STG_SALES   5,000 rows + _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _INGEST_TS, _BATCH_ID
```

Snowflake can't read files on your laptop. It reads files on a **stage**, so every load is two
steps: **get the file onto a stage**, then **COPY from the stage into a table**.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Stage | A named file area that Snowflake can read | `@MY_INT_STAGE` |
| Internal stage | Storage managed by Snowflake inside your account; you upload with `PUT`. No cloud account needed | `MY_INT_STAGE` |
| `PUT` | Upload a local file to an internal stage | runs on the Airflow worker |
| `LIST` | Show the files on a stage (name, size, md5, last modified) | proves the files arrived |
| `COPY INTO` | Bulk-load staged files into a table | 1 statement per file |
| `$1, $2, …` | Field 1, field 2, … of each line in the file | positional mapping |
| `METADATA$FILENAME`, `METADATA$FILE_ROW_NUMBER` | Which staged file and which line a row came from | fill `_SOURCE_FILE`, `_SOURCE_ROW_NUM` |
| Load history | Snowflake remembers for 64 days which files each table has loaded and skips them | why a re-run loads 0 rows |

**Folder per business date.** Files go to `MY_INT_STAGE/2026-11-10/`. One day = one folder, so
LIST, COPY and (later) reloading a bad day only touch that day.

---

## 3.2 Two ways to map file columns

| | By name (`MATCH_BY_COLUMN_NAME`) | By position (`SELECT $1, $2, …`) |
|---|---|---|
| Mapping | Header `ITEM_ID` → column `ITEM_ID` | 1st field → 1st listed column |
| Needs a header | Yes (`PARSE_HEADER = TRUE`) | No |
| Column order in file | Can change | **Must not change** (it's the contract) |
| Add `_INGEST_DT`, `_BATCH_ID` | **No**: only file metadata can be added | **Yes**: any value in the `SELECT` |
| Wrong column count | Configurable error | **Ignored**: missing fields become `NULL` |
| Used here | Demo only (exercise 4) | **All 4 files**; inventory is the brief's "positional load" (no header at all) |

We need lineage on every row, so the pipeline uses positional COPY everywhere. The price is that
Snowflake no longer checks the column count. Gate G2 gets an explicit check in Lesson 5.

---

## 3.3 Code walkthrough

### a) `include/sql/setup/02_stages.sql`: the internal stage

```sql
CREATE STAGE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE
    COMMENT = 'Internal stage: daily source files under YYYY-MM-DD/ folders, gzip-compressed by PUT';
```

- `IF NOT EXISTS`**, not** `OR REPLACE`**:** replacing a stage **deletes every file in it**. File formats could use `OR REPLACE` (no data); stages and tables can't.
- No file format attached: each COPY names its own (`FF_CSV_HEADER` or `FF_CSV_POSITIONAL`), so you can see it in the SQL.

### b) `include/sql/stage/put_file.sql`: upload one file

```sql
PUT 'file://{{ var.json.medallion_config.landing_path }}/{{ params.source }}_{{ ds_nodash }}.csv'
    @{{ ... }}.MY_INT_STAGE/{{ ds }}/
    AUTO_COMPRESS = TRUE
    OVERWRITE     = FALSE
```

Rendered for `put_sales`:

```sql
PUT 'file:///opt/airflow/data/landing/sales_20261110.csv' @MEDALLION_DB.BRONZE.MY_INT_STAGE/2026-11-10/ AUTO_COMPRESS = TRUE OVERWRITE = FALSE
```

| Part | Meaning |
|---|---|
| `file:///opt/airflow/...` | Path **inside the container** (your `data\` folder is mounted there). The path comes from the Variable (`landing_path`), not the DAG |
| `{{ params.source }}` | Set per task by the DAG (`params={"source": source}`), so **one SQL file serves all 4 uploads** |
| `{{ ds_nodash }}` / `{{ ds }}` | Run date: `20261110` for the file name, `2026-11-10` for the folder |
| `AUTO_COMPRESS = TRUE` | Gzips before upload → `sales_20261110.csv.gz`: smaller and faster. COPY unzips automatically |
| `OVERWRITE = FALSE` | A file already on the stage is skipped → the re-run in exercise 2 uploads nothing |

**Why this file has no** `--` **comments above** `PUT`**:** the Python connector only treats the
command as a local upload when the statement **starts with** `PUT`. Leading comments make it
send the text to the Snowflake server instead, which fails with
`Unsupported feature 'unsupported_requested_format:snowflake'`. `CREATE`, `COPY` and `LIST` are
fine with comments; `PUT` (and `GET`) are not.

`PUT` can't run in a Snowsight worksheet (the browser can't reach your disk). It needs a client:
here, the Snowflake Python connector inside Airflow.

### c) `include/sql/checks/list_stage.sql`: what's on the stage

```sql
LIST @{{ ... }}.MY_INT_STAGE/{{ ds }}/;
```

Returns one row per file: `name` (`my_int_stage/2026-11-10/sales_20261110.csv.gz`), `size`,
`md5`, `last_modified`. Here you just read it. In L4 it becomes a check: "4 files, or fail".

### d) `include/sql/load/bronze/stg_<source>.sql`: COPY into STG (4 files, same shape)

```sql
COPY INTO {{ ... }}.STG_INVENTORY (
    SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY,
    UNIT_PRICE_AMT, PROMO_PRICE_AMT,
    _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID          -- (1) target columns
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8,                         -- (2) file fields by position
           '{{ ds }}'::DATE, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{{ run_id }}'   -- (3) lineage
    FROM @{{ ... }}.MY_INT_STAGE/{{ ds }}/                         -- (4) the day's folder
)
FILES       = ('inventory_{{ ds_nodash }}.csv.gz')                -- (5) exactly one file
FILE_FORMAT = (FORMAT_NAME = '{{ ... }}.FF_CSV_POSITIONAL')       -- (6) how to read it
ON_ERROR    = ABORT_STATEMENT;                                     -- (7) any bad line → load nothing
```

| # | Why |
|---|---|
| 1 | Listing target columns makes the mapping explicit. `_INGEST_TS` is left out so its default `CURRENT_TIMESTAMP()` fills it |
| 2 | `$4` → `ON_HAND_QTY` because it's the 4th field. That's the positional contract from L2 |
| 3 | `'2026-11-10'::DATE` = business day (partition); `METADATA$FILENAME` = `2026-11-10/inventory_20261110.csv.gz`; `METADATA$FILE_ROW_NUMBER` = line number; `run_id` = which Airflow run |
| 4–5 | Folder + `FILES` = load **only** this day's file, never another day's by accident |
| 6 | Inventory: `FF_CSV_POSITIONAL` (no header). Other 3: `FF_CSV_HEADER` (skip line 1) |
| 7 | All-or-nothing: a half-loaded file is worse than a failed task you can re-run |

The sales, item and store files are the same pattern with 16, 15 and 14 fields.

### e) `dags/03_stage_load.py`: the DAG

```python
create_stage = SQLExecuteQueryOperator(task_id="create_stage", conn_id=SNOWFLAKE_CONN_ID, sql="setup/02_stages.sql")
list_stage   = SQLExecuteQueryOperator(task_id="list_stage",   conn_id=SNOWFLAKE_CONN_ID, sql="checks/list_stage.sql")

for source in SOURCES:
    put_file = SQLExecuteQueryOperator(
        task_id=f"put_{source}", conn_id=SNOWFLAKE_CONN_ID,
        sql="stage/put_file.sql",
        params={"source": source},            # fills {{ params.source }} in the SQL
    )
    copy_file = SQLExecuteQueryOperator(
        task_id=f"copy_{source}", conn_id=SNOWFLAKE_CONN_ID,
        sql=f"load/bronze/stg_{source}.sql",  # one load file per table
    )
    create_stage >> put_file >> list_stage >> copy_file
```

```
               ┌─► put_sales ────────┐              ┌─► copy_sales
create_stage ──┼─► put_inventory ────┼─► list_stage ┼─► copy_inventory
               ├─► put_item_master ──┤              ├─► copy_item_master
               └─► put_store_master ─┘              └─► copy_store_master
```

| Task | Purpose |
|---|---|
| `create_stage` | Makes sure the stage exists (safe to repeat) |
| `put_<source>` ×4 | Uploads one file; the 4 run in parallel |
| `list_stage` | Waits for **all** uploads (fan-in), then shows what's on the stage, a checkpoint before loading |
| `copy_<source>` ×4 | Loads one file into its STG table; parallel, they don't depend on each other |

`params` vs a separate SQL file: PUT is the same command for every file, so one file + `params`.
Each COPY has different columns, so one file per table (the brief's rule).

---

## 3.4 Run it

**1. Make sure the files exist** (from Lesson 2): `Get-ChildItem data\landing` shows 4 CSVs.

**2. Run the DAG:**

```powershell
docker compose run --rm airflow-cli airflow dags test 03_stage_load 2026-11-10
```

Expected: 10 tasks succeed. In the UI open each task's **XCom** (the query result):

| Task | You should see |
|---|---|
| `put_sales` | `source = sales_20261110.csv`, `target = sales_20261110.csv.gz`, `target_compression = GZIP`, `status = UPLOADED` |
| `list_stage` | 4 rows: `my_int_stage/2026-11-10/<source>_20261110.csv.gz` |
| `copy_sales` | `status = LOADED`, `rows_parsed = 5000`, `rows_loaded = 5000`, `errors_seen = 0` |
| `copy_inventory` / `copy_item_master` / `copy_store_master` | `rows_loaded` 1502 / 101 / 25 |

Check **Rendered Templates** of `copy_inventory`: real names, `'2026-11-10'::DATE` and your `run_id`.

**3. Verify in Snowsight:**

```sql
USE ROLE MEDALLION_ROLE;  USE WAREHOUSE MEDALLION_WH;
USE SCHEMA MEDALLION_DB.BRONZE;

LIST @MY_INT_STAGE/2026-11-10/;                                    -- 4 .csv.gz files

SELECT 'STG_SALES' AS table_name, COUNT(*) AS row_count FROM STG_SALES
UNION ALL SELECT 'STG_INVENTORY',    COUNT(*) FROM STG_INVENTORY
UNION ALL SELECT 'STG_ITEM_MASTER',  COUNT(*) FROM STG_ITEM_MASTER
UNION ALL SELECT 'STG_STORE_MASTER', COUNT(*) FROM STG_STORE_MASTER;  -- 5000, 1502, 101, 25

SELECT _INGEST_DT, _SOURCE_FILE, _SOURCE_ROW_NUM, _BATCH_ID, _INGEST_TS,
       ITEM_ID, LOCN_ID, ON_HAND_QTY, PROMO_PRICE_AMT
FROM STG_INVENTORY ORDER BY _SOURCE_ROW_NUM LIMIT 3;               -- lineage filled; empty promo = NULL

SELECT SUM(NET_SALES_AMT::NUMBER(18,2)) FROM STG_SALES;           -- 2843788.51 = generator total
SELECT COUNT(*) FROM STG_SALES WHERE CUSTOMER_ID IS NULL;         -- 1025 non-member lines
```

The sum matches the generator, so **Bronze ties to the file**. That's the first link of the
reconciliation chain (file → STG → … → Gold).

**4. Look inside a staged file without loading it:**

```sql
SELECT METADATA$FILE_ROW_NUMBER, $1, $2, $3, $4, $8
FROM @MY_INT_STAGE/2026-11-10/inventory_20261110.csv.gz
     (FILE_FORMAT => 'MEDALLION_DB.BRONZE.FF_CSV_POSITIONAL')
LIMIT 3;
```

This is how you inspect a suspicious file before it touches a table. G2 uses it in L5.

**5. Hard-coding check** (no output = pass):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

---

## 3.5 Points to remember

- **Two steps, always:** file → stage (`PUT` or cloud upload) → table (`COPY`). Snowflake never reads your laptop.
- **Stages hold files;** `OR REPLACE` **deletes them.** Use `CREATE STAGE IF NOT EXISTS`.
- **One folder per business date** (`2026-11-10/`) + `FILES = (...)` → a COPY can only load its own day.
- **Positional = contract.** `$4` is "the 4th field", whatever it contains. A reordered file loads silently into the wrong columns.
- **A** `SELECT` **in COPY** lets you add lineage, but switches off the column-count check. That's why G2 is an explicit step (L5).
- **Load history** (64 days) makes COPY skip files it already loaded. That's safe against accidental double loads, but a *corrected* file for the same day needs "delete the day, then `FORCE` reload" (L5).
- `ON_ERROR = ABORT_STATEMENT`: all or nothing per file. Bad *values* still land (everything is `VARCHAR`); only unreadable *lines* abort.
- `PUT` **paths are container paths** (`/opt/airflow/...`), and they come from the Variable.
- `PUT` **/** `GET` **must start the statement.** No `--` comments above them, or the connector won't upload and you get `unsupported_requested_format:snowflake`.

---

## 3.6 Exercises

1. **Read the result.** In `copy_sales`'s XCom, what's the difference between `rows_parsed` and `rows_loaded`? When would they differ?
2. **Run it twice.** Run the DAG again for 2026-11-10. What `status` do the PUT tasks show, what do the COPY tasks return, and did any row count change? Which Snowflake feature protected you? Now imagine the source sends a **corrected** `sales_20261110.csv`: what would PUT (with `OVERWRITE = FALSE`) and COPY do, and why is that a problem?
3. **Line numbers.** Run `SELECT MIN(_SOURCE_ROW_NUM) FROM STG_SALES;`. Is the first data row 1 or 2 (remember the header)? If a row is quarantined with `_SOURCE_ROW_NUM = 1000`, which line do you open in the editor?
4. **By name, hands-on** (Snowsight). Load 3 columns, in a different order, by header name into a temporary table:
   ```sql
           CREATE TEMPORARY TABLE DEMO_BY_NAME (DEPARTMENT_ID VARCHAR, ITEM_NM VARCHAR, ITEM_ID VARCHAR);
           COPY INTO DEMO_BY_NAME
           FROM @MY_INT_STAGE/2026-11-10/ FILES = ('item_master_20261110.csv.gz')
           FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
           MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
           SELECT * FROM DEMO_BY_NAME LIMIT 3;
   ```
5. **Positional risk.** Suppose the inventory source swaps `ON_HAND_QTY` and `RESERVED_QTY`. Does the COPY fail? Which gate from the [THEORY 5.2 table](../THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) would notice? Which rows would slip through unnoticed?
6. **Why the fan-in?** What would go wrong if `copy_sales` depended only on `put_sales` (no `list_stage` in between)? What does `list_stage` give you that a single PUT doesn't?

---

## 3.7 Checkpoint

Reply with:

- [x] `03_stage_load`: 10 tasks green
- [x] `put_sales` XCom: `UPLOADED`, `GZIP`
- [x] `LIST`: 4 `.csv.gz` files under `2026-11-10/`
- [x] Row counts 5000 / 1502 / 101 / 25; `SUM(NET_SALES_AMT)` = 2843788.51; 1025 NULL customers
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6 (exercise 2: paste the re-run's PUT status and COPY result)

## Troubleshooting

| Symptom | Fix |
|---|---|
| `File doesn't exist: ['/opt/airflow/data/landing/...']` | Run the L2 generator; check `./data` is mounted (`docker compose config` → `volumes`) |
| `put_*` ok but `status = SKIPPED` on the first run | The file is already on the stage from an earlier try: fine, COPY still loads it if this table hasn't |
| COPY returns `Copy executed with 0 files processed.` | The table already loaded that file (load history). Expected on a re-run; see exercise 2 |
| `Number of columns in file (16) does not match ...` | You ran a COPY **without** the `SELECT`; use the files in `load/bronze/` |
| `NULL result in a non-nullable column` | A lineage value missing in the `SELECT` (check the 4 extra values) |
| `SQL compilation error: Stage ... does not exist` | `create_stage` failed or a typo in the stage name; run `SHOW STAGES IN SCHEMA MEDALLION_DB.BRONZE` |
| `Insufficient privileges to operate on stage` | Re-run the L1 bootstrap so `MEDALLION_ROLE` owns `BRONZE` |
| `TemplateNotFound: stage/put_file.sql` | File missing under `include/sql/stage/` |
| `Unsupported feature 'unsupported_requested_format:snowflake'` on PUT | A `--` comment sits above `PUT`. The statement must start with `PUT` (see `stage/put_file.sql`) |

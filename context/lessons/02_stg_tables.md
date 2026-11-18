# Lesson 2: Simple data, STG tables and file formats

| | |
|---|---|
| **Goal** | Generate one day of source files, then create the 2 **file formats** and 4 **STG** (Bronze) tables that will receive them ([diagram](../architecture-Simple%20view.png)) |
| **You build** | 2 Python generators, 1 file-format SQL, 4 table SQL files, 1 DAG |
| **Not yet** | No data is loaded. Files reach Snowflake in L3; the tables are filled in L5 |
| **Theory (optional)** | [3.4 File formats](../THEORY.md#34-file-formats-and-copy-into) · [3.10 Data types](../THEORY.md#310-data-types-and-why-we-pick-them) · [5.6 Outliers](../THEORY.md#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |

---

## 2.1 The idea in one picture

```
generate_simple.py ──► data/landing/*.csv ──(L3: PUT to stage)──(L5: COPY INTO)──► BRONZE.STG_* tables
                                                                                   ▲
02_stg_setup DAG ── creates the empty tables + the file formats (this lesson) ┘
```

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Source drop | A file a source system sends every day | `sales_20261110.csv` |
| Header | First line with column names | inventory has **none** on purpose |
| File format | A Snowflake object that says *how to read* a CSV (delimiter, header, NULLs) | `FF_CSV_HEADER`, `FF_CSV_POSITIONAL` |
| DDL | SQL that creates structure (`CREATE TABLE`), not data | `ddl/bronze/stg_sales.sql` |
| STG table | Bronze: an exact copy of the file + a few tracking columns | `STG_SALES` |
| Lineage column | A column *we* add to trace each row back to its file and run | `_SOURCE_FILE`, `_BATCH_ID` |
| Jinja | `{{ ... }}` placeholders Airflow fills in before sending SQL to Snowflake | `{{ var.json.medallion_config.database }}` → `MEDALLION_DB` |
| Operator / task | An operator is a task type; a task is one box in the DAG | `SQLExecuteQueryOperator` → `create_stg_sales` |

---

## 2.2 The data

| File | Rows | Header | One row = | Goes to |
|---|---|---|---|---|
| `sales_20261110.csv` | 5,000 | yes | one till line (+ member details) | `STG_SALES` |
| `inventory_20261110.csv` | 1,502 | **no** | one item at one store: stock + price | `STG_INVENTORY` |
| `item_master_20261110.csv` | 101 | yes | item → subcategory → category → department | `STG_ITEM_MASTER` |
| `store_master_20261110.csv` | 25 | yes | store → city → region → division | `STG_STORE_MASTER` |
| `data/seed/seed_dim_day.csv` | 730 | yes | one calendar day (2026–2027) | `DIM_DAY` in Gold directly (L8) |

Real lines from the files:

```
sales:      T20261110-1001-0002,2,2026-11-10,1001,100010,M000016,Ivy Garcia,EXECUTIVE,ivy.garcia16@example.com,555-0016,5.000,8.60,0.75,43.00,3.38,42.25
inventory:  2026-11-10,100001,1001,20.000,2.000,6.000,2.60,
item:       100001,Family Milk 1,"Family Milk, pack 2",Great Value,FOOD,SC101,Milk,...
```

- Sales maths: `5.000 × 8.60 = 43.00` (SALES), `43.00 − 0.75 = 42.25` (NET), `42.25 × 8% = 3.38` (TAX).
- Inventory: no header; the trailing comma = an empty promo price (→ `NULL`).
- Item: `"Family Milk, pack 2"` is quoted because it contains a comma.

**What's inside the data, and why:**

| Kind | Examples | Purpose |
|---|---|---|
| Normal variety | 1,025 non-member lines (empty customer columns), weighed items (`0.750` kg), promo prices on every 10th item | Test NULL handling, `NUMBER(18,3)`, price logic |
| **Valid outliers** (keep them) | Item at 2,499.99; item at 0.05; 480-unit bulk buy; 100% discount (net 0.00); 0.005 kg; stock 0 and 25,000 | Extreme but true: must reach Gold |
| **Anomalies** (catch them in L7) | A1 price ×1000 (2,499,990.00); A2 10,000 units; A3 price ÷100 (0.17 instead of 17.50); A4 stock 9,999,999 | Wrong but arithmetically consistent. Gate G12 must quarantine them |

The four anomalies add **2,639,490.51** to the day's net sales, so the file total is about 14 times the real one.
In L7, gate G12 catches them with limits stored in `variables.json` → `outlier_rules`. One of those limits flags any price more than 10 times the item's normal price.

---

## 2.3 Code walkthrough

### a) `scripts/generate_simple.py`: makes the 4 files

**Flow (bottom of the file):**

```python
ITEMS, PRICES = build_items(100)            # 100 items + their prices
PRICES["100045"] = (0.05, None, ...)        # one near-free item (outlier)
STORES = build_stores()                     # 25 stores, one per city
SALES = build_sales(ITEMS, STORES, PRICES)  # 25 stores x 100 transactions x 1-3 lines = 5,000
INVENTORY = build_inventory(...)            # 25 stores x 60 items = 1,500
add_outliers(...)                           # +1 premium item, +2 inventory rows, edits a few lines
add_anomalies(SALES, INVENTORY)             # plants A1-A4
write_csv(...)  x4                          # writes data/landing/*.csv
print(...)                                  # control totals you compare later
```

**Key pieces:**

| Code | What it does | Why |
|---|---|---|
| `LANDING_DIR = Path(__file__).resolve().parent.parent / "data" / "landing"` | Finds `data/landing` relative to the script | Works from any folder, no hard-coded `C:\...` path |
| `DEPARTMENTS = [("D01", "Grocery", "FOOD", [...]), ...]` | Nested lists = the hierarchy department → category → subcategory | Typed out because it's small and readable |
| `item_num % len(BRANDS)` | `%` (modulo) cycles through a list: 0, 1, 2, …, 11, 0, 1, … | **Deterministic**: no `random`, so every run writes identical files |
| `txn_id = f"T{DATE_NODASH}-{locn_id}-{txn_n:04d}"` | Builds `T20261110-1001-0002`; `:04d` pads to 4 digits | Readable, unique transaction id |
| `["", "", "", "", ""] if txn_n % 5 == 0 else customer(...)` | Every 5th transaction has empty customer columns | Non-members → `NULL` in Snowflake |
| `qty = 0.5 + ((txn_n + line) % 5) * 0.25` | PERISHABLE items get 0.50, 0.75, … 1.50 kg | Why quantities need 3 decimals |
| `discount = min(..., sales_amt)` | Discount can never exceed the sale | NET is never negative |
| `f"{qty:.3f}"`, `f"{price:.2f}"` | Write numbers as text with fixed decimals | Files are text; `.2f` for money, `.3f` for quantity |
| `set_sales_line(row, qty=..., price=...)` | Changes one line **and recomputes** SALES, TAX, NET | Outliers and anomalies stay arithmetically correct, so only range checks can catch them |
| `write_csv(name, header, rows)` with `header=None` for inventory | Skips the header row | Creates the positional file |
| `open(path, "w", newline="")` + `lineterminator="\n"` | Avoids blank lines / `\r\n` on Windows | Same bytes on every machine |

**Row layout = list positions.** In `set_sales_line`, `row[10]` is `SOLD_QTY`, `row[11]` `UNIT_PRICE_AMT`,
`row[12]` `DISCOUNT_AMT`, `row[13]` `SALES_AMT`, `row[14]` `TAX_AMT`, `row[15]` `NET_SALES_AMT`
(count from 0 in `SALES_HEADER`).

### b) `scripts/generate_seed_dim_day.py`: the calendar

```python
iso_year, iso_week, iso_weekday = day.isocalendar()   # Monday=1 ... Sunday=7
rows.append([day.strftime("%Y%m%d"), day.isoformat(), ...,
             "Y" if iso_weekday >= 6 else "N",          # WEEKEND_FLG
             "Y" if day in HOLIDAYS else "N"])          # HOLIDAY_FLG
day += timedelta(days=1)
```

`DAY_KY` = `20261110` (the date as a number, which sorts and joins fast). It skips STG/TMP because no
source system sends a calendar. It's version controlled, so it's traceable.

### c) `include/sql/setup/01_file_formats.sql`: how Snowflake reads CSV

```sql
CREATE OR REPLACE FILE FORMAT {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_HEADER
    TYPE = CSV   FIELD_DELIMITER = ','   SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'   EMPTY_FIELD_AS_NULL = TRUE   NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = FALSE   ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE   ENCODING = 'UTF8';
```

| Option | Effect | Example |
|---|---|---|
| `SKIP_HEADER = 1` / `0` | Skip the first line (header) / don't | `FF_CSV_POSITIONAL` uses 0 for inventory |
| `FIELD_OPTIONALLY_ENCLOSED_BY = '"'` | Commas inside quotes stay in one field | `"Family Milk, pack 2"` = 1 column, not 2 |
| `EMPTY_FIELD_AS_NULL`, `NULL_IF` | Empty or the text `NULL` → real `NULL` | Non-member `CUSTOMER_ID` |
| `TRIM_SPACE = FALSE` | Keep spaces as they are | Bronze keeps the raw value; Silver trims |
| `ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE` | Wrong number of columns → load fails, **but only for a plain COPY**. Our COPY uses `SELECT $1, $2, …` (Lesson 3), which ignores it, so gate G2 gets its own check in L5 | Source added a column → you notice |

`CREATE OR REPLACE` is safe here: a file format holds no data.

### d) `include/sql/ddl/bronze/stg_*.sql`: the 4 tables

```sql
CREATE OR REPLACE TABLE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_INVENTORY (
    SNAPSHOT_DT      VARCHAR,          -- 8 file columns, in FILE ORDER, all text
    ITEM_ID          VARCHAR,
    ...
    PROMO_PRICE_AMT  VARCHAR,
    _INGEST_DT       DATE          NOT NULL,                            -- business day D
    _SOURCE_FILE     VARCHAR       NOT NULL,                            -- which file
    _SOURCE_ROW_NUM  NUMBER        NOT NULL,                            -- which line
    _INGEST_TS       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), -- when loaded
    _BATCH_ID        VARCHAR       NOT NULL                             -- Airflow run_id
);
```

After Airflow renders it, Snowflake receives `CREATE OR REPLACE TABLE MEDALLION_DB.BRONZE.STG_INVENTORY (...)`.

| Column(s) | Type | Why |
|---|---|---|
| File columns | `VARCHAR` | A bad value like `3.9O` must still land so we can quarantine it later. A number type would reject the whole row |
| `_INGEST_DT` | `DATE` | Partition: reload a day with `WHERE _INGEST_DT = '2026-11-10'` |
| `_SOURCE_ROW_NUM` | `NUMBER` | Points to the exact line in the file |
| `_INGEST_TS` | `TIMESTAMP_NTZ` | Load time; "no time zone" is the simplest choice |
| `_SOURCE_FILE`, `_BATCH_ID` | `VARCHAR` | Labels: which file, which run |

Column counts: `STG_SALES` 16 + 5, `STG_INVENTORY` 8 + 5, `STG_ITEM_MASTER` 15 + 5, `STG_STORE_MASTER` 14 + 5.

### e) `include/medallion/config.py` + `include/config/variables.json`: where names live

| Kind of value | Lives in | Example |
|---|---|---|
| Ids used by DAG code | `config.py` | `SNOWFLAKE_CONN_ID = "snowflake_medallion"`, `SOURCES = ["sales", "inventory", "item_master", "store_master"]` |
| Database / schema names, paths, limits | Variable `medallion_config` (`variables.json`) | `"database": "MEDALLION_DB"`, `"schemas": {"bronze": "BRONZE"}` |
| Table names | SQL files | `STG_INVENTORY` |
| Credentials | Airflow Connection | key file path, account |

`{{ var.json.medallion_config.schemas.bronze }}` reads the Variable as JSON and takes `schemas` → `bronze` = `BRONZE`.

### f) `dags/02_stg_setup.py`: the DAG

```python
@dag(
    dag_id="02_stg_setup",
    start_date=datetime(2026, 11, 1),
    schedule=None,                          # manual only: setup runs once
    catchup=False,
    template_searchpath=[str(SQL_DIR)],     # where to find "setup/..." and "ddl/..." files
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["medallion", "02"],
)
def stg_setup():
    create_file_formats = SQLExecuteQueryOperator(
        task_id="create_file_formats",
        conn_id=SNOWFLAKE_CONN_ID,          # credentials come from the Connection
        sql="setup/01_file_formats.sql",    # a path ending in .sql → Airflow reads + renders the file
    )
    for source in SOURCES:                  # 4 sources → 4 tasks
        create_table = SQLExecuteQueryOperator(
            task_id=f"create_stg_{source}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"ddl/bronze/stg_{source}.sql",
        )
        create_file_formats >> create_table # ">>" = "runs after"

stg_setup()                        # registers the DAG
```

Resulting graph:

```
create_file_formats ──► create_stg_sales
                    ├─► create_stg_inventory
                    ├─► create_stg_item_master
                    └─► create_stg_store_master
```

| Task | Purpose |
|---|---|
| `create_file_formats` | Creates the CSV reading rules that L3's `COPY` will use. Runs first so every later step can rely on it |
| `create_stg_<source>` ×4 | Creates the empty landing table for that file. The 4 don't depend on each other, so they run in parallel |

The DAG file holds no table, schema or database name. Adding a 5th source = 1 word in `SOURCES` + 1 SQL file.

---

## 2.4 Run it

**1. Generate the files** (PowerShell, project folder):

```powershell
cd C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion
Remove-Item data\landing\*.csv
python scripts\generate_simple.py
python scripts\generate_seed_dim_day.py
```

Expected output:

```
wrote sales_20261110.csv: 5000 rows
wrote inventory_20261110.csv: 1502 rows
wrote item_master_20261110.csv: 101 rows
wrote store_master_20261110.csv: 25 rows
SALES_AMT total:     2844752.96
DISCOUNT_AMT total:  964.45
NET_SALES_AMT total: 2843788.51
member lines: 3975 | non-member lines: 1025
extremes: max SOLD_QTY 10000.000 | min SOLD_QTY 0.005 | max UNIT_PRICE_AMT 2499990.00 | zero-net lines 12 | max ON_HAND_QTY 9999999.000 | zero-stock rows 1
wrote seed_dim_day.csv: 730 rows (2026-01-01 to 2027-12-31)
```

The **totals** are your control numbers for reconciliation. The **extremes** line already exposes
the anomalies (10,000 units, a 2.5-million price). Checking max/min is the cheapest outlier check.

**2. Drop the old Bronze tables** (one time, Snowsight; they are empty):

```sql
USE ROLE MEDALLION_ROLE;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.ITEM_MASTER;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.STORE_MASTER;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.POS_SALES;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.INVENTORY_SNAPSHOT;
```

**3. Import the Variable and run the DAG:**

```powershell
docker compose run --rm airflow-cli airflow variables import /opt/airflow/include/config/variables.json
docker compose run --rm airflow-cli airflow dags test 02_stg_setup 2026-11-10
```

Expected: 5 tasks succeed. In the UI → task `create_stg_sales` → **Rendered Templates**, the SQL shows
`MEDALLION_DB.BRONZE.STG_SALES` (Jinja filled in).

**4. Verify in Snowsight:**

```sql
USE ROLE MEDALLION_ROLE;  USE WAREHOUSE MEDALLION_WH;
SHOW FILE FORMATS IN SCHEMA MEDALLION_DB.BRONZE;   -- 2 rows
SHOW TABLES IN SCHEMA MEDALLION_DB.BRONZE;         -- 4 STG_* tables, rows = 0
DESC TABLE MEDALLION_DB.BRONZE.STG_INVENTORY;      -- 8 VARCHAR, then 5 lineage columns (null? = N)
```

**5. Hard-coding check** (no output = pass):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

---

## 2.5 Points to remember

- **Bronze = copy, don't fix.** All file columns are `VARCHAR`; casting, trimming and checks happen in Silver (L7).
- **Lineage columns** (`_` prefix) answer "which file, which line, which run, which day" for every row.
- **Deterministic data** (`%` instead of `random`): same input, same output. That's what lets you prove a rerun changes nothing.
- **Empty ≠ zero.** An empty field becomes `NULL` (unknown); `0.000` stock is a real value.
- **Outlier ≠ error.** Keep extreme-but-true values; catch wrong values by **rules** with **configurable limits**, never by "looks odd".
- `CREATE OR REPLACE` **deletes data.** Fine for file formats and empty tables. Once loaded, use `ALTER TABLE`.
- **No names in DAGs:** ids in `config.py`, names in the Variable, table names in SQL, secrets in Connections.
- **Never write** `{{ }}` **in a SQL comment.** Jinja renders comments too.

---

## 2.6 Exercises

1. **Same files every time.** Run the generator twice and compare `Get-FileHash data\landing\sales_20261110.csv`. Why does this matter for "a second run changes nothing"?
2. **Replace vs alter.** Which statement adds a column to a *loaded* table without losing rows?
3. **Data types.** Why is `SOLD_QTY` `VARCHAR` in STG but `NUMBER(18,3)` in TMP (not `(18,2)`, not an integer)? Why does `LOCN_ID` (`1001`) stay text? Why is `_INGEST_DT` a `DATE` but `_BATCH_ID` a `VARCHAR`?
4. **Control total.** Confirm 2843788.51 and 12 zero-net lines:
   ```powershell
     $s = Import-Csv data\landing\sales_20261110.csv
     ($s | Measure-Object NET_SALES_AMT -Sum).Sum
     @($s | Where-Object { [decimal]$_.NET_SALES_AMT -eq 0 }).Count
   ```

     In L11: Gold net **204,298.00** + quarantined **2,639,490.51** must equal this.
5. **Outlier or error?** Say which, and which gate acts: (a) `SOLD_QTY = 480` (b) `SOLD_QTY = -2` (c) net 0.00 after a full discount (d) empty `ITEM_ID` (e) `RESERVED_QTY = 30`, `ON_HAND_QTY = 20` (f) `SOLD_QTY = 10000` (g) item 100021 sold at 0.17 (normal 17.50).
6. **Find the anomalies.** Show the 3 biggest lines:
   ```powershell
     $s | Sort-Object { [decimal]$_.NET_SALES_AMT } -Descending | Select-Object -First 3 TRANSACTION_ID, ITEM_ID, SOLD_QTY, UNIT_PRICE_AMT, NET_SALES_AMT
   ```

     Is the top one real? Why would a fixed rule `UNIT_PRICE_AMT > 1000` be wrong? If 1,500-unit bulk buys become normal, what do you change and what do you **not** change?
7. **Read the code.** In `set_sales_line`, what would go wrong if it changed `SOLD_QTY` but did not recompute `NET_SALES_AMT`? Which gate would then catch the line?
8. **Read the diagram.** Which TMP and Gold tables does `STG_STORE_MASTER` feed? Which gate catches an inventory file with a changed column count?

---

## 2.7 Checkpoint

Reply with:

- [x] Generator output (5000 / 1502 / 101 / 25 rows, `NET_SALES_AMT total: 2843788.51`, `extremes:` line, seed 730)
- [x] `02_stg_setup`: 5 tasks green
- [x] `SHOW FILE FORMATS`: 2; `SHOW TABLES`: 4 `STG_*` (old 4 dropped)
- [x] Hard-coding check: no output
- [x] Answers to exercises 2–8

## Troubleshooting

| Symptom | Fix |
|---|---|
| `python` not found | `py scripts\generate_simple.py` |
| `Insufficient privileges ... 'BRONZE'` | Re-run the L1 bootstrap so `MEDALLION_ROLE` owns the schemas |
| `TemplateSyntaxError` | `{{ }}` inside a SQL comment; remove the braces |
| `TemplateNotFound: ddl/bronze/stg_sales.sql` | File missing, or a typo in `SOURCES` |
| DAG not in the UI | Wait ~30 s, or run `airflow dags list-import-errors` |
| `'dict object' has no attribute ...` | Variable not re-imported: run step 3's import command again |
| Old tables still listed | Run step 2 |

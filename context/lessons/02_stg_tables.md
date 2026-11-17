# Lesson 2: Simple data, STG tables and file formats

| | |
|---|---|
| **Goal** | Generate one day of source files (+ calendar seed) and create what receives them: 2 file formats and the 4 **STG** (Bronze) tables from [the diagram](../architecture-Simple%20view.png) |
| **Data** | 10 Nov 2026 only: 5,000 sales lines, 1,502 inventory rows, 101 items, 25 stores. Includes **valid outliers** and 4 **planted anomalies** (solved in L7) |
| **Read first** | THEORY [1 Architecture](../THEORY.md#sec-1-medallion) · [3.4 File formats](../THEORY.md#sec-3-4-file-formats) · [3.7 NULLs](../THEORY.md#sec-3-7-null-handling) · [3.10 Data types](../THEORY.md#sec-3-10-data-types) · [5.6 Outliers vs anomalies](../THEORY.md#sec-5-6-outliers) |
| **Not yet** | No data is loaded. Files reach a stage in L3 and the STG tables in L5 |

> If Ctrl+click opens only the top of THEORY, use Markdown preview (`Ctrl+Shift+V`) and click again.

---

## 2.1 Concepts

### The source drops (first two columns of the diagram)

| File | Rows | Header | One row = | Lands in | Load |
|---|---|---|---|---|---|
| `sales_20261110.csv` | 5,000 | yes | one POS line (+ member details if a member) | `STG_SALES` | delta |
| `inventory_20261110.csv` | 1,502 | **no** | one item at one location: stock + prices | `STG_INVENTORY` | full |
| `item_master_20261110.csv` | 101 | yes | one item + subcategory, category, department | `STG_ITEM_MASTER` | full |
| `store_master_20261110.csv` | 25 | yes | one location + city, region, division | `STG_STORE_MASTER` | full |
| `data/seed/seed_dim_day.csv` | 730 | yes | one calendar day, 2026–2027 | `DIM_DAY` directly (L8) | seed |

- **Inventory has no header** on purpose: the brief's **positional load** (`$1..$8` in file order).
- **The seed skips STG and TMP**: the diagram's one departure (a generated calendar has no source). It is version controlled.
- **Column names** follow the diagram: `_ID` key, `_NM` name, `_DESC`, `_DT` date, `_AMT` money, `_QTY` quantity, `_FLG` flag, `_TXT`, `_CD`, `_NUM`. Headers are upper case, so name-matching in L3 is 1:1.

### What the data contains, and why

| Feature | Where | Why it is there |
|---|---|---|
| Non-members (every 5th transaction, 1,025 lines) | Customer columns empty → `NULL` | `TMP_CUSTOMER` skips them; `FCT_SALES` keeps them |
| Weighed items (D03 Fresh) | `SOLD_QTY` like `0.750` | Why quantities are `NUMBER(18,3)` |
| Promo prices (every 10th item) | `PROMO_PRICE_AMT` filled, else empty | Sale uses the promo price; feeds `FCT_PRICE` later |
| Amount rule (gate G6) | `SALES_AMT = SOLD_QTY × UNIT_PRICE_AMT`; `NET = SALES_AMT − DISCOUNT_AMT`; tax separate; discount never above the sale | Every line passes G6 |
| Stock rule (gate G6b) | `RESERVED_QTY ≤ ON_HAND_QTY` | Every row passes G6b |

### Outliers: extreme but valid

An **outlier** is unusual but correct, so it must pass every gate and reach Gold. An
**anomaly** is wrong but looks like normal data, so it gets quarantined. Gates check business
rules, not "looks odd".

| Outlier | Where | What it tests later |
|---|---|---|
| Premium item `100101` at **2,499.99** | 2 real sales lines (stores 1001, 1013), stocked in 2 stores | Money precision `NUMBER(18,2)`; a new item with very few rows |
| Near-free item `100045` at **0.05** | ~50 lines; discount capped → **12 lines with `NET_SALES_AMT = 0.00`** in total | Zero amounts are valid, not NULL |
| Bulk business buy **480** units | `T20261110-1001-0050` line 3 | No "max quantity" rule drops it |
| 100% discount | `T20261110-1002-0001` line 1 | Net 0 still counts in reconciliation |
| **0.005 kg** weighed sale | `T20261110-1002-0026` line 2 | Smallest `NUMBER(18,3)` step survives the cast |
| Out of stock `0.000` | item 100011 at 1001 | Zero ≠ missing |
| Huge stock **25,000** | item 100021 at 1001 | Gate G8 counts rows, not quantities |
| `RESERVED_QTY = ON_HAND_QTY` | item 100031 at 1001 | Boundary of G6b: must **not** alert |

### Planted anomalies: the problem to solve in L7

Each line below still obeys the amount rule (G6), so the arithmetic gates pass it. Only a
**range** check finds it. Together they add **2,639,490.51** to the file's net sales, so one
decimal slip makes the day look 14 times bigger than it was.

| # | Anomaly | Where | Value vs normal | Real-world cause | G12 reason |
|---|---|---|---|---|---|
| A1 | Price ×1000 | `T20261110-1006-0001` line 1, item 100101 | **2,499,990.00** vs 2,499.99 | Decimal point lost | `PRICE_TOO_HIGH`, `LINE_AMT_TOO_HIGH` |
| A2 | Fat-finger quantity | `T20261110-1011-0001` line 1, item 100016 | **10,000** units (net 139,500.00) | Key held down at the till | `QTY_TOO_HIGH`, `LINE_AMT_TOO_HIGH` |
| A3 | Price ÷100 | `T20261110-1016-0001` line 1, item 100021 | **0.17** vs 17.50 | Cents typed as units | `PRICE_TOO_LOW` |
| A4 | Placeholder stock | inventory: item 100041 at 1001 | **9,999,999.000** on hand | "Unknown" filled with 9s | `ON_HAND_TOO_HIGH` |

**How L7 detects them (gate G12):** the limits live in `medallion_config.outlier_rules`
(`variables.json`), not in code:

| Rule | Limit | Catches | Lets through |
|---|---|---|---|
| Sale price ÷ item's inventory price | `price_ratio_max` 10, `price_ratio_min` 0.1 | A1, A3 | 0.05 item (ratio 1), promo prices (0.9), 2,499.99 item (ratio 1) |
| `SOLD_QTY` | `max_sold_qty` 1,000 | A2 | 480-unit bulk buy |
| `NET_SALES_AMT` per line | `max_line_net_amt` 50,000 | A1, A2 | A real 2,499.99 sale |
| `ON_HAND_QTY` | `max_on_hand_qty` 100,000 | A4 | 25,000 stock |

The ratio rule is the important one: one fixed price limit can't serve both a 0.05 item and a
2,499.99 item, but "more than 10 times *this item's* usual price" works for both.
**Resolution:** quarantine the row (never delete it), alert, the source sends a corrected file,
then reload day D ([THEORY 5.5](../THEORY.md#sec-5-5-retention-reload)).

### STG contract and data types

STG keeps a byte-for-byte copy: **no cast, no filter, no rename**. It does not promise types or
unique keys; that is TMP's job (L7).

| Column(s) | Type | Why |
|---|---|---|
| Every file column | `VARCHAR` (no length) | A bad value (`3.9O`) must still land; TMP casts and quarantines. A length would only add a way to reject a row |
| `_INGEST_DT` | `DATE` | Day D of the file. The diagram's partition: reload = `WHERE _INGEST_DT = D` |
| `_SOURCE_FILE` | `VARCHAR` | Which file the row came from |
| `_SOURCE_ROW_NUM` | `NUMBER` | Line in that file, to trace a quarantined row back |
| `_INGEST_TS` | `TIMESTAMP_NTZ` default `CURRENT_TIMESTAMP()` | When it loaded; no time zone = simplest |
| `_BATCH_ID` | `VARCHAR` | Airflow `run_id`: first load vs rerun |

The `_` prefix marks columns *we* add. Only lineage columns are `NOT NULL`. TMP and Gold use
`VARCHAR(50)` etc. (as on the diagram) because there the length documents a checked contract.

### File formats

| Option | `FF_CSV_HEADER` | `FF_CSV_POSITIONAL` | Why |
|---|---|---|---|
| `SKIP_HEADER` | 1 | 0 | Skip the header, or there is none |
| `FIELD_OPTIONALLY_ENCLOSED_BY` | `'"'` | `'"'` | A comma inside quotes stays one field |
| `EMPTY_FIELD_AS_NULL`, `NULL_IF` | TRUE, `('', 'NULL', 'null')` | same | Empty → `NULL` (non-members, no promo) |
| `TRIM_SPACE` | FALSE | FALSE | STG keeps values exactly; TMP trims |
| `ERROR_ON_COLUMN_COUNT_MISMATCH` | TRUE | TRUE | Layout changed → fail loudly (gate G2) |

**`CREATE OR REPLACE`** is used for the STG tables only while they are empty. Once loaded (L5),
change columns with `ALTER TABLE` and reload a day with `DELETE WHERE _INGEST_DT = ...`.

**No names in the DAG:** SQL says `{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES`.
The DAG only knows `SOURCES` and builds the path `ddl/bronze/stg_<source>.sql`.

---

## 2.2 Files

| Path | Purpose |
|---|---|
| `scripts/generate_simple.py` | The 4 drops, deterministic loops; `add_outliers()` and `add_anomalies()` are labelled steps |
| `scripts/generate_seed_dim_day.py` | `data/seed/seed_dim_day.csv` (2026-01-01 → 2027-12-31) |
| `include/sql/setup/01_file_formats.sql` | `FF_CSV_HEADER`, `FF_CSV_POSITIONAL` in BRONZE |
| `include/sql/ddl/bronze/stg_{sales,inventory,item_master,store_master}.sql` | 4 STG tables: 16 / 8 / 15 / 14 file columns + 5 lineage |
| `include/medallion/config.py` | `SOURCES = ["sales", "inventory", "item_master", "store_master"]` |
| `include/config/variables.json` | Adds `seed_path` and `outlier_rules` (used from L7) |
| `dags/lesson02_stg_setup.py` | `create_file_formats` → 4 × `create_stg_<source>` (a `for` loop) |

**Why this task order:** L3's `COPY` needs the file formats; the 4 tables don't depend on each
other, so they run in parallel.

---

## 2.3 Generate the files

```powershell
cd C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion
Remove-Item data\landing\*.csv
python scripts\generate_simple.py
python scripts\generate_seed_dim_day.py
```

**Observe:**

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

The `extremes:` line already gives the anomalies away: 10,000 units, a 2.5 million price, 9,999,999
in stock. Looking at max/min is the first, cheapest outlier check.

Peek: `Get-Content data\landing\inventory_20261110.csv -TotalCount 3` → no header, lines like
`2026-11-10,100001,1001,20.000,2.000,6.000,2.60,` (last field = empty promo price).

---

## 2.4 Drop the old Bronze tables (one time, Snowsight)

They are empty, so nothing is lost.

```sql
USE ROLE MEDALLION_ROLE;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.ITEM_MASTER;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.STORE_MASTER;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.POS_SALES;
DROP TABLE IF EXISTS MEDALLION_DB.BRONZE.INVENTORY_SNAPSHOT;
```

---

## 2.5 Create the file formats and STG tables

```powershell
docker compose run --rm airflow-cli airflow variables import /opt/airflow/include/config/variables.json
docker compose run --rm airflow-cli airflow dags test lesson02_stg_setup 2026-11-10
```

**Observe:** 5 green tasks: `create_file_formats`, then `create_stg_sales`, `_inventory`,
`_item_master`, `_store_master` side by side. **Rendered Templates** of `create_stg_sales`
shows `MEDALLION_DB.BRONZE.STG_SALES`. The old `lesson02_bronze_setup` vanishes from the UI within ~30 s.

---

## 2.6 Verify

```sql
USE ROLE MEDALLION_ROLE;  USE WAREHOUSE MEDALLION_WH;
SHOW FILE FORMATS IN SCHEMA MEDALLION_DB.BRONZE;      -- 2 rows
SHOW TABLES IN SCHEMA MEDALLION_DB.BRONZE;            -- 4 STG_* tables, rows = 0
DESC TABLE MEDALLION_DB.BRONZE.STG_INVENTORY;         -- 8 VARCHAR in file order, then 5 lineage (null? = N)
```

Hard-coding check (no output = pass):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

---

## 2.7 Exercises

1. **Same files every time.** Run the generator twice; compare `Get-FileHash data\landing\sales_20261110.csv`. Why does that matter for acceptance criterion 1?
2. **Replace vs alter.** Which statement adds a column to a loaded table without losing rows?
3. **Data types.** Why is `SOLD_QTY` `VARCHAR` in STG but `NUMBER(18,3)` in TMP (not `(18,2)`, not an integer)? Why does `LOCN_ID` (`1001`) stay text in Gold? Why is `_INGEST_DT` a `DATE` but `_BATCH_ID` a `VARCHAR`?
4. **Control total.** Confirm 2843788.51 and count the zero-net lines (expect 12):
   ```powershell
   $s = Import-Csv data\landing\sales_20261110.csv
   ($s | Measure-Object NET_SALES_AMT -Sum).Sum
   @($s | Where-Object { [decimal]$_.NET_SALES_AMT -eq 0 }).Count
   ```
   Keep it for L11: Gold net (**204,298.00**) + quarantined net (**2,639,490.51**) must equal it (G11).
5. **Outlier or defect?** For each, say which and which gate would act: (a) `SOLD_QTY = 480`, (b) `SOLD_QTY = -2`, (c) `NET_SALES_AMT = 0.00` with a full discount, (d) `ITEM_ID` empty, (e) `RESERVED_QTY = 30` with `ON_HAND_QTY = 20`, (f) `SOLD_QTY = 10000`, (g) item 100021 sold at 0.17.
6. **Find the anomalies yourself.** List the 3 largest lines by net amount, then answer: is the top one real? Which rule in the G12 table proves it isn't?
   ```powershell
   $s | Sort-Object { [decimal]$_.NET_SALES_AMT } -Descending | Select-Object -First 3 TRANSACTION_ID, ITEM_ID, SOLD_QTY, UNIT_PRICE_AMT, NET_SALES_AMT
   ```
   Then: why would a fixed `UNIT_PRICE_AMT > 1000` rule be wrong? If the business decides a bulk buy of 1,500 is normal, what do you change, and what do you **not** change?
7. **Read the diagram.** Which TMP and Gold tables does `STG_STORE_MASTER` feed? Which gate catches an inventory file with a changed column count?
6. **Read the diagram.** Which TMP and Gold tables does `STG_STORE_MASTER` feed? Which gate catches an inventory file with a changed column count?

---

## 2.8 Checkpoint

Reply with:

- [ ] Generator output (5000 / 1502 / 101 / 25 rows, `NET_SALES_AMT total: 2843788.51`, `extremes:` line, seed 730)
- [ ] `lesson02_stg_setup`: 5 tasks green
- [ ] `SHOW FILE FORMATS`: 2; `SHOW TABLES`: 4 `STG_*` (old 4 dropped)
- [ ] Hard-coding check: no output
- [ ] Answers to exercises 2–7

## Troubleshooting

| Symptom | Fix |
|---|---|
| `python` not found | `py scripts\generate_simple.py` |
| `Insufficient privileges ... 'BRONZE'` | Re-run the L1 bootstrap so `MEDALLION_ROLE` owns the schemas |
| `TemplateSyntaxError` | A `{{ }}` inside a SQL comment; remove the braces |
| `TemplateNotFound: ddl/bronze/stg_sales.sql` | File missing, or `SOURCES` still has old prefixes |
| DAG not in the UI | Wait ~30 s, or `airflow dags list-import-errors` |
| Old tables still listed | Run step 2.4 |

# Lesson 5: Bronze load (idempotent COPY, G2, archive)

|                       |                                                                                                                                                                                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Goal**              | Load day D into the 4 STG tables **safely twice**: wait → PUT (overwrite) → G1/G2 → `DELETE` + `COPY FORCE` → archive landing files                                                                                                                                                              |
| **You build**         | Idempotent load SQL, gate G2 check, archive task, DAG `05_bronze_load`                                                                                                                                                                                                                           |
| **Status**            | **Done** (18 Nov 2026) — idempotent Bronze load + G2 + archive; checkpoint passed                                                                                                                                                                                                               |
| **Not yet**           | Silver TMP / cleaning (L7); five-day complex source files (L6)                                                                                                                                                                                                                                   |
| **Theory (optional)** | [3.4 COPY / load history](../THEORY.md#34-file-formats-and-copy-into) · [3.5 partitions](../THEORY.md#35-micro-partitions-pruning-and-the-diagrams-partitioning) · [6 Idempotency](../THEORY.md#6-idempotency-playbook) · [5.2 Gates](../THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

## 5.1 The idea in one picture

```
landing/*.csv
   │ FileSensor (G1 SLA)
   ▼
PUT OVERWRITE=TRUE → @MY_INT_STAGE/D/
   │ assert LIST (G1 presence)
   │ g2_check_*  (column count / header)     ← transforming COPY cannot do this alone
   ▼
DELETE WHERE _INGEST_DT = D
COPY ... FORCE = TRUE  →  STG_*
   │
   ▼
archive/D/*.csv     (landing cleared; evidence kept)
```

Lesson 3 showed that a plain re-run **skips** COPY (load history) and PUT (unchanged file). That
protects you from accidents, but a **corrected** file for the same day would never reload.
Lesson 5 fixes that with an explicit replace-of-day pattern.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Idempotent load | Run twice for the same date → same counts and totals | `DELETE` day D, then `COPY FORCE` |
| Load history | Snowflake remembers loaded files ~64 days and skips them | Why `FORCE = TRUE` is required after a successful first load |
| `FORCE = TRUE` | Reload even if the file was already loaded | Safe **only** after deleting day D (else duplicates) |
| `OVERWRITE = TRUE` (PUT) | Replace the staged object if the local file changed | Corrected `sales_20261110.csv` actually reaches the stage |
| Gate G2 | Header / field count matches the contract | Python `csv` on the landing file before COPY |
| Archive | Move processed landing files aside | `data/archive/2026-11-10/`; bad/good evidence kept |

---

## 5.2 Why DELETE + FORCE (not FORCE alone)

| Step | If you skip it | Result |
|---|---|---|
| No DELETE, `FORCE = TRUE` | — | **Duplicate** rows for day D |
| DELETE, no FORCE | File already in load history | `0 files processed`, table empty for D |
| DELETE + FORCE | — | Day D replaced; counts match the file |

`_INGEST_DT = D` is the diagram's partition: one day in, one day out.

---

## 5.3 Code walkthrough

### a) `include/sql/stage/put_file.sql` — overwrite is a param

```sql
PUT 'file://.../{{ params.source }}_{{ ds_nodash }}.csv'
    @...MY_INT_STAGE/{{ ds }}/
    AUTO_COMPRESS = TRUE
    OVERWRITE     = {{ params.get('overwrite', 'FALSE') }}
```

| DAG | `overwrite` | Why |
|---|---|---|
| `03_stage_load`, `04_waiting` | `FALSE` | Learning PUT skip behaviour |
| `05_bronze_load` | `TRUE` | Corrected files must replace the staged `.gz` |

### b) `include/sql/load/bronze/stg_*.sql` — two statements

```sql
DELETE FROM ...STG_SALES WHERE _INGEST_DT = '{{ ds }}'::DATE;

COPY INTO ...STG_SALES ( ... )
FROM ( SELECT $1, ..., '{{ ds }}'::DATE, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, '{{ run_id }}'
       FROM @...MY_INT_STAGE/{{ ds }}/ )
FILES = ('sales_{{ ds_nodash }}.csv.gz')
FILE_FORMAT = (...)
ON_ERROR = ABORT_STATEMENT
FORCE = TRUE;
```

`split_statements=True` on the operator runs DELETE, then COPY, in order.

### c) Gate G2 — `g2_check_file` in `dags/05_bronze_load.py`

Contracts live in `include/medallion/config.py` as `SOURCE_CONTRACTS` (16 / 8 / 15 / 14 fields).

```python
# inventory: no header; every row must have 8 fields
# sales / item_master / store_master: header length + every data row length
```

Done in Python on the **landing** file (still present before archive) because a `SELECT $1,$2,…`
COPY **ignores** `ERROR_ON_COLUMN_COUNT_MISMATCH`. G2 is the explicit check promised in L3.

### d) `archive_landing_files`

After all four loads succeed, each `data/landing/<source>_20261110.csv` moves to
`data/archive/2026-11-10/`. The next daily run expects **new** drops in landing; to re-test the
same day, copy from archive back (exercise 2).

### e) Graph

```
create_stage → wait_for_file[0..3] → put_* (OVERWRITE TRUE) → assert_files_on_stage
                                      ↓
                               g2_check_* → load_stg_* (DELETE+FORCE) → archive_landing_files
```

| Task | Purpose |
|---|---|
| `wait_for_file` ×4 | G1 SLA |
| `put_*` | Stage the (possibly corrected) files |
| `assert_files_on_stage` | G1 presence on stage |
| `g2_check_*` | G2 contract on landing CSV |
| `load_stg_*` | Replace day D in STG |
| `archive_landing_files` | Clear landing; keep evidence |

---

## 5.4 Run it

**0. Landing files must exist.** If L4/L5 already archived them:

```powershell
Copy-Item data\archive\2026-11-10\*.csv data\landing\
# or regenerate:
python scripts\generate_simple.py
```

**1. First load**

```powershell
docker compose run --rm airflow-cli airflow dags test 05_bronze_load 2026-11-10
```

Expected: sensors green → puts `UPLOADED` → G1 OK → four `G2 OK` → four loads → files under
`data/archive/2026-11-10/`, landing empty of that day's CSVs.

Snowsight:

```sql
USE ROLE MEDALLION_ROLE; USE WAREHOUSE MEDALLION_WH;
SELECT 'STG_SALES' t, COUNT(*) n FROM MEDALLION_DB.BRONZE.STG_SALES WHERE _INGEST_DT = '2026-11-10'
UNION ALL SELECT 'STG_INVENTORY', COUNT(*) FROM MEDALLION_DB.BRONZE.STG_INVENTORY WHERE _INGEST_DT = '2026-11-10'
UNION ALL SELECT 'STG_ITEM_MASTER', COUNT(*) FROM MEDALLION_DB.BRONZE.STG_ITEM_MASTER WHERE _INGEST_DT = '2026-11-10'
UNION ALL SELECT 'STG_STORE_MASTER', COUNT(*) FROM MEDALLION_DB.BRONZE.STG_STORE_MASTER WHERE _INGEST_DT = '2026-11-10';
-- 5000, 1502, 101, 25

SELECT SUM(NET_SALES_AMT::NUMBER(18,2)) FROM MEDALLION_DB.BRONZE.STG_SALES WHERE _INGEST_DT = '2026-11-10';
-- 2843788.51
```

**2. Idempotent re-run** (same counts)

```powershell
Copy-Item data\archive\2026-11-10\*.csv data\landing\
docker compose run --rm airflow-cli airflow dags test 05_bronze_load 2026-11-10
```

Counts and the net-sales sum must match step 1 (acceptance criterion 1, Bronze slice).

**3. Hard-coding check** (no output = pass):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

---

## 5.5 Points to remember

- **Replace a day, don't append:** `DELETE` by `_INGEST_DT` then `COPY`.
- `FORCE` **without** `DELETE` **duplicates;** `DELETE` **without** `FORCE` **can load nothing.**
- **PUT** `OVERWRITE = TRUE` when the same filename can carry new content.
- **G2 is explicit** because transforming COPY will not enforce column count.
- **Archive after success** so the next run does not pick up yesterday's file by mistake; keep the archive for audit.
- **Partition =** `_INGEST_DT`**:** every Bronze reload filter uses it.

---

## 5.6 Exercises

1. **FORCE alone.** If you removed the `DELETE` but kept `FORCE = TRUE` and ran twice, what would `COUNT(*)` for `_INGEST_DT = '2026-11-10'` become on `STG_SALES`?
2. **Re-run proof.** After the idempotent re-run (section 5.4 step 2), paste the four counts and the net sum. Did `_BATCH_ID` change while the counts stayed the same? Why is that OK?
3. **G2 fail.** Add a trailing comma to one data line in `sales_20261110.csv` (extra empty field), put the file in landing, run until `g2_check_sales`. What error do you get? Restore the file from archive afterward.
4. **Overwrite.** Why did L3 use `OVERWRITE = FALSE` but L5 use `TRUE`? What goes wrong if L5 kept `FALSE` and the source sent a fixed file with the same name?
5. **Archive order.** Why does `archive_landing_files` run **after** all `load_stg_`* tasks, not after PUT?
6. **Diagram.** Which badge on the architecture picture matches `_INGEST_DT` + full/delta load for these four STG tables?

---

## 5.7 Checkpoint

Reply with:

- [x] `05_bronze_load` green once; landing files moved under `data/archive/2026-11-10/`
- [x] Snowsight counts 5000 / 1502 / 101 / 25 and net 2843788.51
- [x] Second run (files copied back): same counts and sum
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6

## Troubleshooting

| Symptom | Fix |
|---|---|
| Sensor timeout | Restore CSVs to `data/landing` from archive or regenerate |
| PUT `SKIPPED` on L5 with a corrected file | Confirm `params.overwrite` is `TRUE` in `05_bronze_load` |
| COPY `0 files processed` after DELETE | `FORCE = TRUE` missing from the load SQL |
| Counts doubled | `DELETE` missing or filtered wrong date |
| G2 fails on inventory "header" | Inventory has **no** header; do not expect column names |
| `get_current_context` / TaskFlow error | Image must be Airflow 3.3.x; restart containers if the DAG did not refresh |
| Archive `missing` file | G2 or load already moved/deleted it; regenerate landing files |

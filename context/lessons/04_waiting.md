# Lesson 4: Waiting for files (FileSensor, Assets, gate G1)

| | |
|---|---|
| **Goal** | Wait for the four landing files with a **timeout** (gate G1), upload them, then **fail the DAG** if the stage does not hold exactly four non-empty files |
| **You build** | 1 DAG with mapped `FileSensor`, reuse of PUT, 1 assert `@task`, gate note SQL |
| **Not yet** | Safe re-runs, archive, G2 header check, full Bronze load DAG (L5) |
| **Theory (optional)** | [2.7 Sensors](../THEORY.md#27-sensors) · [2.8 Assets](../THEORY.md#28-assets-datasets-and-the-sensor-trade-off) · [5.2 Gates](../THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

## 4.1 The idea in one picture

```
data/landing/<source>_20261110.csv
        │  FileSensor x4   (poke every 60 s, give up after 10 min)     ← G1 "arrived by SLA"
        ▼
PUT → @MY_INT_STAGE/2026-11-10/
        │  assert_files_on_stage   (LIST → must be 4 files, each size > 0)  ← G1 "present & non-empty"
        ▼
ready for COPY (Lesson 5)
```

Without a wait, the DAG would PUT/COPY too early and fail on a missing file, or worse, load a
partial day. Gate **G1** is that wait plus a proof the files are really there.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Sensor | A task that **waits** until a condition is true, then succeeds | `FileSensor` |
| `poke` mode | Holds a worker slot and sleeps between checks | Fine for seconds; wasteful for minutes |
| `reschedule` mode | Frees the worker between checks (`up_for_reschedule`) | What we use (60 s interval) |
| `poke_interval` | Seconds between checks | 60 (`SENSOR_POKE_SECONDS`) |
| `timeout` | Total seconds before the sensor fails (`AirflowSensorTimeout`) | 600 (10 min) = the SLA |
| `filepath` | Path relative to the `fs` connection's `path` | `sales_{{ ds_nodash }}.csv` under `/opt/airflow/data/landing` |
| Dynamic Task Mapping | One task definition → N task instances at run time | `FileSensor.partial(...).expand(...)` |
| Asset (Dataset) | A named piece of data; a DAG can schedule when assets update | Taught below; **not** used in this DAG |
| Gate G1 | Files arrive by SLA; then they must be on the stage and non-empty | sensor timeout + `assert_files_on_stage` |

---

## 4.2 FileSensor vs Assets (pick one, learn both)

| | **FileSensor** (we use) | **Assets** |
|---|---|---|
| How it waits | Polls the landing folder | Event: another task emits `outlets=[Asset(...)]` |
| Files produced **outside** Airflow | Yes | Only if something emits an event (watcher / producer DAG) |
| "Files for date D" | Natural (`{{ ds_nodash }}` in the path) | Events are not tied to a logical date by themselves |
| Arrival SLA | Built in (`timeout`) | You must build a separate SLA |
| Cost | Short worker use every poke (cheap in `reschedule`) | Almost free |

**Why FileSensor here:** the CSVs are dropped by a source (or our generator), not by another
Airflow DAG, and each run needs **that day's** files with a clear deadline.

**What an Asset DAG looks like** (do not add this file; read it):

```python
from airflow.sdk import Asset, dag, task

sales_asset = Asset("medallion://landing/sales")

@dag(schedule=[sales_asset])          # run when the asset is updated
def consumer():
    @task
    def load():
        ...
    load()

@dag(schedule="@daily")
def producer():
    @task(outlets=[sales_asset])      # emit the event when done
    def write_sales():
        ...
    write_sales()
```

Use Assets when **Airflow** produces the data. Use FileSensor when the world outside drops files.

---

## 4.3 Code walkthrough

### a) Timeouts live in `include/medallion/config.py`

```python
SENSOR_POKE_SECONDS = 60
SENSOR_TIMEOUT_SECONDS = 10 * 60   # 10 minutes: short so you can watch a timeout while learning
```

Same idea as `SOURCES`: needed when the DAG file is parsed, so they are code, not a Variable.
Production would use hours; we keep 10 minutes so exercise 2 is practical.

### b) Mapped `FileSensor` in `dags/04_waiting.py`

```python
wait_for_file = FileSensor.partial(
    task_id="wait_for_file",
    fs_conn_id=LANDING_FS_CONN_ID,     # → connection fs_landing → path /opt/airflow/data/landing
    poke_interval=SENSOR_POKE_SECONDS,
    timeout=SENSOR_TIMEOUT_SECONDS,
    mode="reschedule",
).expand(filepath=[f"{source}_{{{{ ds_nodash }}}}.csv" for source in SOURCES])
```

| Piece | Meaning |
|---|---|
| `.partial(...)` | Shared arguments for every mapped instance |
| `.expand(filepath=[...])` | Four instances: map indexes 0..3, one file each |
| `sales_{{ ds_nodash }}.csv` | Jinja left in the string on purpose; at run time → `sales_20261110.csv` |
| Relative path | Joined with the connection's `path` → full path inside the container |

UI: one task box `wait_for_file` with **4 mapped instances**. Any one missing file → that instance
keeps rescheduling until it appears or `timeout` fires.

### c) PUT (unchanged from L3)

After all sensors succeed, the four `put_<source>` tasks run (same `stage/put_file.sql`).
If L3 already uploaded today, you will see `SKIPPED` — that is fine; the assert still runs.

### d) `assert_files_on_stage` (gate G1, presence)

```python
@task(task_id="assert_files_on_stage")
def assert_files_on_stage(ds: str) -> list[dict]:
    # LIST + RESULT_SCAN on ONE Snowflake session (LAST_QUERY_ID is per session)
    cur.execute(f"LIST @{db}.{bronze}.MY_INT_STAGE/{ds}/")
    cur.execute('SELECT "name", "size" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY 1')
    rows = cur.fetchall()
    if len(files) != 4: raise ValueError("G1 failed: ...")
    if any size == 0:   raise ValueError("G1 failed: empty ...")
```

| Why | Detail |
|---|---|
| `@task` not SQL-only | Clear failure message; division-by-zero tricks in SQL are harder to read |
| One connection | `RESULT_SCAN(LAST_QUERY_ID())` only sees the LIST from **this** session |
| Count = 4 | Missing upload → fail before any COPY |
| `size > 0` | Zero-byte file "arrived" but is useless (G1 row-count / non-empty half) |

`include/sql/gates/g01_list_stage.sql` documents the SELECT shape for the gates folder.

### e) Graph

```
create_stage ──► wait_for_file [0..3] ──► put_sales ────────┐
                                         put_inventory ─────┼──► assert_files_on_stage
                                         put_item_master ───┤
                                         put_store_master ──┘
```

| Task | Purpose |
|---|---|
| `create_stage` | Stage exists (safe to repeat) |
| `wait_for_file` ×4 | G1 SLA: each landing file must appear within 10 minutes |
| `put_*` ×4 | Upload (or skip if already staged) |
| `assert_files_on_stage` | G1 presence: LIST shows 4 non-empty files |

---

## 4.4 Run it

**1. Happy path** (files already in `data/landing` from L2):

```powershell
docker compose run --rm airflow-cli airflow dags test 04_waiting 2026-11-10
```

Expected:

| Task | You should see |
|---|---|
| `wait_for_file` (4 maps) | Success almost immediately (files already there) |
| `put_*` | `UPLOADED` or `SKIPPED` |
| `assert_files_on_stage` | Log `G1 OK: 4 non-empty files` and four paths under `2026-11-10/` |

**2. Timeout (gate G1 SLA)** — temporary, then restore the file:

```powershell
Move-Item data\landing\sales_20261110.csv data\landing\sales_20261110.csv.bak
docker compose run --rm airflow-cli airflow dags test 04_waiting 2026-11-10
# wait_for_file for sales keeps rescheduling, then fails with AirflowSensorTimeout (~10 min)
Move-Item data\landing\sales_20261110.csv.bak data\landing\sales_20261110.csv
```

To practice faster, temporarily set `SENSOR_TIMEOUT_SECONDS = 60` in `config.py`, re-run, then set it back to `10 * 60`.

**3. Missing on stage** (assert half of G1):

Sensors and PUT need the landing files. To fail only the assert, the easiest demo is to
`REMOVE` the stage folder in Snowsight after a successful PUT in a first run, then run a
**modified** mental check — or skip if short on time and answer exercise 3 from the code.
Optional Snowsight:

```sql
REMOVE @MEDALLION_DB.BRONZE.MY_INT_STAGE/2026-11-10/;
```

Then run the DAG again: sensors pass, PUT re-uploads (`OVERWRITE = FALSE` still uploads if
removed), assert should pass again. To **fail** assert without removing, you'd need a PUT bug;
reading the `ValueError` branches in the task is enough for the exercise.

**4. Hard-coding check** (no output = pass):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

---

## 4.5 Points to remember

- **Sensors wait; operators do.** A sensor's job is only "is it here yet?"
- `reschedule` **for long waits** so you don't hold a worker slot for 10 minutes.
- `timeout` **= SLA.** When it fires, G1 fails (alert comes in L11; for now the task failure is the signal).
- **Mapped sensors** = one definition, four files; same pattern L5 will use for COPY.
- **FileSensor path is relative** to the `fs` connection — no `/opt/airflow/...` in the DAG.
- **Assets ≠ FileSensor.** Assets when Airflow produces data; sensors when the outside world drops files.
- **LIST assert uses one session** so `LAST_QUERY_ID()` still points at that LIST.
- **G1 has two halves:** (1) arrived in time, (2) on the stage and non-empty.

---

## 4.6 Exercises

1. **Modes.** If `wait_for_file` used `mode="poke"` with a 10-minute timeout and 60 s interval, what would happen to the worker slot? Why is `reschedule` better here?
2. **Timeout.** Rename or move `sales_20261110.csv`, run the DAG (optionally with a 60 s timeout), paste the sensor error type/message, then restore the file. What gate is that?
3. **Assert logic.** In `assert_files_on_stage`, why would two separate `SnowflakeHook.get_records(...)` calls (one LIST, one SELECT) be unsafe? What breaks?
4. **Mapping.** How many task instances does `wait_for_file` create? Where in the UI do you see the file name for map index 2?
5. **Assets.** Name one pipeline where Assets would be a better fit than FileSensor, and one reason Assets are a poor fit for *this* landing folder.
6. **G1 completeness.** FileSensor only checks that a path **exists**. Name one bad file that would pass the sensor but fail `assert_files_on_stage`, and one bad file that would pass **both** and must be caught later (which gate?).

---

## 4.7 Checkpoint

Reply with:

- [x] `04_waiting`: all tasks green on the happy path (4 mapped sensors, 4 puts, assert)
- [x] Assert log shows `G1 OK` and 4 staged paths
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6 (exercise 2: paste the timeout error if you ran it)

## Troubleshooting

| Symptom | Fix |
|---|---|
| Sensor succeeds but file is empty | Assert should fail on `size = 0`; if you skipped assert, check LIST `size` |
| `AirflowSensorTimeout` with files present | Wrong date (`ds_nodash`), or `fs_landing` `path` not `/opt/airflow/data/landing` |
| `FileNotFoundError` / connection `fs_landing` missing | Re-import `secrets/connections.json` (L1) |
| Assert: `expected 4 files, found 0` | PUT failed or wrong `ds` folder; `LIST @.../2026-11-10/` in Snowsight |
| Assert: `LAST_QUERY_ID` / unexpected columns | LIST and SELECT must share one connection (see the task) |
| Mapped sensor filepath literally `{{ ds_nodash }}` | Template not rendered; keep the double-brace form in the expand list as in the DAG |
| `Unsupported feature ... snowflake` on PUT | `put_file.sql` must start with `PUT` (no leading `--` comments) |

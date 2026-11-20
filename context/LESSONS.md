# Lessons: Airflow + Snowflake medallion pipeline

Course notes for building `retail_medallion_daily`: land retail CSVs through Bronze (STG), Silver (TMP) and Gold (TGT + Aggregate) in Snowflake, with quality gates, reconciliation and safe re-runs.

Related: [architecture (simple)](architecture_simple.md), [assumptions](ASSUMPTIONS.md), [theory](THEORY.md), [README](../README.md), [evidence pack](../evidence/EVIDENCE.md).

## Lesson index

| # | Lesson |
|---|---|
| 1 | [Lesson 1: Environment, connections and variables](#lesson-01) |
| 2 | [Lesson 2: Simple data, STG tables and file formats](#lesson-02) |
| 3 | [Lesson 3: Stages, PUT, LIST and COPY INTO](#lesson-03) |
| 4 | [Lesson 4: Waiting for files (FileSensor, Assets, gate G1)](#lesson-04) |
| 5 | [Lesson 5: Bronze load (idempotent COPY, G2, archive)](#lesson-05) |
| 6 | [Lesson 6: Complex data (five pipeline days)](#lesson-06) |
| 7 | [Lesson 7: Silver (TMP tables, gates G3–G6 + G12)](#lesson-07) |
| 8 | [Lesson 8: Gold dimensions (SCD2 hierarchies, SCD1 customer, DIM_DAY seed)](#lesson-08) |
| 9 | [Lesson 9: Gold facts (FCT_SALES, soft-close inventory, price periods, G7/G8)](#lesson-09) |
| 10 | [Lesson 10: Prove defects (D1–D5), PK checker, gate G10](#lesson-10) |
| 11 | [Lesson 11: Aggregate (G9), reconciliation (G11), return-flag branch](#lesson-11) |
| 12 | [Lesson 12: Idempotency proof + reload of one bad day](#lesson-12) |
| 13 | [Lesson 13: Five-day backfill evidence](#lesson-13) |
| 14 | [Lesson 14: Single-day DAG + Airflow 3 backfill (Option 1)](#lesson-14) |

---

<a id="lesson-01"></a>

## Lesson 1: Environment, connections and variables

**Goal:** a running Airflow 3.3.2 stack that can log in to Snowflake as a service user, read
project config from a Variable, and pass a test that proves no DAG hard-codes secrets,
paths or table names.

**Data level:** none yet. We prove the plumbing before moving any data.

**Read first in [THEORY.md](THEORY.md):**
[1 Medallion architecture](THEORY.md#1-medallion-architecture-stg--tmp--tgt--aggregate),
[2.1 Components](THEORY.md#21-components),
[2.3 Parse time vs run time](THEORY.md#23-parse-time-vs-run-time),
[2.6 Connections, variables, templating](THEORY.md#26-connections-variables-templating-and-the-fernet-key),
[3.1 Warehouses and cost](THEORY.md#31-architecture-warehouses-and-cost),
[3.2 Access control and key pairs](THEORY.md#32-access-control-service-users-and-key-pair-auth),
[8 Docker essentials](THEORY.md#8-docker-essentials).
Section 1.1 below applies them to this project.

---

### 1.1 Concepts

#### What is running in Docker Compose

| Container | Job |
|---|---|
| `airflow-apiserver` | The UI and REST API on <http://localhost:8080> |
| `airflow-dag-processor` | Parses files in `dags/` every ~30 s and stores the DAG structure. New in Airflow 3: parsing is a separate process |
| `airflow-scheduler` | Decides which task instances should run, and when |
| `airflow-worker` | Celery worker that actually executes tasks |
| `airflow-triggerer` | Runs deferrable operators (waits that don't hold a worker slot) |
| `postgres` | Metadata DB: DAG runs, task states, **connections, variables** |
| `redis` | Queue between scheduler and Celery workers |

The DAG file is read in two different places at two different times:

1. **Parse time** (dag-processor, every ~30 s): top-level code runs. It must be fast and

   must not call databases or Variables.
2. **Run time** (worker, once per task): the task body runs and Jinja templates such as

   `{{ ds }}` and `{{ var.json.medallion_config.database }}` are rendered.

That split is why config goes into templates or task bodies, never module top level.

#### Where configuration lives

| Mechanism | Holds | Example in this project |
|---|---|---|
| **Connection** | How to reach a system, plus its credentials | `snowflake_medallion` (account, user, key file), `fs_landing` (landing folder) |
| **Variable** | Non-secret runtime config | `medallion_config` (database, warehouse, schema names; later stage and table names) |
| **Environment variable** | Process-level settings | `FERNET_KEY`, `PYTHONPATH` |

Reasons the DAG code must not hard-code these:

- The same DAG file runs in dev, test and prod; only connections and variables change.
- Rotating a key or renaming a schema becomes a config change, not a code deploy.
- Nothing secret ends up in Git.
- Step 1.10 gives a one-line PowerShell check that lists any DAG line containing an account URL,
a password, an absolute path or a table name (acceptance criterion 4).

#### Two ways to read config, and when each is right

```python
## Rendered at run time by Jinja, inside a templated field (sql=, bash_command=, ...)
"SELECT * FROM {{ var.json.medallion_config.database }}.BRONZE.STG_SALES"

## Called at run time inside a @task body
cfg = Variable.get("medallion_config", deserialize_json=True)
```

**Anti-pattern:** calling `Variable.get()` at module top level. That runs on every parse
(every ~30 s, forever) and hits the metadata DB each time.

#### The Fernet key

Connection passwords and extras are encrypted in Postgres with the `FERNET_KEY`. If it is
empty, they are stored in **plain text**. Set it **before** creating connections. If you
change it later, existing connections can no longer be decrypted.

#### Why key-pair auth for Snowflake

`AIRFLOW_SVC` is created with `TYPE = SERVICE`, which means:

- It cannot log in with a password or open Snowsight.
- It is exempt from MFA, which matters because an unattended scheduler can't answer a prompt.
- It authenticates by signing a token with a **private key** that only Airflow has.
Snowflake holds only the **public key**.

The private key is a file in `secrets/`, mounted read-only into the containers at
`/opt/airflow/secrets/`. The connection stores only the path to it.

---

### 1.2 Files created for this lesson

| Path | Purpose |
|---|---|
| `docker-compose.yaml` | Copied from `day3-airflow`. Uses the official `apache/airflow:3.3.2` image (it already has the Snowflake provider). Adds the project name, `PYTHONPATH`, and mounts for `include/`, `data/`, `secrets/` (read-only) |
| `.env`, `.env.example` | `AIRFLOW_UID`, `FERNET_KEY`. `.env` is git-ignored |
| `include/medallion/config.py` | Connection ids, Variable name, `SQL_DIR` (computed relative to the file, not hard-coded) |
| `include/config/variables.json` | The `medallion_config` Variable, imported with the CLI |
| `include/config/connections.example.json` | Template for both connections (no secrets inside) |
| `include/sql/setup/00_bootstrap.sql` | Warehouse, database, 4 schemas, role, service user |
| `include/sql/checks/whoami.sql` | Smoke-test query using `{{ var.json.* }}`, `{{ conn.* }}` and `{{ ds }}` |
| `dags/01_connection_check.py` | Two tasks: run `whoami.sql`, then check the 4 schemas exist |

Read `dags/01_connection_check.py` now. Notice that it contains no account, no user,
no database name and no path. Everything arrives through `config.py`, the Variable, or the
connection.

---

### 1.3 Generate the Fernet key

Run in PowerShell from the project folder. This uses Python inside the Airflow image, so
you don't need anything installed locally:

```powershell
cd C:\Users\sammit.poudyal\Desktop\Bootcamp\airflow-medallion
docker run --rm --entrypoint python apache/airflow:3.3.2 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output into `.env`:

```
AIRFLOW_UID=50000
FERNET_KEY=<the 44-character key>
```

**Observe:** the key ends with `=`. Keep it; losing it means re-creating every connection.

---

### 1.4 Create the Snowflake key pair

Git for Windows ships `openssl`. Run in PowerShell from the project folder:

```powershell
$openssl = "$env:LOCALAPPDATA\Programs\Git\usr\bin\openssl.exe"
& $openssl genrsa -out secrets\rsa_key.pem 2048
& $openssl pkcs8 -topk8 -inform PEM -in secrets\rsa_key.pem -out secrets\rsa_key.p8 -nocrypt
& $openssl rsa -in secrets\rsa_key.p8 -pubout -out secrets\rsa_key.pub
Remove-Item secrets\rsa_key.pem

## One-line public key body, ready to paste into Snowflake:
(Get-Content secrets\rsa_key.pub | Where-Object { $_ -notmatch '-----' }) -join ''
```

**Why** `-nocrypt`**:** keeps the lesson simple. In production you would encrypt the private
key with a passphrase and put the passphrase in the connection's password field.

**Observe:** `secrets\` now has `rsa_key.p8` (private, starts with `-----BEGIN PRIVATE KEY-----`)
and `rsa_key.pub`. Both are git-ignored.

---

### 1.5 Bootstrap Snowflake

1. Open Snowsight → **Projects → Worksheets → + SQL Worksheet**.
2. Paste `include/sql/setup/00_bootstrap.sql`.
3. Replace both `<PASTE_PUBLIC_KEY_BODY>` placeholders with the one-line key from 1.4.
4. **Run All** (Ctrl+Shift+Enter).
5. Get your account identifier for the connection:

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
```

**Observe:**

- `SHOW SCHEMAS` lists `AUDIT`, `BRONZE`, `GOLD`, `SILVER` (plus `INFORMATION_SCHEMA`, `PUBLIC`).
- `DESC USER AIRFLOW_SVC` shows `TYPE = SERVICE` and `RSA_PUBLIC_KEY_FP = SHA256:...`.
- Running the script a second time succeeds with no errors. That's your first idempotent
script: everything is `IF NOT EXISTS` or a repeatable `GRANT`.

---

### 1.6 Fill in the connections file

```powershell
Copy-Item include\config\connections.example.json secrets\connections.json
notepad secrets\connections.json
```

Replace `<ORGNAME>-<ACCOUNTNAME>` with the identifier from 1.5 (for example `ABCDEFG-XY12345`).
Use the identifier, **not** the full `...snowflakecomputing.com` URL.

`secrets/connections.json` is git-ignored. It holds an account name and a key **path**,
never the key itself.

---

### 1.7 Start Airflow

The `day3-airflow` stack also uses port 8080, so stop it first if it is running.

```powershell
## Only if day3-airflow is running:
docker compose -f ..\day3-airflow\docker-compose.yaml down

docker compose up airflow-init      # migrates the DB, creates user airflow/airflow; ends with exit 0
docker compose up -d
docker compose ps
```

**Observe:** after 1–2 minutes every service shows `(healthy)`. The UI is at
<http://localhost:8080> (login `airflow` / `airflow`).

---

### 1.8 Import the connections and the Variable

```powershell
docker compose run --rm airflow-cli airflow connections import /opt/airflow/secrets/connections.json
docker compose run --rm airflow-cli airflow variables import /opt/airflow/include/config/variables.json

docker compose run --rm airflow-cli airflow connections get snowflake_medallion -o yaml
docker compose run --rm airflow-cli airflow variables get medallion_config
```

**Observe:** in the UI under **Admin → Connections**, `snowflake_medallion` and `fs_landing`
exist; under **Admin → Variables**, `medallion_config` shows the JSON.

The UI alternative is **Admin → Connections → +**, type **Snowflake**, with the same fields.
The import file is preferred: it is repeatable, reviewable, and nothing secret gets typed
into a shell history.

---

### 1.9 Run the smoke-test DAG

Option A, from the UI: find `01_connection_check`, unpause it, click **Trigger**.

Option B, from the CLI (runs the whole DAG in one process and prints the logs):

```powershell
docker compose run --rm airflow-cli airflow dags test 01_connection_check 2026-11-10
```

**Observe:**

- `whoami` log returns one row:
`AIRFLOW_SVC | MEDALLION_ROLE | MEDALLION_WH | MEDALLION_DB | MEDALLION_WH | 2026-11-10`
- In the UI, open the `whoami` task → **Rendered Templates**. The SQL shows real values in
place of `{{ var.json... }}`, `{{ conn... }}` and `{{ ds }}`. This is the run-time rendering
described in 1.1.
- `check_schemas` log: `All expected schemas exist in MEDALLION_DB: ['AUDIT', 'BRONZE', 'GOLD', 'SILVER']`

---

### 1.10 Two quick checks

Airflow itself tells you if a DAG file is broken:

```powershell
docker compose run --rm airflow-cli airflow dags list-import-errors
```

**Observe:** `No data found` = every DAG file parses. (The UI shows the same errors in a red
banner at the top of the DAG list.)

And one PowerShell line proves no DAG hard-codes secrets, paths or Snowflake names
(acceptance criterion 4):

```powershell
Select-String -Path dags\*.py -CaseSensitive -Pattern 'snowflakecomputing','password','PRIVATE KEY','/opt/airflow','MEDALLION_(DB|WH|ROLE)','(BRONZE|SILVER|GOLD|AUDIT)\.','(STG|TMP|DIM|FCT|AGG|VW)_'
```

**Observe:** no output = pass. Any output is a file name, line number and the offending line.
It is case-sensitive on purpose: lowercase task ids like `create_stg_sales` or `load_dim_item`
are fine; uppercase Snowflake names (`STG_SALES`, `DIM_ITEM`, the table-type prefixes from
[THEORY 1.1](THEORY.md#11-naming-convention)) belong in SQL files or the Variable. Re-run it whenever you add a DAG.

---

### 1.11 Exercises

1. **Grow the config.** Add `"alert_email": "you@example.com"` to `include/config/variables.json`,

   re-import it, and add a column `'{{ var.json.medallion_config.alert_email }}' AS alert_email`
       to `whoami.sql`. Re-run and confirm the value in **Rendered Templates**. No DAG change needed.
2. **Break it on purpose.** Add `"platinum": "PLATINUM"` under `schemas` and re-import.

   Re-run: `check_schemas` should fail with `Missing schemas in MEDALLION_DB: ['PLATINUM']`
       after one retry. Note how long the retry waited, then revert and re-import.
3. **Prove the check works.** Temporarily add the line `# MEDALLION_DB.GOLD.DIM_ITEM` to the
   DAG file and re-run the `Select-String` line from 1.10. Which patterns match, and why does a
   comment still count? Remove the line.
4. **Explain in one sentence:** why is `Variable.get()` fine inside `check_schemas` but wrong

   at the top of the file?

---

### 1.12 Checkpoint

Reply with:

- [x] `docker compose ps`: every service `healthy`
- [x] `whoami` result row (the six values)
- [x] `check_schemas` log line
- [x] Checks in 1.10: `No data found` and no `Select-String` output
- [x] Answers to exercises 3 and 4

### Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `JWT token is invalid` / `250001` | Public key in Snowflake doesn't match `rsa_key.p8`, or the account identifier is wrong (use `ORG-ACCOUNT`, not the URL). Compare `DESC USER AIRFLOW_SVC` fingerprint, re-run the `ALTER USER ... SET RSA_PUBLIC_KEY` line |
| `No such file ... /opt/airflow/secrets/rsa_key.p8` | Key generated in the wrong folder, or containers started before the file existed: `docker compose up -d --force-recreate` |
| `Object does not exist or not authorized` | Role grants missing: re-run the bootstrap script |
| `port is already allocated` on 8080 | `day3-airflow` stack still running (step 1.7) |
| `ModuleNotFoundError: include` | Containers started before `PYTHONPATH` was set: `docker compose up -d --force-recreate` |
| `InvalidToken` when reading a connection | `FERNET_KEY` changed after import: delete the connections and import again |
| `Connection snowflake_medallion not found` | Step 1.8 not run, or run against the other stack |
| Bootstrap: `Object does not exist, or operation cannot be performed` on `GRANT ... TO USER IDENTIFIER($my_user)` | Username contains a dot, `@` or lowercase letters. The script now quotes it: `SET my_user = '"' \|\| CURRENT_USER() \|\| '"';` |
| Bootstrap: same error on `DESC USER AIRFLOW_SVC` | Run as `SECURITYADMIN` (owner of the user); `MEDALLION_ROLE` has no privileges on users |
| Bootstrap: `New public key rejected ... 'Invalid Public key'` | Pasted the `-----BEGIN/END-----` lines or line breaks. Paste only the one-line body printed by the command in 1.4 |
| `TemplateSyntaxError: expected name or number` in `whoami` | `{{ ... }}` inside a SQL comment. Jinja renders comments too; keep braces out of them |
| `syntax error ... unexpected '<value>'` after editing `whoami.sql` | Missing comma between SELECT columns |

---

<a id="lesson-02"></a>

## Lesson 2: Simple data, STG tables and file formats

| | |
|---|---|
| **Goal** | Generate one day of source files, then create the 2 **file formats** and 4 **STG** (Bronze) tables that will receive them ([diagram](../architecture-Simple%20view.png)) |
| **You build** | 2 Python generators, 1 file-format SQL, 4 table SQL files, 1 DAG |
| **Not yet** | No data is loaded. Files reach Snowflake in L3; the tables are filled in L5 |
| **Theory (optional)** | [3.4 File formats](THEORY.md#34-file-formats-and-copy-into) · [3.10 Data types](THEORY.md#310-data-types-and-why-we-pick-them) · [5.6 Outliers](THEORY.md#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |

---

### 2.1 The idea in one picture

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

### 2.2 The data

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

### 2.3 Code walkthrough

#### a) `scripts/generate_simple.py`: makes the 4 files

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

#### b) `scripts/generate_seed_dim_day.py`: the calendar

```python
iso_year, iso_week, iso_weekday = day.isocalendar()   # Monday=1 ... Sunday=7
rows.append([day.strftime("%Y%m%d"), day.isoformat(), ...,
             "Y" if iso_weekday >= 6 else "N",          # WEEKEND_FLG
             "Y" if day in HOLIDAYS else "N"])          # HOLIDAY_FLG
day += timedelta(days=1)
```

`DAY_KY` = `20261110` (the date as a number, which sorts and joins fast). It skips STG/TMP because no
source system sends a calendar. It's version controlled, so it's traceable.

#### c) `include/sql/setup/01_file_formats.sql`: how Snowflake reads CSV

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

#### d) `include/sql/ddl/bronze/stg_*.sql`: the 4 tables

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

#### e) `include/medallion/config.py` + `include/config/variables.json`: where names live

| Kind of value | Lives in | Example |
|---|---|---|
| Ids used by DAG code | `config.py` | `SNOWFLAKE_CONN_ID = "snowflake_medallion"`, `SOURCES = ["sales", "inventory", "item_master", "store_master"]` |
| Database / schema names, paths, limits | Variable `medallion_config` (`variables.json`) | `"database": "MEDALLION_DB"`, `"schemas": {"bronze": "BRONZE"}` |
| Table names | SQL files | `STG_INVENTORY` |
| Credentials | Airflow Connection | key file path, account |

`{{ var.json.medallion_config.schemas.bronze }}` reads the Variable as JSON and takes `schemas` → `bronze` = `BRONZE`.

#### f) `dags/02_stg_setup.py`: the DAG

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

### 2.4 Run it

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

### 2.5 Points to remember

- **Bronze = copy, don't fix.** All file columns are `VARCHAR`; casting, trimming and checks happen in Silver (L7).
- **Lineage columns** (`_` prefix) answer "which file, which line, which run, which day" for every row.
- **Deterministic data** (`%` instead of `random`): same input, same output. That's what lets you prove a rerun changes nothing.
- **Empty ≠ zero.** An empty field becomes `NULL` (unknown); `0.000` stock is a real value.
- **Outlier ≠ error.** Keep extreme-but-true values; catch wrong values by **rules** with **configurable limits**, never by "looks odd".
- `CREATE OR REPLACE` **deletes data.** Fine for file formats and empty tables. Once loaded, use `ALTER TABLE`.
- **No names in DAGs:** ids in `config.py`, names in the Variable, table names in SQL, secrets in Connections.
- **Never write** `{{ }}` **in a SQL comment.** Jinja renders comments too.

---

### 2.6 Exercises

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

### 2.7 Checkpoint

Reply with:

- [x] Generator output (5000 / 1502 / 101 / 25 rows, `NET_SALES_AMT total: 2843788.51`, `extremes:` line, seed 730)
- [x] `02_stg_setup`: 5 tasks green
- [x] `SHOW FILE FORMATS`: 2; `SHOW TABLES`: 4 `STG_*` (old 4 dropped)
- [x] Hard-coding check: no output
- [x] Answers to exercises 2–8

### Troubleshooting

| Symptom | Fix |
|---|---|
| `python` not found | `py scripts\generate_simple.py` |
| `Insufficient privileges ... 'BRONZE'` | Re-run the L1 bootstrap so `MEDALLION_ROLE` owns the schemas |
| `TemplateSyntaxError` | `{{ }}` inside a SQL comment; remove the braces |
| `TemplateNotFound: ddl/bronze/stg_sales.sql` | File missing, or a typo in `SOURCES` |
| DAG not in the UI | Wait ~30 s, or run `airflow dags list-import-errors` |
| `'dict object' has no attribute ...` | Variable not re-imported: run step 3's import command again |
| Old tables still listed | Run step 2 |

---

<a id="lesson-03"></a>

## Lesson 3: Stages, PUT, LIST and COPY INTO

| | |
|---|---|
| **Goal** | Move the 10 Nov files from your laptop into Snowflake: upload them to an **internal stage**, list them, and **COPY** them into the 4 STG tables |
| **You build** | 1 stage SQL, 1 PUT SQL, 1 LIST SQL, 4 COPY SQL files, 1 DAG |
| **Result** | `STG_SALES` 5,000 rows · `STG_INVENTORY` 1,502 · `STG_ITEM_MASTER` 101 · `STG_STORE_MASTER` 25, each row with lineage |
| **Not yet** | Waiting for files (L4), safe re-runs and the G2 check (L5) |
| **Theory (optional)** | [3.3 Stages](THEORY.md#33-stages) · [3.4 File formats and COPY INTO](THEORY.md#34-file-formats-and-copy-into) |

---

### 3.1 The idea in one picture

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

### 3.2 Two ways to map file columns

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

### 3.3 Code walkthrough

#### a) `include/sql/setup/02_stages.sql`: the internal stage

```sql
CREATE STAGE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE
    COMMENT = 'Internal stage: daily source files under YYYY-MM-DD/ folders, gzip-compressed by PUT';
```

- `IF NOT EXISTS`**, not** `OR REPLACE`**:** replacing a stage **deletes every file in it**. File formats could use `OR REPLACE` (no data); stages and tables can't.
- No file format attached: each COPY names its own (`FF_CSV_HEADER` or `FF_CSV_POSITIONAL`), so you can see it in the SQL.

#### b) `include/sql/stage/put_file.sql`: upload one file

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

#### c) `include/sql/checks/list_stage.sql`: what's on the stage

```sql
LIST @{{ ... }}.MY_INT_STAGE/{{ ds }}/;
```

Returns one row per file: `name` (`my_int_stage/2026-11-10/sales_20261110.csv.gz`), `size`,
`md5`, `last_modified`. Here you just read it. In L4 it becomes a check: "4 files, or fail".

#### d) `include/sql/load/bronze/stg_<source>.sql`: COPY into STG (4 files, same shape)

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

#### e) `dags/03_stage_load.py`: the DAG

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

### 3.4 Run it

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

### 3.5 Points to remember

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

### 3.6 Exercises

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
5. **Positional risk.** Suppose the inventory source swaps `ON_HAND_QTY` and `RESERVED_QTY`. Does the COPY fail? Which gate from the [THEORY 5.2 table](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) would notice? Which rows would slip through unnoticed?
6. **Why the fan-in?** What would go wrong if `copy_sales` depended only on `put_sales` (no `list_stage` in between)? What does `list_stage` give you that a single PUT doesn't?

---

### 3.7 Checkpoint

Reply with:

- [x] `03_stage_load`: 10 tasks green
- [x] `put_sales` XCom: `UPLOADED`, `GZIP`
- [x] `LIST`: 4 `.csv.gz` files under `2026-11-10/`
- [x] Row counts 5000 / 1502 / 101 / 25; `SUM(NET_SALES_AMT)` = 2843788.51; 1025 NULL customers
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6 (exercise 2: paste the re-run's PUT status and COPY result)

### Troubleshooting

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

---

<a id="lesson-04"></a>

## Lesson 4: Waiting for files (FileSensor, Assets, gate G1)

| | |
|---|---|
| **Goal** | Wait for the four landing files with a **timeout** (gate G1), upload them, then **fail the DAG** if the stage does not hold exactly four non-empty files |
| **You build** | 1 DAG with mapped `FileSensor`, reuse of PUT, 1 assert `@task`, gate note SQL |
| **Not yet** | Safe re-runs, archive, G2 header check, full Bronze load DAG (L5) |
| **Theory (optional)** | [2.7 Sensors](THEORY.md#27-sensors) · [2.8 Assets](THEORY.md#28-assets-datasets-and-the-sensor-trade-off) · [5.2 Gates](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

### 4.1 The idea in one picture

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

### 4.2 FileSensor vs Assets (pick one, learn both)

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

### 4.3 Code walkthrough

#### a) Timeouts live in `include/medallion/config.py`

```python
SENSOR_POKE_SECONDS = 60
SENSOR_TIMEOUT_SECONDS = 10 * 60   # 10 minutes: short so you can watch a timeout while learning
```

Same idea as `SOURCES`: needed when the DAG file is parsed, so they are code, not a Variable.
Production would use hours; we keep 10 minutes so exercise 2 is practical.

#### b) Mapped `FileSensor` in `dags/04_waiting.py`

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

#### c) PUT (unchanged from L3)

After all sensors succeed, the four `put_<source>` tasks run (same `stage/put_file.sql`).
If L3 already uploaded today, you will see `SKIPPED` — that is fine; the assert still runs.

#### d) `assert_files_on_stage` (gate G1, presence)

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

#### e) Graph

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

### 4.4 Run it

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
## wait_for_file for sales keeps rescheduling, then fails with AirflowSensorTimeout (~10 min)
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

### 4.5 Points to remember

- **Sensors wait; operators do.** A sensor's job is only "is it here yet?"
- `reschedule` **for long waits** so you don't hold a worker slot for 10 minutes.
- `timeout` **= SLA.** When it fires, G1 fails (alert comes in L11; for now the task failure is the signal).
- **Mapped sensors** = one definition, four files; same pattern L5 will use for COPY.
- **FileSensor path is relative** to the `fs` connection — no `/opt/airflow/...` in the DAG.
- **Assets ≠ FileSensor.** Assets when Airflow produces data; sensors when the outside world drops files.
- **LIST assert uses one session** so `LAST_QUERY_ID()` still points at that LIST.
- **G1 has two halves:** (1) arrived in time, (2) on the stage and non-empty.

---

### 4.6 Exercises

1. **Modes.** If `wait_for_file` used `mode="poke"` with a 10-minute timeout and 60 s interval, what would happen to the worker slot? Why is `reschedule` better here?
2. **Timeout.** Rename or move `sales_20261110.csv`, run the DAG (optionally with a 60 s timeout), paste the sensor error type/message, then restore the file. What gate is that?
3. **Assert logic.** In `assert_files_on_stage`, why would two separate `SnowflakeHook.get_records(...)` calls (one LIST, one SELECT) be unsafe? What breaks?
4. **Mapping.** How many task instances does `wait_for_file` create? Where in the UI do you see the file name for map index 2?
5. **Assets.** Name one pipeline where Assets would be a better fit than FileSensor, and one reason Assets are a poor fit for *this* landing folder.
6. **G1 completeness.** FileSensor only checks that a path **exists**. Name one bad file that would pass the sensor but fail `assert_files_on_stage`, and one bad file that would pass **both** and must be caught later (which gate?).

---

### 4.7 Checkpoint

Reply with:

- [x] `04_waiting`: all tasks green on the happy path (4 mapped sensors, 4 puts, assert)
- [x] Assert log shows `G1 OK` and 4 staged paths
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6 (exercise 2: paste the timeout error if you ran it)

### Troubleshooting

| Symptom | Fix |
|---|---|
| Sensor succeeds but file is empty | Assert should fail on `size = 0`; if you skipped assert, check LIST `size` |
| `AirflowSensorTimeout` with files present | Wrong date (`ds_nodash`), or `fs_landing` `path` not `/opt/airflow/data/landing` |
| `FileNotFoundError` / connection `fs_landing` missing | Re-import `secrets/connections.json` (L1) |
| Assert: `expected 4 files, found 0` | PUT failed or wrong `ds` folder; `LIST @.../2026-11-10/` in Snowsight |
| Assert: `LAST_QUERY_ID` / unexpected columns | LIST and SELECT must share one connection (see the task) |
| Mapped sensor filepath literally `{{ ds_nodash }}` | Template not rendered; keep the double-brace form in the expand list as in the DAG |
| `Unsupported feature ... snowflake` on PUT | `put_file.sql` must start with `PUT` (no leading `--` comments) |

---

<a id="lesson-05"></a>

## Lesson 5: Bronze load (idempotent COPY, G2, archive)

|                       |                                                                                                                                                                                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Goal**              | Load day D into the 4 STG tables **safely twice**: wait → PUT (overwrite) → G1/G2 → `DELETE` + `COPY FORCE` → archive landing files                                                                                                                                                              |
| **You build**         | Idempotent load SQL, gate G2 check, archive task, DAG `05_bronze_load`                                                                                                                                                                                                                           |
| **Status**            | **Done** (18 Nov 2026) — idempotent Bronze load + G2 + archive; checkpoint passed                                                                                                                                                                                                               |
| **Not yet**           | Silver TMP / cleaning (L7); five-day complex source files (L6)                                                                                                                                                                                                                                   |
| **Theory (optional)** | [3.4 COPY / load history](THEORY.md#34-file-formats-and-copy-into) · [3.5 partitions](THEORY.md#35-micro-partitions-pruning-and-the-diagrams-partitioning) · [6 Idempotency](THEORY.md#6-idempotency-playbook) · [5.2 Gates](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

### 5.1 The idea in one picture

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

### 5.2 Why DELETE + FORCE (not FORCE alone)

| Step | If you skip it | Result |
|---|---|---|
| No DELETE, `FORCE = TRUE` | — | **Duplicate** rows for day D |
| DELETE, no FORCE | File already in load history | `0 files processed`, table empty for D |
| DELETE + FORCE | — | Day D replaced; counts match the file |

`_INGEST_DT = D` is the diagram's partition: one day in, one day out.

---

### 5.3 Code walkthrough

#### a) `include/sql/stage/put_file.sql` — overwrite is a param

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

#### b) `include/sql/load/bronze/stg_*.sql` — two statements

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

#### c) Gate G2 — `g2_check_file` in `dags/05_bronze_load.py`

Contracts live in `include/medallion/config.py` as `SOURCE_CONTRACTS` (16 / 8 / 15 / 14 fields).

```python
## inventory: no header; every row must have 8 fields
## sales / item_master / store_master: header length + every data row length
```

Done in Python on the **landing** file (still present before archive) because a `SELECT $1,$2,…`
COPY **ignores** `ERROR_ON_COLUMN_COUNT_MISMATCH`. G2 is the explicit check promised in L3.

#### d) `archive_landing_files`

After all four loads succeed, each `data/landing/<source>_20261110.csv` moves to
`data/archive/2026-11-10/`. The next daily run expects **new** drops in landing; to re-test the
same day, copy from archive back (exercise 2).

#### e) Graph

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

### 5.4 Run it

**0. Landing files must exist.** If L4/L5 already archived them:

```powershell
Copy-Item data\archive\2026-11-10\*.csv data\landing\
## or regenerate:
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

### 5.5 Points to remember

- **Replace a day, don't append:** `DELETE` by `_INGEST_DT` then `COPY`.
- `FORCE` **without** `DELETE` **duplicates;** `DELETE` **without** `FORCE` **can load nothing.**
- **PUT** `OVERWRITE = TRUE` when the same filename can carry new content.
- **G2 is explicit** because transforming COPY will not enforce column count.
- **Archive after success** so the next run does not pick up yesterday's file by mistake; keep the archive for audit.
- **Partition =** `_INGEST_DT`**:** every Bronze reload filter uses it.

---

### 5.6 Exercises

1. **FORCE alone.** If you removed the `DELETE` but kept `FORCE = TRUE` and ran twice, what would `COUNT(*)` for `_INGEST_DT = '2026-11-10'` become on `STG_SALES`?
2. **Re-run proof.** After the idempotent re-run (section 5.4 step 2), paste the four counts and the net sum. Did `_BATCH_ID` change while the counts stayed the same? Why is that OK?
3. **G2 fail.** Add a trailing comma to one data line in `sales_20261110.csv` (extra empty field), put the file in landing, run until `g2_check_sales`. What error do you get? Restore the file from archive afterward.
4. **Overwrite.** Why did L3 use `OVERWRITE = FALSE` but L5 use `TRUE`? What goes wrong if L5 kept `FALSE` and the source sent a fixed file with the same name?
5. **Archive order.** Why does `archive_landing_files` run **after** all `load_stg_`* tasks, not after PUT?
6. **Diagram.** Which badge on the architecture picture matches `_INGEST_DT` + full/delta load for these four STG tables?

---

### 5.7 Checkpoint

Reply with:

- [x] `05_bronze_load` green once; landing files moved under `data/archive/2026-11-10/`
- [x] Snowsight counts 5000 / 1502 / 101 / 25 and net 2843788.51
- [x] Second run (files copied back): same counts and sum
- [x] Hard-coding check: no output
- [x] Answers to exercises 1–6

### Troubleshooting

| Symptom | Fix |
|---|---|
| Sensor timeout | Restore CSVs to `data/landing` from archive or regenerate |
| PUT `SKIPPED` on L5 with a corrected file | Confirm `params.overwrite` is `TRUE` in `05_bronze_load` |
| COPY `0 files processed` after DELETE | `FORCE = TRUE` missing from the load SQL |
| Counts doubled | `DELETE` missing or filtered wrong date |
| G2 fails on inventory "header" | Inventory has **no** header; do not expect column names |
| `get_current_context` / TaskFlow error | Image must be Airflow 3.3.x; restart containers if the DAG did not refresh |
| Archive `missing` file | G2 or load already moved/deleted it; regenerate landing files |

---

<a id="lesson-06"></a>

## Lesson 6: Complex data (five pipeline days)

| | |
|---|---|
| **Goal** | Generate the **five pipeline days** with hierarchies, stocked-only sales, and the **five planted defects** from the brief |
| **You build** | `scripts/generate_complex.py` (no new DAG — files only) |
| **Status** | **Done** (19 Nov 2026) |
| **Not yet** | Silver/Gold gates that *prove* those defects (L7–L10); optional long history (deferred) |
| **Theory (optional)** | [4.1 Grain, facts, dimensions](THEORY.md#41-grain-facts-and-dimensions) · [4.3 Hierarchies](THEORY.md#43-hierarchies) |

---

### 6.1 The idea in one picture

```
generate_complex.py
        │
        └─► data/landing/*_YYYYMMDD.csv     10, 11, 12, 13, 14 Nov 2026
                 ~810 sales lines/day (400 txns; stores vary 12/14/16/18/20)
                 sales ITEM_ID ∈ that store’s inventory set
                 └── these five days run through medallion_daily (Bronze→…→Gold)
```

Lesson 2 gave you **one simple day**. Lesson 6 gives you **five complex days** with
hierarchies, returning members, and day-to-day stock/price movement — without a 300-day
history dump. A long history folder is **deferred**; revisit only if Gold/statistical G12
truly needs it.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Pipeline days | Days the daily DAG will process | 10, 11, 12, 13, 14 Nov 2026 |
| Grain | What one fact row means | Sales line = one till line; inventory = one item@store snapshot |
| Hierarchy (flattened file) | Parent attributes copied onto each child row | `item_master` and `store_master` CSVs |
| Hierarchy (snowflaked Gold) | One table per level, child holds parent key | Built in L8 from those flat files |
| Stocked set | Items a store carries on the snapshot | 60 items/store (premium forced at 1001 & 1013) |
| Returning customer | Same `CUSTOMER_ID` appears on many days | Fixed member pool `M000001`…`M000500` |

---

### 6.2 What the generator writes

| Path | Days | Sales density | Notes |
|---|---|---|---|
| `data/landing/` | 5 consecutive days | **~810 lines/day** (400 txns; per-store **12/14/16/18/20**) | Same names/sensors as L4–L5 |
| Outliers + anomalies | **2026-11-10 only** | same teaching cases as L2 | G12 still applies in L7 |
| `data/history/` | — | — | **Not written** (deferred) |

Same four file contracts as L2 (`sales` / `inventory` / `item_master` / `store_master`), same headers,
inventory still **positional** (no header).

**Product hierarchy in every** `item_master` **row:**  
`ITEM → SUBCATEGORY → CATEGORY → DEPARTMENT`.

**Location hierarchy in every** `store_master` **row:**  
`LOCN → CITY → REGION → DIVISION`.

**Sales vs stock:** clean lines pick `ITEM_ID` only from that store’s stocked list. The **one**
orphan sale (`199999`) is planted on purpose for G7 — not accidental noise.

#### Planted defects (brief)

| # | What | File / key | Later gate |
|---|---|---|---|
| D1 | Two null `ITEM_ID` | 11 Nov: `T20261111-1001-0003` L1, `T20261111-1001-0004` L1 | G3 |
| D2 | Negative `SOLD_QTY` | 11 Nov: `T20261111-1001-0005` L3 | G6 |
| D3 | Item missing from master | 12 Nov: `T20261112-1002-0003` L1 → `199999` | G7 |
| D4 | Duplicate txn line | 11 Nov: `T20261111-1001-0008` L2 twice (**811** rows) | G4 |
| D5 | Store city (“district”) change | From 13 Nov: `LOCN_ID=1008` Spokane→Portland | SCD2 `DIM_LOCN` |

Full copy also in [`ASSUMPTIONS.md`](ASSUMPTIONS.md).

**What still changes across the five days:**

| Signal | Behaviour |
|---|---|
| Prices | November lift; sparse promos |
| Stock | Sold qty drains on-hand; replenish with ~12% day-over-day cap (G8 prep) |
| Customers | Same ids return; every 5th txn is a non-member |
| Item desc | Light `ITEM_DESC` tweak on 14 Nov (SCD1 practice later) |

`generate_simple.py` stays as the L2 artifact. From here on, **use** `generate_complex.py` for the five days.

---

### 6.3 Code walkthrough (`scripts/generate_complex.py`)

Open the file top → bottom once. There is **no DAG** in this lesson — only this script.

#### a) Constants at the top — what they control

| Name | Value | What it does |
|---|---|---|
| `PIPELINE_DATES` | 10, 11, 12, 13, 14 Nov 2026 | The only days written to landing |
| `TXNS_PATTERN` | `(12, 14, 16, 18, 20)` | Cycles across stores → **400 txns/day** total, uneven traffic |
| `txns_for_store(i)` | `TXNS_PATTERN[i % 5]` | Store 1001=12, 1002=14, … 1005=20, then repeats |
| `STOCKED_PER_STORE` | `60` | How many items each store carries (inventory + sales pool) |
| `TAX_RATE` | `0.08` | `TAX_AMT = NET × 0.08` (same as L2) |
| `ORPHAN_ITEM_ID` | `"199999"` | Sold on 12 Nov; **never** in `item_master` (D3 / G7) |
| `SCD2_LOCN_ID` | `"1008"` | Store that changes city from 13 Nov (D5) |
| `DEPARTMENTS` / `CITIES` | nested lists | Product and location hierarchies (same shape as L2) |
| `*_HEADER` | column name lists | Written as the first CSV line (inventory uses `header=None`) |

`LANDING_DIR` is resolved from the script path (`…/data/landing`), so it works from any working directory.

#### b) Flow of `main()` — how a run works

```python
items, base_prices = build_items(100)          # 100 items + premium 100101; base shelf prices
stores = build_stores()                        # 25 stores, one per city
stocked_by_store = build_stocked_by_store(...) # locn → list of 60 ITEM_IDs (+ premium at 1001/1013)
stock = init_stock(...)                        # starting ON_HAND per (item, store)

for d in PIPELINE_DATES:                       # five consecutive calendar days
    prices = prices_for_day(base_prices, d)    # that day's shelf + promo
    sales = build_sales(...)                   # ~810 lines; per-store txn counts vary; stocked items only
    inventory = build_inventory(...)           # one row per stocked (item, store)
    add_pipeline_outliers_and_anomalies(...)   # Nov 10 only: valid extremes + G12 anomalies
    evolve_stock(...)                          # drain sold qty, replenish, ±12% cap
    plant_defects(d, sales)                    # D1–D4 on sales (11/12 Nov)
    unexpected_unstocked_count(...)            # fail if accidental orphan slipped in
    stores_out = store_master_for_day(...)     # D5 city move from 13 Nov
    write_csv(... x4)                          # sales, inventory, item_master, store_master
```

Order matters: **evolve stock before planting defects**, so a null `ITEM_ID`, negative qty, or duplicate does not scramble the inventory time series used later for G8.

#### c) Catalog and stocked set

`build_items(100)` walks `DEPARTMENTS` and builds flat rows that already carry  
`ITEM → SUBCATEGORY → CATEGORY → DEPARTMENT` on every line (plus `ITEM_TYPE`).  
It also returns `base_prices[item_id] = (unit_price, item_type, item_num)`.  
Premium `100101` and near-free `100045` (0.05) are added for outlier teaching.

`build_stores()` turns each `CITIES` entry into one `LOCN` with  
`LOCN → CITY → REGION → DIVISION` flattened onto the row (`LOCN_ID` 1001…1025).

`stocked_item_ids(items, store_index)` — the important fix vs the first draft:

```python
start = (store_index * 7) % len(items)
ids = [items[(start + k) % len(items)][0] for k in range(60)]
## premium forced onto stores 1001 and 1013 if missing
```

Each store gets a **rotating window of 60 items**. Sales and inventory both use this list, so a clean sale cannot invent an item the store does not stock.

`build_stocked_by_store` builds `{ "1001": [item_ids…], "1002": […], … }` once; every day reuses it.

#### d) Prices and sales

`prices_for_day(base_prices, d)` copies base prices into that day’s shelf price:

- November ×1.02 list-price lift
- promo on every 10th item (10% off), extra Monday promos, extra Nov promos on some items  
Returns `{ item_id: (shelf, promo_or_None, item_type) }`.

`customer(n)` builds member columns for `M00000n` (name, type, email, phone).  
Same `n` → same member every day → **returning customers**.

`build_sales(d, stores, prices, stocked_by_store)`:

| Step | Code idea | Why |
|---|---|---|
| Loop stores × `txns_for_store(s_i)` | 12/14/16/18/20 cycle (sum 400) | Uneven stores, same day total (~810 lines) |
| `txn_id = f"T{date}-{locn}-{txn:04d}"` | e.g. `T20261110-1001-0003` | Unique, readable |
| Every 5th txn → empty member cols | non-member | Becomes `NULL` in Snowflake |
| `item_id = stocked[(…) % len(stocked)]` | pick from **this store only** | No accidental inventory orphans |
| PERISHABLE → decimal kg qty | `0.50`… | Needs `NUMBER(18,3)` later |
| `SALES = qty×price`, `NET = SALES−discount`, `TAX = NET×0.08` | same maths as L2 | Recon can tie |

**Sales row positions** (0-based, same as L2):  
`[0] TRANSACTION_ID … [4] ITEM_ID … [10] SOLD_QTY [11] UNIT_PRICE [12] DISCOUNT [13] SALES [14] TAX [15] NET`.

#### e) Inventory across days

`init_stock` — dict keyed by `(ITEM_ID, LOCN_ID)` with starting on-hand; premium fixed at 4.0 in 1001 and 1013.

`build_inventory` — for each stocked pair, write the 8 positional columns  
(`SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND, RESERVED, IN_TRANSIT, UNIT_PRICE, PROMO`).  
No header. Promo empty string → `NULL` after COPY.

`evolve_stock(stock, sales_rows, d)` — after each day:

1. Sum sold qty per `(item, locn)` from that day’s sales
2. `on_hand − sold`, then add a small replenishment
3. Clamp the new value to roughly **88%–112%** of yesterday (prep for gate G8 ±20%)

`stable_mix(item, locn, day)` — deterministic stand-in for Python’s `hash()` (which is salted per process and would make two runs differ).

`item_master_for_day` — usually a copy of the catalog; on **14 Nov** appends `(rev Nov)` to one `ITEM_DESC` (SCD1 practice later).

#### f) Helpers that edit rows

`set_sales_line(row, qty=…, price=…, item_id=…, discount=…)`  
Changes fields and **recomputes** SALES / TAX / NET so arithmetic stays consistent.  
Uses `if item_id is not None` so you can set `ITEM_ID` to `""` for D1.  
Allows negative qty (D2) without clamping discount against a negative sales amount.

`first_row_at(sales, locn_id, skip=0)` — finds the n-th sales line for a store. Used so premium / anomaly edits stay on stores that actually stock `100101`.

`write_csv(path, header, rows)` — `csv.writer` with `lineterminator="\n"` (same bytes on Windows/Mac). Inventory passes `header=None`.

#### g) Outliers, anomalies, defects

`add_pipeline_outliers_and_anomalies` — runs **only if** `d == 2026-11-10`:

| Kind | Examples | Purpose |
|---|---|---|
| Valid outliers | premium 2499.99, qty 480, 100% discount, 0.005 kg, stock 0 / 25000 | Must reach Gold |
| Anomalies (G12) | price ×1000, qty 10_000, price ÷100, on-hand 9_999_999 | Quarantine in L7 |

`plant_defects(d, sales)` — mutates / appends sales for the brief defects:

| Day | Code does | Defect |
|---|---|---|
| 11 Nov | `sales[10][4] = ""`, `sales[11][4] = ""` | D1 null `ITEM_ID` ×2 |
| 11 Nov | `set_sales_line(sales[20], qty=-2)` | D2 negative qty |
| 11 Nov | `dup = sales[30][:]; …; sales.append(dup)` | D4 duplicate keys, 801st row |
| 12 Nov | `set_sales_line(sales[40], item_id="199999", …)` | D3 orphan item |

`store_master_for_day(stores, d)` — from **13 Nov** onward, for `LOCN_ID=1008`, overwrites city/region fields (Spokane → Portland). That is D5; Gold will SCD2 `DIM_LOCN` (brief “district change” = `CITY_ID` move).

`unexpected_unstocked_count` — after defects, counts sales whose `ITEM_ID` is not in that store’s stocked list, **ignoring** `""` and `199999`. If anything else appears → `RuntimeError` (guards against regenerating the accidental-orphan bug).

#### h) What you are *not* running

- No Airflow DAG, no Snowflake SQL in this lesson.  
- `generate_simple.py` is unchanged (L2 artifact). Prefer `generate_complex.py` for the five days going forward.  
- No `data/history/` writes (deferred).

---

### 6.4 Run it

```powershell
python scripts\generate_complex.py
```

Expected shape:

```
pipeline landing (no history generated):
  20261110: sales=810 inventory=1501 NET_SALES_AMT=2819814.46
  20261111: sales=811 inventory=1501 NET_SALES_AMT=81601.46    ← +1 from D4 duplicate
  20261112: sales=810 ...
  ...
planted defects:
  D1 null ITEM_ID on T20261111-1001-0003 L1 and T20261111-1001-0004 L1
  D2 negative qty on T20261111-1001-0005 L3
  D4 duplicate line T20261111-1001-0008 L2
  D3 orphan ITEM_ID 199999 on T20261112-1002-0003 L1
  D5 SCD2: 1008 CITY_ID Spokane→Portland from 2026-11-13 onward
```

Txn counts by store (same every day): `1001=12, 1002=14, 1003=16, 1004=18, 1005=20`, then repeat.

If an old `data\history` folder remains from the earlier draft, the script reminds you it is unused — safe to delete:

```powershell
Remove-Item -Recurse -Force data\history
```

**Quick checks**

```powershell
Get-ChildItem data\landing\*_2026111*.csv, data\landing\*_20261116.csv | Select-Object Name
(Get-Content data\landing\sales_20261110.csv | Measure-Object -Line).Lines   # 811 = header + 810

Get-Content data\landing\item_master_20261110.csv -TotalCount 2
Get-Content data\landing\store_master_20261110.csv -TotalCount 2

Select-String -Path data\landing\sales_20261110.csv, data\landing\sales_20261116.csv -Pattern 'M000016' | Select-Object -First 4

## defects
Select-String -Path data\landing\sales_20261111.csv -Pattern 'T20261111-1001-0003,1,.*1001,,'
Select-String -Path data\landing\sales_20261112.csv -Pattern '199999'
Select-String -Path data\landing\store_master_20261112.csv, data\landing\store_master_20261113.csv -Pattern '^1008,'
```

---

### 6.5 Points to remember

- **Five days are enough** for the medallion pipeline + recon. Long history is optional later.
- **400 txns/day fixed**; stores vary (**12/14/16/18/20**) so traffic looks uneven → **~810 lines** (Nov 11 = **811** with D4).
- **Clean sales ⊆ stocked inventory**; only D3 is an intentional orphan.
- **Defects are data now; gates come later** (L7–L10).
- **Flat files → snowflaked Gold** in L8; D5 needs SCD2 on `DIM_LOCN`.
- Re-run the generator anytime; it overwrites landing.

---

### 6.6 Exercises

1. **Grain.** In one sentence each: grain of a `sales` row, an `inventory` row, and a future `FCT_SALES` row?
2. **Volume.** Why vary txns per store but keep **400** total? What would a flat `16` everywhere hide?
3. **Hierarchy.** From one `item_master` data row, list the four level ids. Which Gold tables in L8?
4. **Stocked set.** Why allow only the planted `199999` orphan, not ~40% accidental ones?
5. **D5.** Diff `store_master` for `1008` on 12 Nov vs 13 Nov. Which attributes changed, and why is that SCD2 (not SCD1)?

---

### 6.7 Checkpoint

Reply with:

- [x] Generator finished; sales counts **810 / 811 / 810 / 810 / 810**; store `1001` has 12 txns, `1005` has 20
- [x] Nov 10 net ≈ **2819814.46**; defect lines printable (D1–D5)
- [x] `1008` city Spokane on 12 Nov, Portland on 13 Nov
- [x] (Optional) old `data\history` removed
- [x] Answers to exercises 1–5

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| `RuntimeError: … unstocked items` | Stocked list and sales picker out of sync — don’t hand-edit mid-run |
| Landing still has 5000-line sales | Re-run the generator; confirm you ran `generate_complex.py` not `generate_simple.py` |
| Want L2 simple Nov 10 back | `python scripts\generate_simple.py` (L2 only) |
| Still see `data\history` | Leftover from the first draft; delete it |

---

<a id="lesson-07"></a>

## Lesson 7: Silver (TMP tables, gates G3–G6 + G12)

| | |
|---|---|
| **Goal** | Turn Bronze STG text into **typed Silver TMP** tables; **quarantine** bad rows (never drop); prove **STG = TMP + quarantine** |
| **You build** | 6 TMP DDLs, sales/inventory/customer/hier loads, DAG `07_silver` |
| **Status** | **Done** (19 Nov 2026) |
| **Not yet** | Gold dims/facts (L8–L9); G7 orphan resolve; SCD2 proof on D5 |
| **Theory (optional)** | [3.6 SQL toolkit](THEORY.md#36-sql-toolkit) · [3.7 NULL](THEORY.md#37-null-handling) · [3.10 types](THEORY.md#310-data-types-and-why-we-pick-them) · [5.1 Quarantine](THEORY.md#51-quarantine-never-drop) · [5.2 Gates](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) · [5.6 G12](THEORY.md#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |

---

### 7.1 The idea in one picture

```
STG_* (VARCHAR, day D)
        │  cast · trim · NULLIF
        │  G3 keys · G4 dedupe · G5 date window · G6 maths/qty · G12 outliers
        ├──────────────────► TMP_QUARANTINE  (GATE_ID, REASON_CD, RAW_RECORD)
        ▼
TMP_SALES_LINE ──► TMP_CUSTOMER (MERGE, LAST_SEEN_DT)
TMP_ITEM_LOC_SNAPSHOT  (AVAILABLE_QTY = ON_HAND − RESERVED)  + G6b alert only
TMP_ITEM_HIER / TMP_LOCN_HIER  (INSERT OVERWRITE from masters)

Identity:  COUNT(STG_SALES D)  =  COUNT(TMP_SALES_LINE D)  +  COUNT(quarantine sales D)
```

Bronze promised “as landed.” Silver promises **types, keys, and honesty about rejects**.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Transient table | Snowflake table not Fail-safe; cheap rebuild | All `TMP_*` |
| Quarantine | Bad row kept with reason, not deleted | `TMP_QUARANTINE` |
| `TRY_TO_NUMBER` / `TRY_TO_DATE` | Cast or return NULL (no hard fail) | Typing STG → TMP |
| `INSERT OVERWRITE` | Replace **all** rows of the table | Masters `TMP_*_HIER` |
| `MERGE` | Upsert | `TMP_CUSTOMER` |
| G6b | Soft rule | `RESERVED > ON_HAND` → **alert**, still load |
| G12 | Configurable ranges | Limits from `medallion_config.outlier_rules` |

---

### 7.2 Tables you create

| Table | Load pattern | Grain |
|---|---|---|
| `TMP_SALES_LINE` | `DELETE` day D then `INSERT` survivors | one cleaned POS line |
| `TMP_CUSTOMER` | `MERGE` on `CUSTOMER_ID` | one member; `LAST_SEEN_DT` |
| `TMP_ITEM_LOC_SNAPSHOT` | `DELETE` day D then `INSERT` | one item@store that day; has `AVAILABLE_QTY` |
| `TMP_ITEM_HIER` | `INSERT OVERWRITE` | one item + full product hierarchy |
| `TMP_LOCN_HIER` | `INSERT OVERWRITE` | one store + full location hierarchy |
| `TMP_QUARANTINE` | append (clear day+object first) | one rejected STG row |

Types follow [THEORY 3.10](THEORY.md#310-data-types-and-why-we-pick-them): money `NUMBER(18,2)`, qty `NUMBER(18,3)`, flags `BOOLEAN`, quarantine payload `VARIANT`.

---

### 7.3 Code walkthrough

#### a) DDL — `include/sql/ddl/silver/tmp_*.sql`

Each file is `CREATE OR REPLACE TRANSIENT TABLE …`. Quarantine columns match the diagram:  
`INGEST_DT`, `SOURCE_FILE`, `OBJECT_NM`, `GATE_ID`, `REASON_CD`, `RAW_RECORD`, `QUARANTINED_TS`.

#### b) Sales load — `load/silver/tmp_sales_line.sql` (the important one)

Runs as several statements (`split_statements=True`):

1. Delete day’s quarantine rows for `TMP_SALES_LINE` + delete day’s TMP sales
2. **G3** — null `TRANSACTION_ID` / `LINE_NUM` / `LOCN_ID` / `ITEM_ID` after `TRIM`
3. **G4** — duplicate `(TRANSACTION_ID, LINE_NUM)`: keep max `_SOURCE_ROW_NUM`, quarantine others
4. **G5** — `SALE_DT` not in `[D−7, D]` (or unparseable)
5. **G6** — `SOLD_QTY ≤ 0` or `NET` not within 0.01 of `SALES − DISCOUNT`
6. **G12** — qty / line net / price vs inventory reference (limits from Variable)
7. **INSERT** survivors into `TMP_SALES_LINE` with typed columns

`RAW_RECORD = OBJECT_CONSTRUCT(*)` keeps the whole STG row for debugging.

#### c) Other loads

| File | What it does |
|---|---|
| `tmp_customer.sql` | `MERGE` members from cleaned sales + STG name/email |
| `tmp_item_loc_snapshot.sql` | G3 + G12 on-hand; insert with `AVAILABLE_QTY` |
| `tmp_item_hier.sql` / `tmp_locn_hier.sql` | `INSERT OVERWRITE` typed masters for day D |

#### d) DAG — `dags/07_silver.py`

```
create_tmp_* (×6)
   → load_tmp_sales_line → load_tmp_customer
                       └→ load_tmp_item_loc_snapshot → g06b_alert_reserved
   → load_tmp_item_hier → load_tmp_locn_hier
   → assert_sales_identity → quarantine_summary
```

- `g06b_alert_reserved`: prints rows where `RESERVED > ON_HAND`; **does not fail**  
- `assert_sales_identity`: fails if `STG ≠ TMP + quarantine` for sales  
- `quarantine_summary`: prints `GATE_ID / REASON_CD / count`

No hard-coded Snowflake names in the DAG — SQL uses `{{ var.json.medallion_config.* }}`.

---

### 7.4 Run it

**Prerequisite:** Bronze must hold the **complex** day (not the old L2 simple Nov 10).

```powershell
## if landing empty, regenerate complex files
python scripts\generate_complex.py

## load Bronze for Nov 10 (overwrite PUT + DELETE/FORCE)
docker compose run --rm airflow-cli airflow dags test 05_bronze_load 2026-11-10

## Silver
docker compose run --rm airflow-cli airflow dags test 07_silver 2026-11-10
```

**Snowsight checks (Nov 10):**

```sql
-- identity (expect STG = TMP + QUAR)
SELECT
  (SELECT COUNT(*) FROM MEDALLION_DB.BRONZE.STG_SALES WHERE _INGEST_DT = '2026-11-10') AS stg,
  (SELECT COUNT(*) FROM MEDALLION_DB.SILVER.TMP_SALES_LINE WHERE _INGEST_DT = '2026-11-10') AS tmp,
  (SELECT COUNT(*) FROM MEDALLION_DB.SILVER.TMP_QUARANTINE
    WHERE INGEST_DT = '2026-11-10' AND OBJECT_NM = 'TMP_SALES_LINE') AS quar;

-- G12 reasons (A1–A3 on sales; A4 on inventory object)
SELECT GATE_ID, REASON_CD, OBJECT_NM, COUNT(*) AS cnt
FROM MEDALLION_DB.SILVER.TMP_QUARANTINE
WHERE INGEST_DT = '2026-11-10'
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- valid outliers still in TMP (e.g. qty 480, price 2499.99)
SELECT TRANSACTION_ID, SOLD_QTY, UNIT_PRICE_AMT, NET_SALES_AMT
FROM MEDALLION_DB.SILVER.TMP_SALES_LINE
WHERE _INGEST_DT = '2026-11-10'
  AND (SOLD_QTY = 480 OR UNIT_PRICE_AMT = 2499.99);
```

**Optional — Nov 11 (planted D1/D2/D4):** copy that day’s files to landing, bronze-load, then silver; expect G3 / G6 / G4 in quarantine.

---

### 7.5 Points to remember

- **Quarantine, don’t drop** — finance and recon need the rejected amount later (G11).  
- **G12 limits are data** — change `variables.json` → re-import Variable; no SQL edit.  
- **G6b alerts only** — reserved > on hand is worth knowing, not always a hard fail.  
- **Masters overwrite; facts/slices delete-by-day** — matches full vs delta source patterns.  
- **Identity every run** — if STG ≠ TMP + quar, the load is wrong even if the DAG stayed green.

---

### 7.6 Exercises

1. **G3 vs G12.** Why can’t G6 catch a price typed ×1000? Which gate does, and why?
2. **G4.** On Nov 11, which physical row of the duplicate is kept, and which column decides “latest”?
3. **Reference price.** G12 joins inventory for the ratio rule. What goes wrong if that item has no inventory row that day?
4. **Identity.** If you deleted a quarantine insert by mistake but still inserted all STG rows into TMP, what would `assert_sales_identity` do?
5. **Variable.** Change `max_sold_qty` to `400` in the Variable, re-import, re-run Silver for Nov 10. What extra row is quarantined? Restore `1000` afterward.

---

### 7.7 Checkpoint

Reply with:

- [x] `07_silver` green for `2026-11-10` after complex Bronze load  
- [x] Identity: `stg = tmp + quar` for sales  
- [x] Quarantine shows G12 reasons for the Nov 10 anomalies (sales + on-hand)  
- [x] Valid outliers (480 qty / 2499.99) still in `TMP_SALES_LINE`  
- [x] Answers to exercises 1–5  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| Identity fails / STG count is 5000 | Bronze still has L2 simple data — regenerate complex + re-run `05_bronze_load` |
| `TMP_*` missing | DAG create tasks failed — check Snowflake role can create in `SILVER` |
| G12 catches nothing | Variable `outlier_rules` missing — re-import `variables.json` |
| Jinja / unsupported format on PUT | That’s Bronze; Silver only reads STG tables |
| `Duplicate field key 'ITEM_ID'` | Use `OBJECT_CONSTRUCT(s.*)` after a JOIN, not `OBJECT_CONSTRUCT(*)` |
| G6 quarantines many good rows | `TRY_TO_NUMBER(x)` defaults to **scale 0** (integer) — use `TRY_TO_NUMBER(x, 18, 2)` / `(…, 18, 3)` |
| `OBJECT_CONSTRUCT` / VARIANT path error | Confirm quarantine `RAW_RECORD:"_SOURCE_ROW_NUM"` matches STG lineage column name |

---

<a id="lesson-08"></a>

## Lesson 8: Gold dimensions (SCD2 hierarchies, SCD1 customer, DIM_DAY seed)

| | |
|---|---|
| **Goal** | Build the **10 Gold dimensions**: snowflaked product + location hierarchies (SCD2), `DIM_CUSTOMER` (SCD1), `DIM_DAY` (seed), with surrogate `*_KY` keys |
| **You build** | Sequences, DIM DDLs, SCD2/SCD1/seed loads, DAG `08_gold_dims` |
| **Status** | **Done** (19 Nov 2026) |
| **Not yet** | Facts G7/G8 (L9); full SCD2 proof on D5 across days (exercise / L10) |
| **Theory (optional)** | [4.2 Keys](THEORY.md#42-natural-keys-vs-surrogate-keys) · [4.3 Hierarchies](THEORY.md#43-hierarchies) · [4.4 SCD](THEORY.md#44-slowly-changing-dimensions-scd) · [3.6 SQL](THEORY.md#36-sql-toolkit) |

---

### 8.1 The idea in one picture

```
TMP_ITEM_HIER ──► DIM_DEPARTMENT → DIM_CATEGORY → DIM_SUBCATEGORY → DIM_ITEM
                         (SCD2 parents first; ITEM_DESC = SCD1 overwrite)

TMP_LOCN_HIER ──► DIM_DIVISION → DIM_REGION → DIM_CITY → DIM_LOCN
                         (SCD2; D5 city move → new DIM_LOCN version)

TMP_CUSTOMER ──► DIM_CUSTOMER   (SCD1 MERGE)

seed_dim_day.csv ──PUT──► @stage/seed/ ──COPY──► DIM_DAY   (no STG/TMP)
```

Silver cleaned the text. Gold gives each member a **warehouse key** (`*_KY`) and keeps **history** where the brief requires it.

**Basic terms**

| Term | Meaning | Here |
|---|---|---|
| Natural key | Source id | `ITEM_ID`, `LOCN_ID` |
| Surrogate key | Warehouse id facts join on | `ITEM_KY` from `SEQ_ITEM_KY` |
| SCD2 | Close old row, insert new version | 8 hierarchy dims |
| SCD1 | Overwrite in place | `DIM_CUSTOMER`; `DIM_ITEM.ITEM_DESC` |
| Snowflaked hierarchy | Child holds parent `*_KY` | `DIM_ITEM.SUBCATEGORY_KY` |
| Open end date | Still current | `EFFECTIVE_END_DT = 9999-12-31`, `CURRENT_FLG = TRUE` |

**Sequence vs HASH (we pick sequence):**

| | Sequence | `HASH(id, start_dt)` |
|---|---|---|
| New version | `NEXTVAL` | Same inputs → same key |
| Re-run safety | Guard inserts with `NOT EXISTS` (current row) | Naturally stable |
| Full rebuild | New numbers → facts must reload | Facts still point correctly |

We use **sequences +** `NOT EXISTS` so keys stay simple `NUMBER(38,0)`. HASH is taught so you know the rebuild trade-off.

---

### 8.2 SCD2 daily steps (day D)

For each hierarchy dim:

1. From TMP, compute a hash of **tracked attributes** (including parent key).
2. **Close** current rows whose hash differs → `EFFECTIVE_END_DT = D−1`, `CURRENT_FLG = FALSE`.
3. **Insert** where no current row exists (brand-new or just closed) → `EFFECTIVE_START_DT = D`, end `9999-12-31`, `CURRENT_FLG = TRUE`, `SEQ_*.NEXTVAL`.

Re-run same D: hashes match → nothing closes → nothing inserts. Safe.

`DIM_ITEM`: SCD2 ignores `ITEM_DESC`; a final `UPDATE` always refreshes `ITEM_DESC` on the current row (SCD1).

---

### 8.3 Code walkthrough

#### a) DDL — `include/sql/ddl/gold/`

- `sequences.sql` — one sequence per dim that needs a surrogate  
- `dim_*.sql` — `CREATE TABLE IF NOT EXISTS` (keeps SCD2 history across days; never `OR REPLACE` on Gold dims)

#### b) Loads — `include/sql/load/gold/`

| File | Pattern |
|---|---|
| `dim_day.sql` | `DELETE` + `COPY` from staged seed (`FORCE`) |
| `dim_department.sql` … `dim_locn.sql` | SCD2 close + insert |
| `dim_item.sql` | SCD2 + SCD1 `ITEM_DESC` update |
| `dim_customer.sql` | `MERGE` SCD1 |

`put_seed_dim_day.sql` must start with `PUT` (same Snowflake client rule as L3).

#### c) DAG — `dags/08_gold_dims.py`

```
create_* → put_seed_dim_day → load_dim_day
                ├─ dept → cat → subcat → item
                └─ div  → region → city → locn
                              └───────────────→ load_dim_customer
```

Parents before children; product ‖ location in parallel after `DIM_DAY`.

---

### 8.4 Run it

**Prerequisite:** Silver for the same date (complex Bronze → `07_silver`).

```powershell
## seed file (once, if missing)
python scripts\generate_seed_dim_day.py

docker compose run --rm airflow-cli airflow dags test 08_gold_dims 2026-11-10
```

**Snowsight checks:**

```sql
SELECT COUNT(*) FROM MEDALLION_DB.GOLD.DIM_DAY;           -- 730
SELECT COUNT(*) FROM MEDALLION_DB.GOLD.DIM_ITEM WHERE CURRENT_FLG;  -- 101
SELECT COUNT(*) FROM MEDALLION_DB.GOLD.DIM_LOCN WHERE CURRENT_FLG;  -- 25

-- one current row per natural key
SELECT ITEM_ID, COUNT(*) AS cnt
FROM MEDALLION_DB.GOLD.DIM_ITEM WHERE CURRENT_FLG
GROUP BY 1 HAVING COUNT(*) > 1;   -- expect 0 rows

-- parent keys resolve
SELECT COUNT(*) AS orphan_items
FROM MEDALLION_DB.GOLD.DIM_ITEM i
LEFT JOIN MEDALLION_DB.GOLD.DIM_SUBCATEGORY s
  ON i.SUBCATEGORY_KY = s.SUBCATEGORY_KY AND s.CURRENT_FLG
WHERE i.CURRENT_FLG AND s.SUBCATEGORY_KY IS NULL;  -- 0
```

**Optional SCD2 peek (after you also run Bronze→Silver→Gold for 2026-11-13):**

```sql
SELECT LOCN_ID, LOCN_NM, CITY_KY, EFFECTIVE_START_DT, EFFECTIVE_END_DT, CURRENT_FLG
FROM MEDALLION_DB.GOLD.DIM_LOCN
WHERE LOCN_ID = '1008'
ORDER BY EFFECTIVE_START_DT;
-- expect Spokane version closed, Portland version current
```

---

### 8.5 Points to remember

- **Parents first** — child insert looks up the parent’s **current** `*_KY`.  
- **SCD2 = close then insert**; never update tracked attrs in place.  
- `ITEM_DESC` **is the exception** (SCD1 on an SCD2 table).  
- `DIM_DAY` **skips Bronze/Silver** — departure 1.  
- **Date order matters** later (`max_active_runs=1`) when versions chain across days.

---

### 8.6 Exercises

1. **Keys.** Why do facts join on `ITEM_KY` instead of `ITEM_ID` once SCD2 exists?
2. **Re-run.** Run `08_gold_dims` twice for 2026-11-10. Do current row counts stay the same? Do sequence values stay unused on the second run? Why?
3. **SCD1 vs SCD2.** `ITEM_NM` changes vs `ITEM_DESC` changes — which opens a new version?
4. **Hierarchy.** From `DIM_ITEM` current row, write the join path up to `DIM_DEPARTMENT`.
5. **D5.** What attribute change on `LOCN_ID=1008` forces a new `DIM_LOCN` version?

---

### 8.7 Checkpoint

Reply with:

- [x] `08_gold_dims` green for `2026-11-10`  
- [x] `DIM_DAY` = 730; current `DIM_ITEM` = 101; current `DIM_LOCN` = 25  
- [x] No duplicate current natural keys; no orphan parent keys  
- [x] Answers to exercises 1–5  

---

### Troubleshooting

| Symptom                                      | Fix                                                                                               |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| PUT seed fails / unsupported format          | SQL file must **start with** `PUT`                                                                |
| `UPPER` / `TRIM` not supported within a COPY | Transforming COPY allows only a small function set — compare flags as `$11 = 'Y'` (seed is clean) |
| Child insert 0 rows                          | Parent dim empty — run parent load first; confirm Silver TMP for `ds`                             |
| Sequence / NEXTVAL error                     | `create_sequences` must run; role needs USAGE on sequence                                         |
| SCD2 doubles current rows                    | Close step missing or `CURRENT_FLG` not set FALSE                                                 |
| History wiped across days                    | Gold DDL must be `IF NOT EXISTS`, not `OR REPLACE`                                                |
| `EQUAL_NULL` / MERGE error                   | Check typed `TMP_CUSTOMER`; Snowflake supports `EQUAL_NULL`                                       |
| Empty child dims                             | Silver TMP for `ds` empty — re-run `07_silver` for that date first                                |

---

<a id="lesson-09"></a>

## Lesson 9: Gold facts (FCT_SALES, soft-close inventory, price periods, G7/G8)

| | |
|---|---|
| **Goal** | Load the **3 Gold facts** from Silver TMP using dim surrogate keys; prove **G7** (orphan quarantine + alert) and **G8** (inventory ±20%) |
| **You build** | `FCT_`* DDL + loads, gates G7/G8, DAG `09_gold_facts` |
| **Status** | **Done** (19 Nov 2026) |
| **Not yet** | PK checker / G10 (L10); AGG + G11 (L11); full `medallion_daily` stitch (L11–L13) |
| **Theory (optional)** | [4.1 Grain](THEORY.md#41-grain-facts-and-dimensions) · [4.5 Fact types](THEORY.md#45-fact-types-sales-inventory-soft-close-price-periods) · [4.6 Orphans](THEORY.md#46-orphans-and-late-arriving-dimensions) · [5.2 Gates](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

### 9.1 The idea in one picture

```
TMP_SALES_LINE ──G7──► (orphans → TMP_QUARANTINE + alert)
                    └─► FCT_SALES   MERGE (TRANSACTION_ID, LINE_NUM)  CLUSTER BY DAY_KY

TMP_ITEM_LOC_SNAPSHOT ──G8──► (±20% vs prior STG day → fail)
                           ├─► FCT_INVENTORY  soft-close (never DELETE)
                           └─► FCT_PRICE      periods on UNIT/PROMO change
```

Dims must already have current `ITEM_KY` / `LOCN_KY` (and `CUSTOMER_KY` when present).

| Term | Meaning | Here |
|---|---|---|
| Grain | What one fact row is | Sales = one POS line; inventory = one item×locn version; price = one price period |
| Soft-close | Close old, insert new; never delete | `FCT_INVENTORY` (departure 3) |
| G7 | Keys must resolve | Quarantine + alert; Gold stays clean |
| G8 | Snapshot size stable | Fail if \|today − prior\| / prior > 20% |
| First pipeline day | No prior inventory day | G8 **skips** (pass) |

---

### 9.2 Code walkthrough

#### a) DDL — `include/sql/ddl/gold/fct_*.sql`

`CREATE TABLE IF NOT EXISTS` (keep history). Surrogate PKs match the diagram: `SALES_LINE_KY`, `INVENTORY_KY`, `PRICE_KY` (sequences in `sequences.sql`). Business keys stay unique: `(TRANSACTION_ID, LINE_NUM)`, `(ITEM_KY, LOCN_KY, SNAPSHOT_DT)`, `(ITEM_KY, LOCN_KY, EFFECTIVE_START_DT)`. Soft-close adds `EFFECTIVE_END_DT` / `CURRENT_FLG` on inventory (departure 3). `FCT_SALES` clusters on `DAY_KY`; inventory on `SNAPSHOT_DT`.

#### b) Gates

| File | Role |
|---|---|
| `gates/g07_keys_resolve.sql` | Insert orphans into `TMP_QUARANTINE` (`GATE_ID=G7`) |
| `g07_alert` (Python) | Print counts — does **not** fail the DAG |
| `g08_inventory_count` (Python) | Compare TMP today vs prior STG day — **fails** if >20% |

#### c) Loads — `include/sql/load/gold/`

| File | Pattern |
|---|---|
| `fct_sales.sql` | `MERGE` only rows that join current `DIM_ITEM` + `DIM_LOCN` |
| `fct_inventory.sql` | Same-day UPDATE; else close older open rows → INSERT |
| `fct_price.sql` | Same pattern on `UNIT_PRICE_AMT` / `PROMO_PRICE_AMT` |

#### d) DAG — `dags/09_gold_facts.py`

```
create_fct_* → g07_keys_resolve → g07_alert → load_fct_sales
             → g08_inventory_count → load_fct_inventory ‖ load_fct_price
```

---

### 9.3 Run it

**Prerequisite:** `08_gold_dims` and `07_silver` green for the same date.

```powershell
docker compose run --rm airflow-cli airflow dags test 09_gold_facts 2026-11-10
```

**Snowsight checks:**

```sql
-- Nov 10 file = 810 sales / 1501 inv; G12 drops 3 sales + 1 inv → Gold 807 / 1500
SELECT COUNT(*) FROM MEDALLION_DB.GOLD.FCT_SALES WHERE DAY_KY = 20261110;  -- 807

SELECT COUNT(*) FROM MEDALLION_DB.GOLD.FCT_INVENTORY WHERE CURRENT_FLG;  -- 1500
SELECT COUNT(*) FROM MEDALLION_DB.GOLD.FCT_PRICE WHERE CURRENT_FLG;      -- 1500

-- Must match Silver (facts only load what TMP kept)
SELECT COUNT(*) FROM MEDALLION_DB.SILVER.TMP_SALES_LINE WHERE _INGEST_DT = '2026-11-10';
SELECT COUNT(*) FROM MEDALLION_DB.SILVER.TMP_ITEM_LOC_SNAPSHOT WHERE _INGEST_DT = '2026-11-10';

-- G7: empty result = 0 orphans on Nov 10 (good)
SELECT REASON_CD, COUNT(*) FROM MEDALLION_DB.SILVER.TMP_QUARANTINE
WHERE INGEST_DT = '2026-11-10' AND GATE_ID = 'G7' GROUP BY 1;

-- Optional: prove D3 orphan on Nov 12 (after Bronze→Silver→dims→facts for 12)
-- Expect 1× ORPHAN_ITEM for ITEM_ID 199999; that line absent from FCT_SALES
```

**Re-run check:** run `09_gold_facts` twice for `2026-11-10`. Counts should stay the same (MERGE + soft-close guards).

---

### 9.4 Points to remember

- Facts join **surrogate** keys so SCD2 history on dims does not rewrite fact FKs for past days.  
- G7 keeps Gold clean; G11 later adds quarantined net back for recon.  
- Soft-close: stock on date X → `WHERE X BETWEEN SNAPSHOT_DT AND EFFECTIVE_END_DT`.  
- G8 uses the **prior loaded STG day**, not “calendar yesterday” alone (first day has no prior).

---

### 9.5 Exercises

1. **Grain.** One sentence each: grain of `FCT_SALES`, open `FCT_INVENTORY`, open `FCT_PRICE`.
2. **G7.** Why quarantine instead of `ITEM_KY = -1`? What does recon need later?
3. **G8.** Why skip on the first pipeline day? What would a 25% drop mean operationally?
4. **Soft-close.** After Nov 10 then Nov 11, how do you read on-hand for a pair as of Nov 10?
5. **MERGE.** Re-run day D with a corrected net amount — which clause updates the existing fact?

---

### 9.6 Checkpoint

Reply with:

- [x] `09_gold_facts` green for `2026-11-10`  
- [x] `FCT_SALES` for `DAY_KY=20261110` = **807** (= TMP; no G7 orphans that day)  
- [x] Current `FCT_INVENTORY` / `FCT_PRICE` = **1500** (= TMP after G12)  
- [x] Second run same date: counts unchanged  
- [x] Answers to exercises 1–5  

---

### Troubleshooting

| Symptom                                     | Fix                                                                                                                                                                                       |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Orphan floods on Nov 10                     | Dims missing — run `08_gold_dims` first; confirm `CURRENT_FLG`                                                                                                                            |
| G8 fails first day                          | Should skip — check task log for `G8 SKIP`                                                                                                                                                |
| PK / column missing (`SALES_LINE_KY`, etc.) | Old empty FCT tables from an earlier draft — drop once then re-run DAG: `DROP TABLE IF EXISTS MEDALLION_DB.GOLD.FCT_SALES, MEDALLION_DB.GOLD.FCT_INVENTORY, MEDALLION_DB.GOLD.FCT_PRICE;` |
| PK violation on inventory                   | Same-day path must UPDATE not re-INSERT; re-pull `fct_inventory.sql`                                                                                                                      |
| Sales count low                             | G7 quarantined rows, or Silver TMP empty for `ds`                                                                                                                                         |
| Temp table / session error                  | `split_statements=True` keeps one session for inventory/price loads                                                                                                                       |

---

<a id="lesson-10"></a>

## Lesson 10: Prove defects (D1–D5), PK checker, gate G10

| | |
|---|---|
| **Goal** | **Prove** the five planted defects end-to-end; assert **natural-key uniqueness** on every Gold table; fail on **overlapping price periods** (G10) |
| **You build** | `gates/pk_*.sql`, `gates/g10_price_overlap.sql`, DAG `10_prove_defects` |
| **Status** | Done |
| **Not yet** | AGG + G9 + G11 recon (L11); idempotency proof (L12); five-day backfill (L13) |
| **Theory (optional)** | [3.8 Constraints](THEORY.md#38-constraints-are-not-enforced) · [4.4 SCD](THEORY.md#44-slowly-changing-dimensions-scd) · [4.6 Orphans](THEORY.md#46-orphans-and-late-arriving-dimensions) · [5.2–5.3 Gates / PK](THEORY.md#52-quality-gates-red-hexagons-on-the-diagram) |

---

### 10.1 The idea in one picture

```
L6 planted data          Already built gates              This lesson asserts
─────────────────        ──────────────────               ──────────────────
D1 null ITEM ×2    →     G3 quarantine (L7)         →     count = 2 on 2026-11-11
D2 neg qty         →     G6 quarantine (L7)         →     count ≥ 1 on 2026-11-11
D4 duplicate       →     G4 quarantine (L7)         →     count = 1 on 2026-11-11
D3 orphan 199999   →     G7 quarantine (L9)         →     count = 1; not in FCT
D5 LOCN 1008 move  →     SCD2 DIM_LOCN (L8)         →     Spokane closed, Portland current

Snowflake PK/UNIQUE = metadata only  →  pk_*.sql must return 0 rows
FCT_PRICE periods must not overlap   →  g10_price_overlap.sql → 0 rows
```

**Why a separate DAG?** Defects span **four dates**. L10 is the exam: load the days, then one green `10_prove_defects` run.

**Silver change (needed for multi-day quarantine):** TMP DDL is now `CREATE TRANSIENT TABLE IF NOT EXISTS` (not `OR REPLACE`), so `TMP_QUARANTINE` keeps Nov 10–13 rows as you load each day.

---

### 10.2 Code walkthrough

#### a) PK checkers — `include/sql/gates/pk_*.sql`

| Pattern | Tables |
|---|---|
| Simple unique | `DIM_DAY`, `DIM_CUSTOMER`, `FCT_SALES` |
| Current + no overlap | 8 SCD2 hierarchy dims |
| Current + version unique | `FCT_INVENTORY`, `FCT_PRICE` |

Any **returned row** = fail (see [THEORY 3.8](THEORY.md#38-constraints-are-not-enforced)).

#### b) G10 — `gates/g10_price_overlap.sql`

Self-join on `(ITEM_KY, LOCN_KY)` where effective ranges overlap.

#### c) DAG — `dags/10_prove_defects.py`

```
prove_d1_d2_d4 ‖ prove_d3_g7 ‖ prove_d5_scd2
                    └──────────► pk_checker ► g10_price_overlap
```

---

### 10.3 Run it

**1. Ensure complex landing files exist** for 11, 12, 13 (and 10 if needed):

```powershell
python scripts\generate_complex.py
```

**2. Load each defect day in order** (Bronze → Silver → dims → facts). Example for one date:

```powershell
docker compose run --rm airflow-cli airflow dags test 05_bronze_load 2026-11-11
docker compose run --rm airflow-cli airflow dags test 07_silver 2026-11-11
docker compose run --rm airflow-cli airflow dags test 08_gold_dims 2026-11-11
docker compose run --rm airflow-cli airflow dags test 09_gold_facts 2026-11-11
```

Repeat for **2026-11-12** and **2026-11-13**.  
(You already did **2026-11-10**; re-run Silver for 10 only if Nov 10 quarantine was wiped by an old `OR REPLACE`.)

**3. Prove:**

```powershell
docker compose run --rm airflow-cli airflow dags test 10_prove_defects 2026-11-13
```

(`ds` is unused for the assert dates; any logical date is fine.)

**Snowsight peek (D5):**

```sql
SELECT l.LOCN_NM, c.CITY_NM, l.EFFECTIVE_START_DT, l.EFFECTIVE_END_DT, l.CURRENT_FLG
FROM MEDALLION_DB.GOLD.DIM_LOCN l
JOIN MEDALLION_DB.GOLD.DIM_CITY c ON l.CITY_KY = c.CITY_KY AND c.CURRENT_FLG
WHERE l.LOCN_ID = '1008'
ORDER BY l.EFFECTIVE_START_DT;
```

---

### 10.4 Points to remember

- **Constraints on Snowflake are not enforced** — the PK checker is the real guard.  
- **Quarantine is evidence** — proving D1–D4 is counting `TMP_QUARANTINE`, not hunting for rows in Gold.  
- **G7 keeps Gold clean** — orphan never gets an `ITEM_KY`.  
- **SCD2 proof** = closed prior city + one current row, not “two currents”.  
- **Date order** matters for SCD2 and soft-close (`max_active_runs=1` later).

---

### 10.5 Exercises

1. **PK.** Why isn’t `PRIMARY KEY` on `FCT_SALES` enough to stop duplicates?
2. **D3.** After G7, where does the orphan’s net sales show up for recon (G11)?
3. **D5.** What attribute change forced a new `DIM_LOCN` version (name vs city vs both)?
4. **G10.** Write in one sentence what “overlap” means for two price periods.
5. **Re-run.** Run `08_gold_dims` + `10_prove_defects` again for 2026-11-13. Does D5 still pass with exactly one current row?

---

### 10.6 Checkpoint

Reply with:

- [x] Pipeline green for **11, 12, 13** (Bronze→Silver→dims→facts)  
- [x] `10_prove_defects` green  
- [x] D5 peek shows Spokane closed + Portland current  
- [x] Answers to exercises 1–5  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| D1/D2/D4 counts 0 | Re-run `07_silver` for `2026-11-11` after IF NOT EXISTS fix; confirm landing files are complex |
| D3 G7 = 0 | Need `09_gold_facts` for **2026-11-12** (G7 runs in facts DAG) |
| D5 only one version | Need `08_gold_dims` for **2026-11-13** after Nov 10 (or 11/12) dims exist |
| PK OVERLAP false positive | Adjacent periods must touch (`END = START−1`), not overlap; check close uses `DATEADD(day,-1,ds)` |
| Landing empty | `05_bronze_load` archives files — regenerate with `generate_complex.py` before Bronze |

---

<a id="lesson-11"></a>

## Lesson 11: Aggregate (G9), reconciliation (G11), return-flag branch

| | |
|---|---|
| **Goal** | Build the Gold **AGG** + live **view**, prove **G9** (AGG = FCT), write the **G11** recon table, and **branch** on a bool → pass or alert-then-fail |
| **You build** | AGG/VW DDL + load, `g09`, recon DDL/load, DAG `11_agg_recon` |
| **Status** | Done |
| **Not yet** | Idempotent reload proof (L12); five-day backfill evidence (L13) |
| **Deferred** | Local SMTP inbox (Mailpit) — not required for this lesson; see §11.7 |
| **Theory (optional)** | [4.7 Aggregates](THEORY.md#47-aggregates-and-views) · [5.4 Recon G11](THEORY.md#54-reconciliation-g11) · [2.10 Branching](THEORY.md#210-branching-trigger-rules-and-the-return-flag-pattern) · [2.11 Alerts](THEORY.md#211-failure-handling-retries-callbacks-and-alerts) |

---

### 11.1 The idea in one picture

```
FCT_SALES (line grain)
        │
        ├─► AGG_DAILY_ITEM_LOCN_SALES   (table: fast rollup for day D)
        │         └─ G9: AGG totals = FCT totals  → fail if ≠
        │
        └─► VW_DAILY_ITEM_LOCN_SALES    (view: always live, same grain)

G11 identity (per day):
  STG net  −  quarantined net (G3–G7, G12)  −  FCT net  =  VARIANCE  (must be 0)

reconcile() → bool
  True  → recon_passed
  False → send_alert → fail_run
```

**Why AGG and a view?** Reports want pre-rolled totals (AGG). The view proves the same grain without lag — G9 checks the table against FCT so a bad rollup cannot hide.

**Why return-flag?** The recon task returns `True`/`False`. `@task.branch` picks the next task id; the other path is **skipped**. On failure the DAG still goes through `send_alert` then `fail_run` (you will only hit that path if variance ≠ 0).

**Mailpit / SMTP:** not used for this lesson’s checkpoint. A green run never reaches `send_alert`.

---

### 11.2 Code walkthrough

#### a) AGG + VW — `include/sql/ddl/gold/`

| Object | Role |
|---|---|
| `AGG_DAILY_ITEM_LOCN_SALES` | `(DAY_KY, ITEM_KY, LOCN_KY)` + counts/sums; `CLUSTER BY (DAY_KY)` |
| `VW_DAILY_ITEM_LOCN_SALES` | Same grain over `FCT_SALES` (`CREATE OR REPLACE VIEW`) |

Load (`load/gold/agg_daily_item_locn_sales.sql`): **DELETE** day D → **INSERT** `GROUP BY` from `FCT_SALES` for that day only.

#### b) G9 — `gates/g09_agg_equals_fct.sql`

Any returned row = fail (NET delta > 0.01 or line count mismatch).

#### c) G11 — `AUDIT.RECON_DAILY_NET_SALES`

Refreshes **every day present in** `STG_SALES` (not only `{{ ds }}`):

`VARIANCE_AMT = SOURCE_NET − QUARANTINE_NET − FCT_NET`

Quarantine net comes from `TMP_QUARANTINE` where `OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES')` (Silver gates + G7).

#### d) DAG — `dags/11_agg_recon.py`

```
create_agg ‖ create_vw ‖ create_recon
        └─► load_agg → g09 → write_recon → reconcile → branch_recon
                                                      ├─► recon_passed
                                                      └─► send_alert → fail_run
```

On success you only care about the left branch (`recon_passed`). The right branch is there for the return-flag pattern; you do not need SMTP wired up to pass the checkpoint.

---

### 11.3 Run it

#### 1) Prerequisite

Bronze → Silver → dims → facts for the pipeline days you care about (at least **2026-11-10…13**; add **16** if you generated it). L10 already proved defects on those days.

#### 2) Run AGG + recon

Logical date = a day that has `FCT_SALES` (G9 uses `{{ ds }}`; recon covers all STG days):

```powershell
docker compose run --rm airflow-cli airflow dags test 11_agg_recon 2026-11-13
```

Optional: re-run for each day so AGG is filled for every `DAY_KY` (G9 only checks that run’s `ds`).

#### 3) Peek in Snowsight

```sql
-- G9 sanity for one day
SELECT SUM(NET_SALES_AMT) FROM MEDALLION_DB.GOLD.AGG_DAILY_ITEM_LOCN_SALES WHERE DAY_KY = 20261113;
SELECT SUM(NET_SALES_AMT) FROM MEDALLION_DB.GOLD.FCT_SALES WHERE DAY_KY = 20261113;

-- G11 table (expect VARIANCE_AMT = 0 on every row)
SELECT DAY_KY, SOURCE_NET_AMT, QUARANTINE_NET_AMT, FCT_NET_AMT, VARIANCE_AMT
FROM MEDALLION_DB.AUDIT.RECON_DAILY_NET_SALES
ORDER BY DAY_KY;
```

On success: graph shows `recon_passed` green; `send_alert` / `fail_run` **skipped**.

---

### 11.4 Points to remember

- AGG is a **materialized** rollup; VW is **always** current — G9 guards the materialization.  
- Recon uses **STG** net (not TMP) so Silver bugs cannot hide.  
- Quarantined net **must** be subtracted or G11 fails while Gold is “correct.”  
- `@task.branch` returns a **task_id** string; skipped tasks are normal, not errors.  
- Alerting (email / Mailpit) is optional infrastructure — the important idea is **branch on a bool**, then fail.

---

### 11.5 Exercises

1. **Grain.** One sentence: why `(DAY_KY, ITEM_KY, LOCN_KY)` and not also `CUSTOMER_KY`?
2. **G9 vs G11.** What does each prove that the other does not?
3. **Quarantine in recon.** Which planted defect’s dollars show up in `QUARANTINE_NET_AMT` on 2026-11-11 vs 2026-11-12?
4. **Branch.** If `reconcile` returns `False`, which tasks run and which are skipped?

---

### 11.6 Checkpoint

Reply with:

- [x] `11_agg_recon` green for at least one loaded day (e.g. `2026-11-13`)  
- [x] `RECON_DAILY_NET_SALES`: every `VARIANCE_AMT` = **0** (paste the small result set)  
- [x] Answers to exercises 1–4  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| G9 fails, AGG empty | No `FCT_SALES` for that `ds` — run `09_gold_facts` first |
| G11 variance ≠ 0 | Missing quarantine for that day, or facts not loaded; compare STG / quar / FCT sums in Snowsight |
| `RECON` empty | No rows in `STG_SALES` — Bronze days never loaded |
| DAG not listed | Wait ~30s for dag-processor; check parse errors in processor logs |
| `send_alert` fails on a bad recon | Expected if SMTP is not set up — fix variance so you stay on `recon_passed`, or see §11.7 later |

---

### 11.7 Optional later: Mailpit + SMTP (not used now)

Skip this section for the current checkpoint. The DAG still has an `EmailOperator` on the fail branch (`send_alert`); that only runs when G11 finds a non-zero variance.

If you ever want a local inbox to see that email:

1. Compose service `mailpit` is already in `docker-compose.yaml` (UI **http://localhost:8025**, SMTP **1025**).
2. Connection id `smtp_default` is in `include/config/connections.example.json` (host must be `mailpit`, not `localhost`, from inside Compose).
3. Merge into `secrets/connections.json` and import:

```powershell
docker compose up -d mailpit
docker compose run --rm airflow-cli airflow connections import /opt/airflow/secrets/connections.json
```

Then force a fail path (e.g. temporarily break variance), confirm the message in Mailpit, restore, and re-run green.

---

<a id="lesson-12"></a>

## Lesson 12: Idempotency proof + reload of one bad day

| | |
|---|---|
| **Goal** | Prove a **second apply** of day D leaves **counts and nets unchanged**; learn the diagram’s **reload-of-day-D** policy |
| **You build** | Fingerprint SQL, `AUDIT.IDEM_DAY_SNAPSHOT`, DAG `12_idempotency` |
| **Status** | Done (reload-of-D drill deferred to L13 / full pipeline) |
| **Not yet** | Five-day backfill evidence (L13); final README (L14) |
| **Theory (optional)** | [6 Idempotency](THEORY.md#6-idempotency-playbook) · [5.5 Reload of day D](THEORY.md#55-retention-and-the-reload-of-one-bad-day-d) |

---

### 12.1 The idea in one picture

```
Idempotent  =  same logical date, run twice  →  same row counts and totals

Fingerprint (day D)
  STG / TMP / quarantine counts + nets
  FCT_SALES cnt+net · AGG cnt+net
  FCT_INVENTORY / FCT_PRICE current counts
  DIM_LOCN / DIM_ITEM current counts

DAG 12_idempotency
  snapshot BEFORE
       │
       ▼
  re-apply Silver → dims → facts → AGG   (same SQL as L7–L11)
       │
       ▼
  snapshot AFTER  →  assert BEFORE == AFTER
```

**Why not re-run Bronze in this DAG?** Landing files for D are usually already in `data/archive/` after `05_bronze_load`. Silver→Gold is enough to prove the warehouse loads are safe to repeat. Full Bronze reload (corrected file) is the **policy** in §12.4 — same idea, one more step at the front.

**What may change on a Bronze reload?** `_BATCH_ID` (it is `{{ run_id }}`). Counts and nets must not. The fingerprint **ignores** `_BATCH_ID` on purpose.

---

### 12.2 Code walkthrough

#### a) Fingerprint — `include/sql/checks/idem_day_fingerprint.sql`

One row of control totals for `{{ ds }}`. Any drift after a second apply = fail.

#### b) Audit table — `ddl/audit/idem_day_snapshot.sql`

Stores `BEFORE` / `AFTER` as `METRICS` (VARIANT) so you can peek in Snowsight later.

#### c) DAG — `dags/12_idempotency.py`

```
create_idem_day_snapshot → snapshot_before
        → reload TMP_* (parallel)
        → dim product chain ‖ dim location chain → dim_customer
        → g07 → fct_sales
        → fct_inventory ‖ fct_price
        → reload AGG → snapshot_after → assert_idempotent
```

Techniques you already built (playbook §6):

| Layer | Technique |
|---|---|
| TMP day slice | `DELETE WHERE _INGEST_DT = D` then insert |
| Hier TMP | `INSERT OVERWRITE` |
| SCD2 dims | hash compare + `NOT EXISTS` |
| `DIM_CUSTOMER` / `FCT_SALES` | `MERGE` |
| Inventory / price | soft-close / periods (no duplicate currents) |
| AGG | delete + recompute `DAY_KY` |

---

### 12.3 Run it

#### Prerequisite

Day **2026-11-13** (or any loaded day) already through Bronze→Silver→dims→facts→AGG (L7–L11). Prefer a mid/late pipeline day so G8 has a prior day.

#### Run the proof

```powershell
docker compose run --rm airflow-cli airflow dags test 12_idempotency 2026-11-13
```

Expect `assert_idempotent` green. Task log should show `IDEM OK: BEFORE == AFTER`.

#### Peek (optional)

```sql
SELECT DAY_KY, PHASE, METRICS, SNAPSHOT_TS
FROM MEDALLION_DB.AUDIT.IDEM_DAY_SNAPSHOT
WHERE DAY_KY = 20261113
ORDER BY SNAPSHOT_TS, PHASE;
```

---

### 12.4 Reload of one bad day D (policy — read, don’t automate yet)

When source sends a **corrected** file for day D ([THEORY 5.5](THEORY.md#55-retention-and-the-reload-of-one-bad-day-d)):

1. Put the corrected file in `data/landing/` (bad file stays in `data/archive/` — departure 5).
2. Bronze: `DELETE STG_* WHERE _INGEST_DT = D` then `COPY FORCE` (`05_bronze_load`).
3. Silver: rebuild TMP (+ quarantine) for D (`07_silver`).
4. Gold dims: re-apply masters; SCD2 undoes/reopens only if attributes actually change (`08_gold_dims`).
5. Gold facts: `MERGE` sales; soft-close inventory; re-derive price periods (`09_gold_facts`).
6. AGG for touched `DAY_KY` + G9; recon G11 (`11_agg_recon`).
7. Re-check gates / PK as needed.

Every step is the same idempotent SQL you already have — that is why the fingerprint proof matters.

**Optional manual drill:** regenerate or copy one day’s files back to landing, run `05→07→08→09→11` for that `ds`, then run `12_idempotency` again (or compare fingerprints in Snowsight).

---

### 12.5 Points to remember

- Idempotent ≠ “do nothing”; it means **end state matches** after a second apply.  
- Scope every reload with `_INGEST_DT` / `DAY_KY` (cheap + safe).  
- Sequences need `NOT EXISTS` / `MERGE` or re-runs mint new surrogate keys.  
- Soft-closed inventory: never `DELETE` history; same-day re-run updates, does not double current rows.  
- Archive keeps evidence; Bronze **replace** keeps “run twice = same counts” simple.

---

### 12.6 Exercises

1. **Playbook.** For `FCT_SALES` vs `TMP_SALES_LINE`, which idempotency technique does each use, and why different?
2. **Fingerprint.** Why is `_BATCH_ID` left out of the metrics?
3. **SCD2.** If you re-run dims for D with **no** attribute change, should `DIM_LOCN` current count rise? Why / why not?
4. **Reload policy.** In one sentence each: what happens to STG for D, and what happens to the bad file in archive?
5. **AGG.** Why delete-by-`DAY_KY` then re-insert instead of `MERGE` on the aggregate?

---

### 12.7 Checkpoint

Reply with:

- [x] `12_idempotency` green for a loaded day (e.g. `2026-11-13`)  
- [x] Log / note that BEFORE metrics equal AFTER (or paste the two `METRICS` objects)  
- [x] Answers to exercises 1–5  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| Fingerprint zeros / empty STG | That `ds` was never Bronze-loaded — pick a day you already ran |
| `FCT_INV_CURRENT_CNT` grows | Inventory same-day path should UPDATE; re-check `fct_inventory.sql` |
| `DIM_*_CURRENT_CNT` grows | SCD2 opening new versions without change — hash/`NOT EXISTS` bug |
| G8 fails inside a manual full re-run | Need a prior inventory day in STG; don’t use first pipeline day alone |
| DAG not listed | Wait for dag-processor (~30s); check import errors |

---

<a id="lesson-13"></a>

## Lesson 13: Five-day backfill evidence

| | |
|---|---|
| **Goal** | Load **five consecutive calendar days** (**10, 11, 12, 13, 14 Nov 2026**) end-to-end and prove every day is green with **G11 variance = 0** |
| **You build** | Extended `run_pipeline_days.ps1`, DAG `13_backfill_evidence` |
| **Status** | Done |
| **Not yet** | Final README + acceptance walkthrough (L14); optional single `medallion_daily` stitch |
| **Deferred** | L12 “reload corrected file for day D” drill — do it here or after L14 if you still want it |
| **Theory (optional)** | [2.4 Scheduling / backfill](THEORY.md#24-scheduling-logical-date-catchup-and-backfill) |

---

### 13.1 The idea in one picture

```
Five consecutive calendar days (no weekend / holiday skip)
  2026-11-10 → 11 → 12 → 13 → 14
       │
       │  max_active_runs = 1 conceptually
       │  (SCD2, soft-close, G8 need day order)
       ▼
For each ds (serial):
  05_bronze → 07_silver → 08_dims → 09_facts → 11_agg_recon

Then:
  13_backfill_evidence
    ├─ FCT + AGG present for all 5 DAY_KY
    └─ RECON: 5 rows, VARIANCE_AMT = 0  ← acceptance evidence table
```

**Dates in this project:** only **2026-11-10 … 2026-11-14**. There is no Nov 15/16 gap and no “Sat/Sun = holiday” rule.

**Backfill vs** `dags test`**:** Airflow 3’s `airflow backfill create --from-date 2026-11-10 --to-date 2026-11-14` schedules those five logical dates for **one** DAG id. This bootcamp still uses **layer DAGs**, so the practical backfill is serial `dags test` (script). Same idea: one logical date at a time, in order.

**Schedule framing:** daily cron `0 6 * `* * over that five-day window (not Mon–Fri-only).

---

### 13.2 Code walkthrough

#### a) Script — `scripts/run_pipeline_days.ps1`

Default `-Dates` are the five consecutive days ending **2026-11-14**. Each day runs Bronze → Silver → dims → facts → `11_agg_recon`.

| Switch | Effect |
|---|---|
| `-SkipGenerate` | Don’t regenerate landing CSVs |
| `-SkipAgg` | Stop after `09_gold_facts` |
| `-SkipProve` | Skip `10_prove_defects` |
| `-SkipEvidence` | Skip `13_backfill_evidence` |
| `-Dates ...` | Subset (e.g. only `2026-11-14`) |

#### b) Evidence DAG — `dags/13_backfill_evidence.py`

Expects `DAY_KY` in:

`20261110, 20261111, 20261112, 20261113, 20261114`

Two asserts:

1. Each of those five days has `FCT_SALES` and `AGG` (nets match).
2. `AUDIT.RECON_DAILY_NET_SALES` has those five days with `VARIANCE_AMT ≈ 0`.

---

### 13.3 Run it

#### Already have days 10–13 loaded?

1. Fill AGG for any earlier day you never ran `11_agg_recon` on (evidence requires AGG for all five):

```powershell
foreach ($d in @("2026-11-10","2026-11-11","2026-11-12","2026-11-13")) {
  docker compose run --rm airflow-cli airflow dags test 11_agg_recon $d
}
```

2. Generate + load the **fifth** consecutive day (**Nov 14**):

```powershell
.\scripts\run_pipeline_days.ps1 -Dates 2026-11-14 -SkipProve
```

(`generate_complex.py` runs first unless you pass `-SkipGenerate` after generating yourself. Regenerating writes landing files for **10–14**, not the old Nov 16 set.)

After Nov 14 facts+agg, recon covers **all** STG days still in Bronze (including 10–13).

#### Cold start (all five consecutive days)

```powershell
.\scripts\run_pipeline_days.ps1 -SkipProve
```

Runs generate (unless skipped) then 10 → 11 → 12 → 13 → 14. Long run; progress board prints between steps.

#### Evidence only (everything already loaded + recon refreshed)

```powershell
docker compose run --rm airflow-cli airflow dags test 13_backfill_evidence 2026-11-14
```

#### Snowsight peek (paste for checkpoint)

```sql
SELECT DAY_KY, VARIANCE_AMT
FROM MEDALLION_DB.AUDIT.RECON_DAILY_NET_SALES
WHERE DAY_KY IN (20261110, 20261111, 20261112, 20261113, 20261114)
ORDER BY DAY_KY;
```

---

### 13.4 Optional later: corrected-file reload (from L12)

When you want it:

1. Change one value in a day’s sales CSV (or regenerate and edit).
2. Drop corrected file into `data/landing/` (leave bad copy in archive).
3. Re-run `05 → 07 → 08 → 09 → 11` for that `ds` only.
4. Re-run `12_idempotency` or `13_backfill_evidence`.

---

### 13.5 Points to remember

- Pipeline window = **five consecutive calendar days** (10–14 Nov 2026).  
- **Logical date** = which calendar day the run owns; backfill = many logical dates in that range.  
- **Order matters** (SCD2, inventory, G8) → serial runs, `max_active_runs=1`.  
- Layer DAGs + script ≈ backfill until a single `medallion_daily` exists.  
- Evidence for the brief: **5-row** `DAY_KY | VARIANCE_AMT` with zeros.

---

### 13.6 Exercises

1. **Range.** Why does `from-date 2026-11-10` / `to-date 2026-11-14` with a **daily** schedule produce exactly five runs?
2. **Order.** What breaks if you load Nov 13 dims/facts **before** Nov 10?
3. **catchup vs backfill.** One sentence each: when would you use catchup, and when `backfill create`?
4. **Evidence.** What two things does `13_backfill_evidence` prove that G9 alone (one day) does not?

---

### 13.7 Checkpoint

Reply with:

- [x] All five consecutive dates (**10–14**) present in FCT (and AGG)  
- [x] `13_backfill_evidence` green **or** Snowsight paste of five `DAY_KY | VARIANCE_AMT = 0` rows  
- [x] Answers to exercises 1–4  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| Landing empty for Nov 14 | `python scripts\generate_complex.py` then Bronze |
| Still seeing Nov 16 files / days | Old generator output — re-run `generate_complex.py` (PIPELINE_DATES is 10–14 only) |
| Evidence: missing DAY_KY in RECON | Run `11_agg_recon` after the last day is in STG/FCT |
| Evidence: AGG empty for a day | Run `11_agg_recon` with that day’s `ds` (AGG is day-scoped) |
| G8 fails on Nov 14 | Prior inventory day must exist in STG (10–13 already loaded) |
| Script hangs on docker | Avoid redirecting compose output with `*>`; use the simple script as shipped |

---

<a id="lesson-14"></a>

## Lesson 14: Single-day DAG + Airflow 3 backfill (Option 1)

| | |
|---|---|
| **Goal** | One **ds-driven** daily DAG; load five consecutive days with **native backfill** (not five TaskGroups in one run) |
| **You build** | `14_generate_source` (simulator) + `14_medallion_daily` (pipeline) |
| **Status** | In progress |
| **Not this lesson** | Multi-day orchestrator / controller DAGs / asset chaining |
| **Theory** | [2.4 Scheduling & backfill](THEORY.md#24-scheduling-logical-date-catchup-and-backfill) · [6 Idempotency](THEORY.md#6-idempotency-playbook) · [5.4 Recon](THEORY.md#54-reconciliation-g11) |

---

### 14.1 Why Option 1

| Phrase in the brief | Meaning here |
|---|---|
| **Backfilled in one run** | One `airflow backfill create` job covers 10→14 Nov (five logical dates), each date is its own DagRun |
| **Safe to run twice** | Every layer is idempotent for the same `ds` — re-run one date; counts and nets do not change |

**Skip:** five-groups-per-day mega-DAG, controller DAGs, asset chaining.

```
14_medallion_daily (×N via backfill / Trigger)
──────────────────────────────────────────────
generate_landing_day → wait → bronze → silver → dims → facts → reconcile
(writes 4 CSVs for ds)  (sensor + mapped ingest for that ds)
```

Optional: `14_generate_source` still exists if you want landing files without running the full pipeline.

---

### 14.2 Shape of the daily DAG

```
generate_landing_day    Writes four CSVs for this logical date (ds)
        │                 defects = toordinal()%5; log → data/store/defect_log.txt
        ▼
wait_for_files          FileSensor.partial.expand ×4  (reschedule + timeout)
        │
        ▼
bronze                  create_stage → ingest_one.expand(source=SOURCES) → G1 LIST → archive
        │                 (PUT + G2 + DELETE/COPY FORCE per file)
        ▼
silver                  TMP loads; gates quarantine (run stays green)
        │
        ▼
gold_dims               SCD2 product ‖ SCD2 location; SCD1 customer; DIM_DAY seed
        │                 effective dates from {{ ds }}
        ▼
gold_facts              G7 quarantine+alert; FCT_SALES MERGE; inventory soft-close; FCT_PRICE
        │
        ▼
reconcile               AGG + G9; write RECON; assert G11 for this ds  ← only hard fail for variance
```

`max_active_runs=1` — backfill processes dates **chronologically**, never reverse (SCD2 + closed inventory).

---

### 14.3 Design checklist (mapped to this repo)

| Requirement | How we do it |
|---|---|
| **File sensor** | `wait_for_files` — four paths `{source}_{{ ds_nodash }}.csv`, `mode=reschedule`, timeout from config. Files already land from the generator during backfill → sensor passes fast. Delete one file to demo timeout. |
| **Dynamic mapping** | `ingest_one.expand(source=SOURCES)` — one mapped Bronze task per file from config, not copy-pasted tasks. |
| **Positional load** | Inventory has **no header**; Bronze `stg_inventory.sql` uses transforming `COPY` with `$1..$8` (positions fixed in SQL + `SOURCE_CONTRACTS` in config). Not a single RAW + `SUBSTR` redesign — same contract as L3/L5. |
| **SCD2** | Product + location **hierarchies** (`DIM_ITEM`, `DIM_LOCN`, …): close-then-insert / hash compare; surrogate `*_KY` from **sequences**; `EFFECTIVE_*` from **`{{ ds }}`**, not `CURRENT_DATE`. |
| **SCD1** | `DIM_CUSTOMER` (+ `ITEM_DESC` in-place): `MERGE … WHEN MATCHED THEN UPDATE` — visibly different from SCD2. |
| **FCT_INVENTORY** | Soft-close: set end date / `CURRENT_FLG`; **never DELETE**. |
| **Quarantine** | Silver (+ G7) write `TMP_QUARANTINE` with `GATE_ID` / `REASON_CD`; **do not raise**. Calendar defect buckets plant issues on matching days; DagRun stays green until reconcile. |
| **Reconciliation** | Last group: AGG, G9, G11. **Fails** if `VARIANCE_AMT ≠ 0` for this `ds`. |

#### Three decisions that matter

**1. Idempotency is a rule for every layer**  
Bronze: `DELETE WHERE _INGEST_DT = ds` + `COPY FORCE`. Silver TMP: delete day / overwrite hier. Facts: `MERGE` / soft-close. AGG: delete+recompute `DAY_KY`. Prove it: run one `ds` twice; compare STG/FCT counts and nets.

**2. Recon must include quarantine**

```
source file net sales  =  Gold (FCT) net sales  +  quarantined net sales
```

Otherwise day-3-style defects fail a correct Gold. Also check **row** identity on the sales path (`STG ≈ FCT + sales quarantine`), not only the sum. Implemented in `assert_recon_day` + Silver `assert_sales_identity`.

**3. Backfill settings**  
`max_active_runs=1`, chronological `from-date` → `to-date`. Always use the run’s **logical date** (`{{ ds }}`). (We avoid `depends_on_past` on the first backfill day — it would wait for dates that never ran; order comes from `max_active_runs=1` instead.)

---

### 14.4 Generator (first task of the daily DAG)

`generate_landing_day` is **step 1** of `14_medallion_daily`:

- Writes **this `ds` only** into landing (safe to re-run after archive).  
- Transaction defects from `date.toordinal() % 5` (exclusive): `0` outliers/anomalies · `1` null ITEM_ID ×2 · `2` neg qty · `3` orphan `199999` · `4` duplicate line. Any 5 consecutive days cover every defect.  
- SCD2 city move on store `1008` from **2026-11-13 onward** (`SCD2_FROM`, not a % bucket).  
- SCD1 description change on item `100011` ("(rev Nov)") from **2026-11-14** (`SCD1_FROM`).  
- Stock is replayed from `STOCK_ANCHOR` (2026-11-10), so a day's files are identical whether generated alone or inside a window.  
- Appends each planted defect (with **store** id) to `data/store/defect_log.txt`.  
- Sensor still runs afterward (passes when files exist; delete one file to demo timeout).

For Nov 2026: 10→outliers, 11→null ITEM_ID, 12→neg qty, 13→orphan (+ SCD2 starts), 14→dup (+ SCD1 starts). 20 Nov (today) → outliers.

Optional standalone: `14_generate_source` if you only want CSVs without the pipeline.

---

### 14.5 Run the five days (native backfill)

#### Wipe history (clean restart)

UI: open the DAG → **⋯** / delete options on each run, or delete the DAG from the DAGs list (the `.py` file stays; Airflow re-imports an empty history).

CLI (fast wipe of every run + TI):

```powershell
docker compose run --rm airflow-cli airflow dags delete 14_medallion_daily -y
```

Wait ~30s for the DAG to reappear. It starts **paused** — unpause only when you start a backfill or manual triggers.

**Unpausing always creates today's run.** With `@daily` + `catchup=False` the timetable schedules the most recent tick (today 00:00 UTC) the moment the DAG is unpaused; `run_immediately` cannot stop it because the DAG has a `start_date`. Backfill tasks need the DAG unpaused, so accept it: let today's run finish, reset Snowflake only (`run_queries.py reset --yes`), backfill 10–14, then **Clear** today's run so it reloads after 14 Nov. The next automatic run is tomorrow 00:00 UTC (05:45 Nepal time) — pause before then if not wanted.

#### A) Backfill from the UI (preferred)

Airflow 3 needs a **periodic** schedule (`@daily`) with `catchup=False`. Generate + load follow the run’s **logical date** (any calendar day).

1. Scheduler up (`docker compose up -d`).
2. Open **`14_medallion_daily`** → **unpause**.
3. Open **Backfill** (DAG page / Browse → Backfills) → create:
   - From: `2026-11-10`
   - To: `2026-11-14`
   - Max active runs: `1`
4. Watch **Grid**: chronological greens; check `data/store/defect_log.txt` for store-tagged lines.

#### A2) Backfill from CLI (same job)

```powershell
docker compose run --rm airflow-cli airflow backfill create `
  --dag-id 14_medallion_daily `
  --from-date 2026-11-10 `
  --to-date 2026-11-14 `
  --max-active-runs 1
```

#### A3) Manual Trigger in the UI (one day at a time)

**Trigger** → set **logical date** to `2026-11-10`, run, wait for green → repeat for 11, 12, 13, 14 **in order** (`max_active_runs=1`). Do **not** leave logical date as “now”.

#### B) Without scheduler — serial `dags test` (same idea)

```powershell
foreach ($d in @("2026-11-10","2026-11-11","2026-11-12","2026-11-13","2026-11-14")) {
  docker compose run --rm airflow-cli airflow dags test 14_medallion_daily $d
}
```

#### C) Idempotency proof (the latest loaded date, twice)

Re-run the **latest** loaded day (after the 10–14 backfill: `2026-11-14`). Clear its run in the UI, or `dags test` it. Counts and nets must match (`generate_landing_day` recreates that day's landing files before the second run). For the evidence pack use `evidence/run_queries.py snapshot --ds 2026-11-14 --label before|after` around the re-run.

**Never re-run an older day alone after newer days are loaded.** SCD2 (`DIM_LOCN`) and the inventory/price soft-close are order-dependent: re-running 12 Nov after 14 Nov reopens Spokane for store 1008 and leaves extra "current" inventory/price rows (seen 20 Nov: 1501 → 1561). To correct a past day, replay forward from it in order (backfill `D … latest` with reprocess).

---

### 14.6 No hard-coding

| What | Where |
|---|---|
| Credentials | Connection `snowflake_medallion` |
| DB / schemas / paths | Variable `medallion_config` |
| File list / contracts | `SOURCES`, `SOURCE_CONTRACTS` in `include/medallion/config.py` |
| SQL | `include/sql/**` templated with `{{ ds }}` / `{{ ds_nodash }}` |

Self-check:

```powershell
Select-String -Path dags\14_*.py -Pattern "MEDALLION_DB|STG_SALES|GOLD\." 
## expect no literal Snowflake object names in the DAG files
```

---

### 14.7 Evidence for the walkthrough

| Evidence | Where |
|---|---|
| Grid: five green backfill runs | Airflow UI → `14_medallion_daily` |
| Run log (date / rows) | Task logs: `assert_sales_identity`, `assert_recon_day` print STG/TMP/QUAR and nets |
| Quarantine defects | `SELECT * FROM …TMP_QUARANTINE WHERE INGEST_DT IN (…) ORDER BY 1,4` |
| Recon table | `SELECT DAY_KY, SOURCE_NET_AMT, QUARANTINE_NET_AMT, FCT_NET_AMT, VARIANCE_AMT FROM …RECON_DAILY_NET_SALES ORDER BY 1` |
| Rerun proof | Same peeks before/after second run of one `ds` |

```sql
-- recon (expect VARIANCE_AMT = 0)
SELECT DAY_KY, SOURCE_NET_AMT, QUARANTINE_NET_AMT, FCT_NET_AMT, VARIANCE_AMT
FROM MEDALLION_DB.AUDIT.RECON_DAILY_NET_SALES
WHERE DAY_KY BETWEEN 20261110 AND 20261114
ORDER BY 1;

-- quarantine (defects on window days 2–3 / classic Nov 11–12)
SELECT INGEST_DT, GATE_ID, REASON_CD, OBJECT_NM, COUNT(*)
FROM MEDALLION_DB.SILVER.TMP_QUARANTINE
WHERE INGEST_DT BETWEEN '2026-11-10' AND '2026-11-14'
GROUP BY 1,2,3,4
ORDER BY 1,2;
```

---

### 14.8 Code map

| File | Role |
|---|---|
| `dags/14_medallion_daily.py` | Single-day stitch (`generate_landing_day` → sensor → … → recon) |
| `dags/14_generate_source.py` | Optional simulator-only (same generate, no pipeline) |
| `include/medallion/runner.py` | Shared SQL render + one-session multi-statement (temps) |
| `include/sql/**` | Unchanged lesson SQL (`{{ ds }}`) |

---

### 14.9 Checkpoint

Reply with:

- [ ] Five green runs of `14_medallion_daily` (backfill **or** serial `dags test`)  
- [ ] Recon paste: five rows, `VARIANCE_AMT = 0`  
- [ ] One date re-run: counts/nets unchanged  
- [ ] One sentence: why `max_active_runs=1` matters for SCD2 / inventory  

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| Sensor timeout | `generate_landing_day` failed, or a scheduled run for "today" fired while you expected another date; check the run's logical date and the task log |
| `TMP_INV_D` missing | Multi-statement file must share one session (`split_statements=True` / runner) |
| G11 variance ≠ 0 | Quarantine net not subtracted, or facts not loaded for that day |
| Backfill out of order / dim weirdness | Ensure `--max-active-runs 1` and from-date ≤ to-date chronological |
| Import `generate_complex` | `scripts/` volume mounted; `docker compose up -d` |
| DAG not listed | `airflow dags list-import-errors` |

---

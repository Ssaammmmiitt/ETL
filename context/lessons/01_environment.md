# Lesson 1: Environment, connections and variables

**Goal:** a running Airflow 3.3.2 stack that can log in to Snowflake as a service user, read
project config from a Variable, and pass a test that proves no DAG hard-codes secrets,
paths or table names.

**Data level:** none yet. We prove the plumbing before moving any data.

**Read first in [THEORY.md](../THEORY.md):**
[1 Medallion architecture](../THEORY.md#1-medallion-architecture-stg--tmp--tgt--aggregate),
[2.1 Components](../THEORY.md#21-components),
[2.3 Parse time vs run time](../THEORY.md#23-parse-time-vs-run-time),
[2.6 Connections, variables, templating](../THEORY.md#26-connections-variables-templating-and-the-fernet-key),
[3.1 Warehouses and cost](../THEORY.md#31-architecture-warehouses-and-cost),
[3.2 Access control and key pairs](../THEORY.md#32-access-control-service-users-and-key-pair-auth),
[8 Docker essentials](../THEORY.md#8-docker-essentials).
Section 1.1 below applies them to this project.

---

## 1.1 Concepts

### What is running in Docker Compose

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

### Where configuration lives

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

### Two ways to read config, and when each is right

```python
# Rendered at run time by Jinja, inside a templated field (sql=, bash_command=, ...)
"SELECT * FROM {{ var.json.medallion_config.database }}.BRONZE.STG_SALES"

# Called at run time inside a @task body
cfg = Variable.get("medallion_config", deserialize_json=True)
```

**Anti-pattern:** calling `Variable.get()` at module top level. That runs on every parse
(every ~30 s, forever) and hits the metadata DB each time.

### The Fernet key

Connection passwords and extras are encrypted in Postgres with the `FERNET_KEY`. If it is
empty, they are stored in **plain text**. Set it **before** creating connections. If you
change it later, existing connections can no longer be decrypted.

### Why key-pair auth for Snowflake

`AIRFLOW_SVC` is created with `TYPE = SERVICE`, which means:

- It cannot log in with a password or open Snowsight.
- It is exempt from MFA, which matters because an unattended scheduler can't answer a prompt.
- It authenticates by signing a token with a **private key** that only Airflow has.
Snowflake holds only the **public key**.

The private key is a file in `secrets/`, mounted read-only into the containers at
`/opt/airflow/secrets/`. The connection stores only the path to it.

---

## 1.2 Files created for this lesson

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

## 1.3 Generate the Fernet key

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

## 1.4 Create the Snowflake key pair

Git for Windows ships `openssl`. Run in PowerShell from the project folder:

```powershell
$openssl = "$env:LOCALAPPDATA\Programs\Git\usr\bin\openssl.exe"
& $openssl genrsa -out secrets\rsa_key.pem 2048
& $openssl pkcs8 -topk8 -inform PEM -in secrets\rsa_key.pem -out secrets\rsa_key.p8 -nocrypt
& $openssl rsa -in secrets\rsa_key.p8 -pubout -out secrets\rsa_key.pub
Remove-Item secrets\rsa_key.pem

# One-line public key body, ready to paste into Snowflake:
(Get-Content secrets\rsa_key.pub | Where-Object { $_ -notmatch '-----' }) -join ''
```

**Why** `-nocrypt`**:** keeps the lesson simple. In production you would encrypt the private
key with a passphrase and put the passphrase in the connection's password field.

**Observe:** `secrets\` now has `rsa_key.p8` (private, starts with `-----BEGIN PRIVATE KEY-----`)
and `rsa_key.pub`. Both are git-ignored.

---

## 1.5 Bootstrap Snowflake

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

## 1.6 Fill in the connections file

```powershell
Copy-Item include\config\connections.example.json secrets\connections.json
notepad secrets\connections.json
```

Replace `<ORGNAME>-<ACCOUNTNAME>` with the identifier from 1.5 (for example `ABCDEFG-XY12345`).
Use the identifier, **not** the full `...snowflakecomputing.com` URL.

`secrets/connections.json` is git-ignored. It holds an account name and a key **path**,
never the key itself.

---

## 1.7 Start Airflow

The `day3-airflow` stack also uses port 8080, so stop it first if it is running.

```powershell
# Only if day3-airflow is running:
docker compose -f ..\day3-airflow\docker-compose.yaml down

docker compose up airflow-init      # migrates the DB, creates user airflow/airflow; ends with exit 0
docker compose up -d
docker compose ps
```

**Observe:** after 1–2 minutes every service shows `(healthy)`. The UI is at
<http://localhost:8080> (login `airflow` / `airflow`).

---

## 1.8 Import the connections and the Variable

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

## 1.9 Run the smoke-test DAG

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

## 1.10 Two quick checks

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
[THEORY 1.1](../THEORY.md#11-naming-convention)) belong in SQL files or the Variable. Re-run it whenever you add a DAG.

---

## 1.11 Exercises

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

## 1.12 Checkpoint

Reply with:

- [x] `docker compose ps`: every service `healthy`
- [x] `whoami` result row (the six values)
- [x] `check_schemas` log line
- [x] Checks in 1.10: `No data found` and no `Select-String` output
- [x] Answers to exercises 3 and 4

## Troubleshooting

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

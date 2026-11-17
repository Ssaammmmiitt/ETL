# Assumptions and decisions

Single place for what this project assumes and why. Update this file when a decision changes.
Detail lives in [THEORY.md](THEORY.md); the lesson plan is in [LESSONS.md](LESSONS.md); the
target picture is [architecture-Simple view.png](architecture-Simple%20view.png).

---

## Environment

| Assumption | Why |
|---|---|
| Airflow **3.3.2** in Docker Compose (official image) | Matches "2.7+"; Snowflake provider already included, no custom image |
| Learner on **Windows + PowerShell**; SQL run in **Snowsight** | No Astro CLI / SnowSQL required |
| Snowflake trial with **ACCOUNTADMIN** | Enough to create warehouse, role, service user, stages |
| Auth = **key pair**, user `AIRFLOW_SVC` (`TYPE = SERVICE`) | No password, no MFA for an unattended scheduler |
| Port **8080** free (stop `day3-airflow` if needed) | Same default UI port |

---

## Scope and simplicity

| Assumption | Why |
|---|---|
| Only features the brief and the diagram need; plain readable code | Learn more with fewer tools |
| No `tests/` / pytest | Gates and reconciliation are SQL tasks in the DAG; plus `list-import-errors` and one `Select-String` line |
| No secrets, paths, or Snowflake object names in DAG files | Connections + Variable + Jinja; SQL files hold table names |

---

## Architecture (diagram = source of truth for names)

| Table type | Layer / schema | Promise | Tables |
|---|---|---|---|
| **STG** | Bronze / `BRONZE` | Land as-is | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | Silver / `SILVER` | Typed, deduped, compared to TGT; rejects to quarantine | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER`, `TMP_QUARANTINE` |
| **TGT** | Gold / `GOLD` | Star schema, SCD rules, FKs resolve | 10 `DIM_*` + `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` |
| **Aggregate** | Gold / `GOLD` | Totals tie to `FCT_SALES` | `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES` |
| — | `AUDIT` | Gate results and reconciliation | `RECON_DAILY_NET_SALES` (L11) |

- **Source files** keep their names: `sales_`, `inventory_`, `item_master_`, `store_master_YYYYMMDD.csv`.
- **STG lineage:** `_INGEST_DT`, `_SOURCE_FILE`, `_SOURCE_ROW_NUM`, `_INGEST_TS`, `_BATCH_ID`.
- **Archive:** after a successful STG load, files move `data/landing/` → `data/archive/`.
- **Stage:** named internal `@my_int_stage`; external stage written and explained only (no bucket).

---

## Dates and schedule

| Assumption | Why |
|---|---|
| Five business days: **10, 11, 12, 13, 16 Nov 2026** | Weekdays in 10–16 Nov |
| Schedule `0 6 * * 1-5`; day **D** = `{{ ds }}` | One run per weekday; data is as of D+1 in spirit |
| `_INGEST_DT` = D; `_BATCH_ID` = `{{ run_id }}` | Day to delete / reload by, and which run did it |
| `max_active_runs = 1` | SCD2, soft-close, price periods and G8 depend on the previous day |

---

## Data types (diagram's choices)

| Type | Where | Why (short) |
|---|---|---|
| `VARCHAR` | Every STG business column | Text never rejects a row; TMP casts |
| `VARCHAR(n)` | TMP / TGT text (ids 50) | Documents the contract; same storage cost |
| `NUMBER(18,2)` | Money (`*_AMT`) | Exact. **Never `FLOAT`**: recon needs variance = 0 |
| `NUMBER(18,3)` | Quantities (`*_QTY`) | Weighed fresh items (`0.750`) |
| `NUMBER(38,0)` | Surrogate keys `*_KY`, counts | Whole numbers, never run out |
| `NUMBER(8,0)` | `DAY_KY` | `YYYYMMDD`, readable and joins like a number |
| `DATE` / `TIMESTAMP_NTZ` | Business and effective dates / load and audit times | Day only / clock time without time-zone noise |
| `BOOLEAN` | `*_FLG` | Files send `Y`/`N`; no ambiguity after TMP |
| `VARIANT` | `TMP_QUARANTINE.RAW_RECORD` | Whole rejected row, any shape |

Full table: [THEORY 3.10](THEORY.md#sec-3-10-data-types).

---

## Quality gates (diagram G1–G10 + G11)

| Gate | Boundary | Rule | Action |
|---|---|---|---|
| G1 | Source → STG | File by SLA; row count | alert, then fail |
| G2 | Source → STG | Header / column count | quarantine file |
| G3–G6 | STG → TMP | Null keys, duplicates, date window, amounts/qty | quarantine row (G6b alert) |
| G7 | TMP → TGT | Keys resolve | quarantine + alert |
| G8 | TMP → TGT | Inventory ±20% of prior day | fail |
| G9 | TGT → AGG | AGG = FCT_SALES per day | fail |
| G10 | TGT | No price-period overlap | fail |
| G11 | TGT → Source | Gold + quarantined net = file net | fail + alert |
| G12 | STG → TMP | Outlier limits from `outlier_rules` (price ratio 0.1–10× reference, qty ≤ 1,000, line net ≤ 50,000, on hand ≤ 100,000) | quarantine + alert |

- Quarantine **never** silently drops a row; it stores `GATE_ID` and `REASON_CD`.
- Identity per day: **STG rows = TMP rows + quarantined rows**.
- **Outlier ≠ defect.** Valid extremes (2,499.99 item, 480 units, 25,000 stock) must reach Gold.
  Anomalies (price ×1000, 10,000 units, price ÷100, 9,999,999 stock) are caught by G12 using a
  reference price (today's inventory price) and limits that change by Variable import, not code.

---

## Modelling and load patterns

| Topic | Decision | Why |
|---|---|---|
| Waiting | **FileSensor** (mapped ×4, `reschedule`); Assets taught | Files come from outside Airflow, per date, with a timeout |
| Inventory file | No header → **positional** `COPY` (`$1..$8`) | Brief's positional-load requirement |
| Idempotent day | `DELETE WHERE _INGEST_DT = D` then reload | Re-run same day → same counts |
| Hierarchy dims | Snowflaked, SCD2, parents first | As on the diagram |
| `DIM_ITEM.ITEM_DESC` | SCD1 | Brief needs an SCD1 item attribute |
| `DIM_CUSTOMER` | SCD1 `MERGE` | As on the diagram |
| `DIM_DAY` | Seed straight into Gold | The diagram's one departure |
| `FCT_SALES` | `MERGE` on `(TRANSACTION_ID, LINE_NUM)`, clustered by `DAY_KY` | Reload policy step 3 |
| `FCT_INVENTORY` | Soft-close, never delete | Brief requirement (overrides the diagram's delete + insert) |
| `FCT_PRICE` | Periods derived from inventory prices | As on the diagram |
| Orphans | G7 quarantine + alert; recon adds quarantined back | Gold stays clean and still ties |
| Surrogate keys | Teach sequence vs `HASH`; pick in L8 | Re-run behaviour differs |

---

## Departures (full list: [THEORY 1.4](THEORY.md#sec-1-4-departures))

1. `DIM_DAY` seeded into Gold (diagram's own departure).
2. `ITEM_DESC` SCD1 inside an SCD2 `DIM_ITEM` (brief).
3. `FCT_INVENTORY` soft-close (brief).
4. Silver = Snowflake transient tables, not Parquet.
5. Corrected file replaces day D in STG; bad file kept in `data/archive/`.
6. Extra gate G11 = the brief's reconciliation.
7. Brief "district change" = store moves city.
8. Extra gate G12: configurable outlier limits.
9. No `tests/`; external stage documented only; `@task.snowflake` doesn't exist.

---

## Do not change lightly

- `FERNET_KEY` after connections are imported (they become unreadable).
- Table and lineage column names once loads exist (needs `ALTER` / migration).
- `_INGEST_DT` meaning (day D) once STG has data.

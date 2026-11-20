# airflow-medallion

Airflow 3.3.2 + Snowflake medallion pipeline for retail files. One DAG (`retail_medallion_daily`) lands four daily CSVs, moves them through Bronze → Silver → Gold, enforces quality gates, and reconciles Gold net sales back to source.

> ***Evidence pack: [evidence/EVIDENCE.md](evidence/EVIDENCE.md) - screenshots of a full proof run: fresh start, five-day backfill, daily load, idempotent rerun, and reload of one past day.***

Course notes: [THEORY.md](context/THEORY.md) · [LESSONS.md](context/LESSONS.md) · [runbook](context/runbook.md)

![Architecture, simple view](context/architecture-Simple%20view.png)

**Data path:** Source files → Bronze (STG) → Silver (TMP) → Gold TGT → Gold Aggregate → Consumers

### DAG structure (`retail_medallion_daily`)

One run = one business date `ds`. Top-level chain, then task groups. Every task attempt writes to `AUDIT.C_BATCH_LOG`.

```text
setup_platform
load_c_batch_scripts → load_c_param
generate_landing_day → wait_for_files ×4
│
├─ bronze
│    ingest_one ×4 → assert_stage_g1 → archive_landing
│
├─ silver
│    load_tmp_sales_line
│         ├─ load_tmp_customer
│         └─ load_tmp_item_loc_snapshot → g06b_alert
│    → load_tmp_item_hier → load_tmp_locn_hier → assert_sales_identity
│
├─ gold_dims
│    rewind_gold_for_reload → put_seed_dim_day → load_dim_day
│         ├─ load_dim_department → category → subcategory → item
│         └─ load_dim_division → region → city → locn
│    → load_dim_customer
│
├─ gold_facts
│    g07_keys_resolve
│         ├─ g07_alert → load_fct_sales
│         ├─ g08_inventory_count → load_fct_inventory
│         └─ load_fct_price → g10_price_overlap
│
└─ reconcile
     load_agg_daily_item_locn_sales → load_recon_daily_checks
     → assert_reconciliation → assert_primary_keys
load_c_load_dates
```

| Group / stage | Tasks (task_id) | Role |
|---|---|---|
| Control (start) | `setup_platform`, `load_c_batch_scripts`, `load_c_param` | DDL if missing; script registry; run parameters |
| Source | `generate_landing_day`, `wait_for_files` ×4 | Create CSVs for `ds`; G1 arrival sensor |
| `bronze` | `ingest_one` ×4, `assert_stage_g1`, `archive_landing` | PUT + G2 + `STG_*`; stage list; archive files |
| `silver` | `load_tmp_sales_line`, `load_tmp_customer`, `load_tmp_item_loc_snapshot`, `g06b_alert`, `load_tmp_item_hier`, `load_tmp_locn_hier`, `assert_sales_identity` | Clean TMP tables; G3-G6/G12 reject to `C_REJ_TABLE` |
| `gold_dims` | `rewind_gold_for_reload`, `put_seed_dim_day`, `load_dim_day`, product dims, location dims, `load_dim_customer` | Past-day rewind; seed calendar; SCD2/SCD1 |
| `gold_facts` | `g07_keys_resolve`, `g07_alert`, `load_fct_sales`, `g08_inventory_count`, `load_fct_inventory`, `load_fct_price`, `g10_price_overlap` | Orphans; sales MERGE; inventory soft-close; price periods |
| `reconcile` | `load_agg_daily_item_locn_sales`, `load_recon_daily_checks`, `assert_reconciliation`, `assert_primary_keys` | AGG; G9 + five G11 checks; PK uniqueness |
| Control (end) | `load_c_load_dates` | High-water mark per target (only on a green run) |

| Table type | Schema | Promise | Tables |
|---|---|---|---|
| **STG** | `BRONZE` | Land as-is | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | `SILVER` | Typed, deduped; rejects to quarantine | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER` |
| **TGT** | `GOLD` | Star schema, SCD rules, FKs resolve | 10 `DIM_*` + `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` |
| **Aggregate** | `GOLD` | Totals tie to `FCT_SALES` | `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES` |
| - | `AUDIT` | Gates, rejects, recon, control | `C_REJ_TABLE`, `RECON_DAILY_CHECKS`, `C_BATCH_*`, `C_LOAD_DATES`, `C_PARAM` |

Source files keep their names (`sales_`, `inventory_`, `item_master_`, `store_master_YYYYMMDD.csv`). After a successful STG load they move `data/landing/` → `data/archive/`. Stage is internal `@my_int_stage` only.

### What each layer guarantees

- **STG** - byte-for-byte landing with lineage (`_INGEST_DT`, `_SOURCE_FILE`, `_SOURCE_ROW_NUM`, `_INGEST_TS`, `_BATCH_ID`). No types, keys, or dedup promised.
- **TMP** - typed and deduplicated; bad rows go to `AUDIT.C_REJ_TABLE` (never silently dropped). No surrogates or SCD history.
- **TGT** - dimensions (product/location SCD2, customer SCD1, `DIM_DAY` seeded) then facts. Data as of D+1.
- **Aggregate** - rolled-up marts that reconcile to `FCT_SALES`; consumers read Aggregate for reporting and TGT for detail.

### Quality gates (G1-G12)

| Gate | Boundary | Rule | On fail |
|---|---|---|---|
| G1-G2 | Source → STG | 4 files on time / non-empty; contracted column count | fail |
| G3-G6, G12 | STG → TMP | null keys, dup lines (keep latest), date window, qty/net, outliers | reject → `C_REJ_TABLE` |
| G6b | STG → TMP | reserved > on hand | alert (rows still load) |
| G7 | TMP → TGT | item / location resolve to a current dim key | reject + alert |
| G8, G10 | TMP → TGT | inventory count ±20% vs prior day; no price overlaps | fail |
| G9, G11, PK | TGT / Aggregate | AGG = `FCT_SALES`; five recon checks PASS; no duplicate Gold keys | fail |

Rejects stay in `AUDIT.C_REJ_TABLE` (rule + reason + full row); the run stays green. Per day: STG rows = TMP rows + rejected rows. Outliers ≠ structural defects - valid extremes still reach Gold. G11: Gold net + quarantined net = file net.

---

## Assumptions

**Environment:** Airflow 3.3.2 in Docker Compose; Windows + PowerShell; SQL in Snowsight; Snowflake key-pair auth as service user `AIRFLOW_SVC`; UI on port 8080.

**Scope:** Implements the architecture diagram and acceptance gates. Secrets and Snowflake object names stay in Connections / Variable / SQL files - not in DAG Python. Gates and reconciliation are SQL tasks in the DAG.

**Dates:** Evidence window **10-14 Nov 2026** (plus 20 Nov daily run); schedule `@daily` (00:00 UTC); day D = `{{ ds }}`; `_INGEST_DT` = D; `_BATCH_ID` = `{{ run_id }}`; `max_active_runs = 1` (SCD2 / soft-close / price periods depend on the prior day). ~810 sales lines/day; sales items ⊆ store stock except one planted orphan.

**Planted defects**

| # | Defect | Gate / effect |
|---|---|---|
| D1 | Null `ITEM_ID` (2 rows) | G3 quarantine |
| D2 | Negative `SOLD_QTY` | G6 quarantine |
| D3 | Sale for missing item | G7 quarantine + alert |
| D4 | Duplicate `(TRANSACTION_ID, LINE_NUM)` | G4 keep latest |
| D5 | Store city move (`LOCN_ID=1008`) | SCD2 on `DIM_LOCN` |

**Load patterns:** FileSensor wait; inventory via positional `COPY`; idempotent day = `DELETE WHERE _INGEST_DT = D` then reload; hierarchy dims SCD2 parents-first; `DIM_CUSTOMER` / `ITEM_DESC` SCD1; `FCT_SALES` MERGE; `FCT_INVENTORY` soft-close; `FCT_PRICE` from inventory price changes.

**Notable departures:** `DIM_DAY` seeded straight into Gold; Silver = Snowflake tables (not Parquet); soft-close inventory; extra gates G11/G12; internal stage only (no cloud bucket).

**Do not change lightly:** `FERNET_KEY` after connections are imported; table / lineage names once loads exist; `_INGEST_DT` meaning once STG has data.

---

## How to run

1. Configure Fernet key, Snowflake key-pair user (`AIRFLOW_SVC`), connection `snowflake_medallion`, and Variable `medallion_config`.
2. From the project root: `docker compose up -d`
3. Open http://localhost:8080 (default `airflow` / `airflow`). Set the UI timezone to **UTC**.
4. Follow [context/runbook.md](context/runbook.md) for scheduled runs, single-day trigger / `dags test`, backfill (UI or CLI), idempotent rerun, and past-day reload.

Offline checks (no Snowflake writes):

```powershell
docker compose run --rm airflow-cli python /opt/airflow/tests/run_all.py
```

Snowsight queries: `evidence/snowsight/`.

## Limitations

- Demo scale only (~810 sales lines/day); not sized for production volume or SLAs.
- Sources are generated in-DAG; landing is a local mount plus an internal Snowflake stage (no external producer or cloud stage).
- `max_active_runs = 1` and chronological days are required; fixing a past day needs a Gold rewind then forward replay.
- Alerts are task-log lines only (no email or tickets); `C_BATCH_LOG` writes are best-effort and never fail the run.

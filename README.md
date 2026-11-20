# airflow-medallion

Airflow 3.3.2 + Snowflake medallion pipeline for retail files. One DAG (`retail_medallion_daily`) lands four daily CSVs, moves them through Bronze → Silver → Gold, enforces quality gates, and reconciles Gold net sales back to source.

![Architecture, simple view](context/architecture-Simple%20view.png)

**Flow:** Source files → Bronze (STG) → Silver (TMP) → Gold TGT → Gold Aggregate → Consumers

| Table type | Schema | Promise | Tables |
|---|---|---|---|
| **STG** | `BRONZE` | Land as-is | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | `SILVER` | Typed, deduped; rejects to quarantine | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER` |
| **TGT** | `GOLD` | Star schema, SCD rules, FKs resolve | 10 `DIM_*` + `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` |
| **Aggregate** | `GOLD` | Totals tie to `FCT_SALES` | `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES` |
| — | `AUDIT` | Gates, rejects, recon, control | `C_REJ_TABLE`, `RECON_DAILY_CHECKS`, `C_BATCH_*`, `C_LOAD_DATES`, `C_PARAM` |

Source files keep their names (`sales_`, `inventory_`, `item_master_`, `store_master_YYYYMMDD.csv`). After a successful STG load they move `data/landing/` → `data/archive/`. Stage is internal `@my_int_stage` only.

### What each layer guarantees

- **STG** — byte-for-byte landing with lineage (`_INGEST_DT`, `_SOURCE_FILE`, `_SOURCE_ROW_NUM`, `_INGEST_TS`, `_BATCH_ID`). No types, keys, or dedup promised.
- **TMP** — typed and deduplicated; bad rows go to `AUDIT.C_REJ_TABLE` (never silently dropped). No surrogates or SCD history.
- **TGT** — dimensions (product/location SCD2, customer SCD1, `DIM_DAY` seeded) then facts. Data as of D+1.
- **Aggregate** — rolled-up marts that reconcile to `FCT_SALES`; consumers read Aggregate for reporting and TGT for detail.

---

## Assumptions

**Environment:** Airflow 3.3.2 in Docker Compose; Windows + PowerShell; SQL in Snowsight; Snowflake key-pair auth as service user `AIRFLOW_SVC`; UI on port 8080.

**Scope:** Only what the brief and diagram need. Secrets and Snowflake object names stay in Connections / Variable / SQL files — not in DAG Python. Gates and reconciliation are SQL tasks in the DAG.

**Dates:** Five consecutive days **10–14 Nov 2026**; schedule `0 6 * * *`; day D = `{{ ds }}`; `_INGEST_DT` = D; `_BATCH_ID` = `{{ run_id }}`; `max_active_runs = 1` (SCD2 / soft-close / price periods depend on the prior day). ~810 sales lines/day; sales items ⊆ store stock except one planted orphan.

**Planted defects**

| # | Defect | Gate / effect |
|---|---|---|
| D1 | Null `ITEM_ID` (2 rows) | G3 quarantine |
| D2 | Negative `SOLD_QTY` | G6 quarantine |
| D3 | Sale for missing item | G7 quarantine + alert |
| D4 | Duplicate `(TRANSACTION_ID, LINE_NUM)` | G4 keep latest |
| D5 | Store city move (`LOCN_ID=1008`) | SCD2 on `DIM_LOCN` |

**Quality gates (G1–G12):** file SLA and shape into STG; nulls, duplicates, date window, amounts, and outliers into TMP; key resolution, inventory swing, price overlap into TGT; AGG = fact; Gold + quarantined net = file net (G11). Identity per day: STG rows = TMP rows + rejected rows. Outliers ≠ structural defects — valid extremes still reach Gold.

**Load patterns:** FileSensor wait; inventory via positional `COPY`; idempotent day = `DELETE WHERE _INGEST_DT = D` then reload; hierarchy dims SCD2 parents-first; `DIM_CUSTOMER` / `ITEM_DESC` SCD1; `FCT_SALES` MERGE; `FCT_INVENTORY` soft-close; `FCT_PRICE` from inventory price changes.

**Notable departures:** `DIM_DAY` seeded straight into Gold; Silver = Snowflake tables (not Parquet); soft-close inventory; extra gates G11/G12; internal stage only (no cloud bucket).

**Do not change lightly:** `FERNET_KEY` after connections are imported; table / lineage names once loads exist; `_INGEST_DT` meaning once STG has data.

---

## How to run

1. Set Fernet key, Snowflake key pair + bootstrap, Airflow connection and Variable.
2. `docker compose up`
3. Trigger or backfill `retail_medallion_daily` (UI at http://localhost:8080).

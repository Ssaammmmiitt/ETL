# Airflow + Snowflake Medallion Pipeline: Course Book

Build a production-grade Airflow DAG that loads five days of retail source files through
Bronze (STG), Silver (TMP) and Gold (TGT + Aggregate) in Snowflake, reconciles Gold back to
source, and is safe to re-run. The target architecture is
[`architecture-Simple view.png`](architecture-Simple%20view.png).

This file is the index. Each lesson has its own file in [`lessons/`](lessons/).
Rule: we do not start a lesson until the previous checkpoint passes.

## Start here

| Step | Open | Why |
|---|---|---|
| 1 | [`architecture-Simple view.png`](architecture-Simple%20view.png) and [`architecture_simple.md`](architecture_simple.md) | The picture everything is built to |
| 2 | This file, down to "Target DAG graph" | The whole goal, the decisions and the checklist |
| 3 | [`ASSUMPTIONS.md`](ASSUMPTIONS.md) | Short list of project assumptions and why |
| 4 | [`THEORY.md`](THEORY.md): the sections listed for your lesson | Concepts before code |
| 5 | [`PROGRESS.md`](PROGRESS.md) | What is done vs pending |
| 6 | The lesson file, e.g. [`lessons/05_bronze_load.md`](lessons/05_bronze_load.md) | Follow the steps in order and run the checkpoint |
| 7 | Send the checkpoint results to the agent | The next lesson starts only after it passes |

New chat with an agent? Give it [`AGENT_CONTEXT.md`](AGENT_CONTEXT.md) first.

## Progress so far (18 Nov 2026)

| # | Lesson | Status |
|---|---|---|
| 1 | Environment, connections, variables | **Done** |
| 2 | Simple data + STG DDL + file formats | **Done** |
| 3 | Internal stage: PUT / LIST / COPY into STG | **Done** |
| 4 | FileSensor, Assets trade-off, gate G1 | **Done** |
| 5 | Bronze: idempotent DELETE+FORCE, G2, archive | **Almost done** — DAG and docs written; finish checkpoint (2nd run + exercises) then reply |
| 6 | Complex five-day generator (history deferred) | **In progress** |
| 7–14 | Silver → Gold → recon → backfill → README | **Pending** |

**Built and working in Snowflake (day 2026-11-10):**
- Schemas `BRONZE` / `SILVER` / `GOLD` / `AUDIT`, role, service user, warehouse
- File formats `FF_CSV_HEADER`, `FF_CSV_POSITIONAL`; stage `MY_INT_STAGE`
- Empty-then-loaded STG tables with lineage; row counts **5000 / 1502 / 101 / 25**; net **2,843,788.51**
- DAGs `01_` … `05_` (no `lesson` prefix); landing files archived under `data/archive/2026-11-10/`

**Not built yet:** Silver TMP, Gold dims/facts, recon G11, five-day backfill. Planted defects are **in L6 data**; L10 proves gates/PK/SCD2. Long history optional/deferred.

## Decisions

| Topic | Decision | Why |
|---|---|---|
| Architecture | Follow the simple-view diagram: STG → TMP → TGT → Aggregate, names, gates G1–G10, retention, reload policy | One shared picture; see [THEORY 1](THEORY.md#1-medallion-architecture-stg--tmp--tgt--aggregate) |
| Brief vs diagram | Diagram names everywhere; brief requirements kept as documented departures | [THEORY 1.4](THEORY.md#14-departures-and-where-the-brief-and-the-diagram-differ) |
| Airflow | 3.3.2 (existing Docker Compose stack, official image) | Satisfies "2.7+"; 2.x names are mapped below |
| Backfill dates | 10, 11, 12, 13 and 16 Nov 2026 | The 5 weekdays in 10–16 Nov; schedule `0 6 * * 1-5` produces exactly these |
| External stage | **Dropped** (departure 9); the pipeline uses only the internal stage | No S3, Azure Blob or GCS bucket available, and external stages require one |
| Snowflake auth | Key pair for a `TYPE = SERVICE` user | No password, no MFA prompts |
| Local email | Mailpit container (Lesson 11) | Free SMTP server with a web inbox |
| Keep it simple | Official image, no pytest, plain readable code, only features the brief and diagram need | Learn more by using fewer tools |
| Testing deliverable | Gates and reconciliation are **SQL tasks in the DAG**, plus two one-line checks (L1) | Same guarantees without a Python test framework |
| Data types | As on the diagram: STG all `VARCHAR`; TMP/TGT ids `VARCHAR(50)`, money `NUMBER(18,2)`, qty `NUMBER(18,3)`, keys `NUMBER(38,0)`, `DAY_KY NUMBER(8,0)`, `DATE`, `BOOLEAN`, `TIMESTAMP_NTZ` | [THEORY 3.10](THEORY.md#310-data-types-and-why-we-pick-them) |
| Quarantine | `SILVER.TMP_QUARANTINE` with `GATE_ID` and `REASON_CD`; never drop | [THEORY 5.1](THEORY.md#51-quarantine-never-drop) |
| Outliers | Valid extremes are kept; anomalies are caught by gate G12, limits in `medallion_config.outlier_rules` | [THEORY 5.6](THEORY.md#56-outliers-vs-anomalies-and-the-configurable-gate-g12) |

### Airflow 2.x names in the brief vs Airflow 3.3

| Brief (2.x) | Airflow 3.3 equivalent |
|---|---|
| `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |
| `SnowflakeOperator` | `SQLExecuteQueryOperator(conn_id=...)` (SnowflakeOperator was removed) |
| `@task.snowflake` | Not a real decorator in either version. Use `@task` + `SnowflakeHook`, or `@task.snowpark` |
| `Dataset` | `Asset` (`from airflow.sdk import Asset`) |
| `airflow.sensors.filesystem.FileSensor` | `airflow.providers.standard.sensors.filesystem.FileSensor` |
| `airflow.operators.email.EmailOperator` | `airflow.providers.smtp.operators.smtp.EmailOperator` |
| `{{ execution_date }}` | `{{ logical_date }}` / `{{ ds }}` |
| `schedule_interval=` | `schedule=` |
| `airflow dags backfill` | `airflow backfill create` |

### Brief names vs project (diagram) names

| Brief | This project |
|---|---|
| `pos_sales_YYYYMMDD.csv` / `inventory_snapshot_YYYYMMDD.csv` | `sales_YYYYMMDD.csv` / `inventory_YYYYMMDD.csv` |
| `item_master_*`, `store_master_*` | same file names |
| DIM_SUBCLASS (subclass → class → dept) | `DIM_SUBCATEGORY` → `DIM_CATEGORY` → `DIM_DEPARTMENT` |
| DIM_DISTRICT (store → district → region) | `DIM_CITY` → `DIM_REGION` → `DIM_DIVISION` |
| DIM_ITEM, DIM_LOCN, FCT_SALES, FCT_INVENTORY | same names |
| (not in brief) | `DIM_CUSTOMER`, `DIM_DAY`, `FCT_PRICE`, `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES` |
| `valid_from` / `valid_to` / `is_current` | `EFFECTIVE_START_DT` / `EFFECTIVE_END_DT` / `CURRENT_FLG` |
| `load_date`, `BATCH_ID` | `_INGEST_DT` (day D), `_BATCH_ID` (Airflow run id) |

## Running checklist

Legend: `[x]` done and checkpoint passed · `[~]` in progress · `[ ]` pending (lesson number in brackets)

### Pipeline shape

- [ ] Stages `wait → bronze → silver → gold → reconcile` (L9, L11)
- [x] Sensor on all four files with `timeout`, `poke_interval`, `mode='reschedule'` (gate G1) (L4)
- [x] Datasets/Assets taught, trade-off explained, FileSensor picked (L4)
- [x] Mapped ingest: one STG load task per file (L5: `load_stg_*`; sensors use `.expand`)
- [ ] Silver TMP: clean, dedupe, validate, gates G3–G6 + G12 (L7)
- [ ] Gold TGT: dimensions first, then facts (L8, L9)
- [ ] Gold Aggregate: `AGG_DAILY_ITEM_LOCN_SALES` + `VW_DAILY_ITEM_LOCN_SALES`, gate G9 (L11)
- [ ] Reconciliation task (G11) that fails the DAG (L11)
- [ ] Idempotent: second run for the same date changes nothing (L12)
- [ ] Five-day backfill (10, 11, 12, 13, 16 Nov) all green (L13)

### Source data

- [~] `sales`, `inventory`, `item_master`, `store_master` files for 5 dates (L2 simple: 1 date; L6 complex + defects)
- [~] `seed_dim_day.csv` generated and version controlled (L2 generated; L8 loaded)
- [~] Hierarchies: item → subcategory → category → department; locn → city → region → division (L6 files; L8 dims)
- [~] Defect 1: two rows with null `ITEM_ID` (planted L6 on 2026-11-11; G3 proven L7)
- [~] Defect 2: one negative `SOLD_QTY` (planted L6 on 2026-11-11; G6 proven L7)
- [~] Defect 3: one sale for item missing from item master (`199999`, planted L6 on 2026-11-12; G7 proven L9)
- [~] Defect 4: one duplicated transaction line (planted L6 on 2026-11-11; G4 proven L7)
- [~] Defect 5: store `1008` moves city Spokane→Portland from 2026-11-13 (planted L6; SCD2 proven L8/L10)
- [~] Valid outliers (premium 2,499.99 item, 0.05 item, 480-unit buy, 0.005 kg, zero net, zero / 25,000 stock) kept end to end (L2 planted; L7–L11 verified)
- [~] Anomalies (price ×1000, 10,000 qty, price ÷100, 9,999,999 stock) caught by **configurable gate G12** (L2 planted; L7 solved)

### Snowflake

- [x] Internal stage `@my_int_stage` (L3: `MY_INT_STAGE`, one folder per date, PUT + COPY)
- [x] ~~External stage `@my_ext_stage` + storage integration~~: dropped 18 Nov 2026 (no cloud bucket); departure 9
- [x] `LIST @stage` in validation (L3 list; L4/L5 `assert_files_on_stage` = G1 presence)
- [x] Gate G2 column-count / header check before COPY (L5: `g2_check_*`)
- [x] Idempotent Bronze: `DELETE WHERE _INGEST_DT` + `COPY FORCE` + archive (L5)
- [ ] Surrogate keys `*_KY` for every DIM (sequence and hash compared) (L8)
- [ ] SCD2 on hierarchy dims; planted change on `DIM_LOCN` (L8, L10)
- [ ] SCD1: `DIM_CUSTOMER` and `DIM_ITEM.ITEM_DESC` (L8)
- [x] Positional load: `COPY INTO STG_INVENTORY` by `$1..$8`; all 4 STG loads use transforming COPY for lineage (L3, L5)
- [ ] Hierarchy parent → child keys preserved (L8)
- [ ] Parallel dimension loads (task groups / mapping) (L8)
- [ ] Joins and rollup for fact builds and the aggregate (L9, L11)
- [ ] TMP → TGT incremental inserts with `NOT EXISTS` (L8, L9)
- [ ] PK checker on every Gold table (L10)
- [ ] `FCT_INVENTORY` soft-closed (`EFFECTIVE_END_DT` / `CURRENT_FLG`), never deleted (L9)
- [ ] `FCT_PRICE` periods, no overlaps (G10) (L9, L10)
- [ ] `DIM_DAY` seeded straight into Gold (departure) (L8)
- [ ] NULL handling everywhere: `COALESCE`, `NULLIF`, `TRY_TO_*` (L7)

### Reconciliation and alerting

- [ ] Two-column, five-row table (`DAY_KY | VARIANCE_AMT`) (L11)
- [ ] DAG fails if any variance ≠ 0 (L11)
- [ ] Alert via `EmailOperator` / `on_failure_callback` with SMTP connection (L11)
- [ ] Return-flag pattern: task returns bool → downstream branch (L11)

### Engineering rules

- [~] One SQL file per target table under `include/sql/` (every lesson)
- [~] No credentials, paths or table names in DAG files (checked with one `Select-String` line)
- [~] Retries set, no cycles, DAGs parse cleanly (`airflow dags list-import-errors`)
- [~] Data types chosen and explained per column (L2 STG; L7 TMP; L8–L9 TGT)

### Idempotency notes to teach

- [ ] MERGE vs INSERT OVERWRITE vs `NOT EXISTS` (L7, L8)
- [ ] Deterministic surrogate keys vs sequence keys on re-runs (L8)
- [ ] SCD2 effective-date recompute on re-run (L10)
- [~] Partition pruning by `_INGEST_DT` / `DAY_KY` (L5 taught; L7+)
- [~] `COPY INTO ... FORCE = TRUE` after `DELETE WHERE _INGEST_DT` (L5 — almost done)

### Deliverables

- [~] `dags/` (L1 onward)
- [~] `include/sql/` (L1 onward)
- [ ] `README.md`: how to run, backfill, limitations, evidence incl. backfill screenshot description and 2×5 reconciliation table (L14)

### Acceptance criteria

- [ ] 1. Second run for the same date changes no counts and no totals (L12)
- [ ] 2. Five-day backfill completes with every task green (L13)
- [ ] 3. Broken records quarantined and counted, run continues (L10)
- [~] 4. No credentials, paths or table names in DAG files (check from L1)
- [ ] 5. Gold net sales ties to source for all five days (L11)

## Target folder structure

```
airflow-medallion/
├── README.md                     run, backfill, limitations, evidence
├── docker-compose.yaml           official Airflow 3.3.2 image + extra mounts
├── .env / .env.example           AIRFLOW_UID, FERNET_KEY (.env is git-ignored)
├── context/                      all documentation
│   ├── LESSONS.md                this file
│   ├── THEORY.md                 concept notes
│   ├── ASSUMPTIONS.md            short decisions
│   ├── AGENT_CONTEXT.md          handoff for a new agent chat
│   ├── architecture*.png/.md/.drawio   the target architecture
│   └── lessons/                  one file per lesson
├── dags/
│   ├── 01_connection_check.py
│   ├── 02_stg_setup.py           file formats + STG DDL
│   ├── 03_stage_load.py          stage, PUT, LIST, COPY into STG
│   ├── 04_waiting.py             FileSensor (G1) + PUT + assert on stage
│   ├── 05_bronze_load.py         wait → PUT → G1/G2 → DELETE+FORCE COPY → archive
│   └── medallion_daily.py        the final pipeline (assembled from L5 onward)
├── include/
│   ├── medallion/config.py       connection ids, Variable name, SQL_DIR, SOURCES
│   ├── config/                   variables.json, connections.example.json
│   └── sql/
│       ├── setup/                bootstrap, file formats, stages, integration
│       ├── ddl/bronze/           stg_<source>.sql            (L2)
│       ├── ddl/silver/           tmp_<table>.sql             (L7)
│       ├── ddl/gold/             dim_*.sql, fct_*.sql, agg_*.sql, vw_*.sql (L8–L11)
│       ├── stage/                put_file.sql (one PUT for every source, via params) (L3)
│       ├── load/bronze/          stg_<source>.sql  COPY      (L3; idempotent in L5)
│       ├── load/silver/          tmp_<table>.sql             (L7)
│       ├── load/gold/            dim_*.sql, fct_*.sql, agg_*.sql (L8–L11)
│       ├── gates/                g01_*.sql … g11_*.sql, pk_*.sql
│       └── checks/               whoami.sql (smoke test), list_stage.sql (L3)
├── scripts/                      generate_simple.py, generate_seed_dim_day.py (later: complex, very complex)
├── data/landing/                 the four daily drops land here (FileSensor watches it)
├── data/archive/                 original files kept after a successful STG load (L5)
├── data/seed/                    seed_dim_day.csv (version controlled)
└── secrets/                      rsa_key.p8, connections.json (git-ignored)
```

## Target DAG graph (`medallion_daily`)

Schedule `0 6 * * 1-5`, one run per business date D = `{{ ds }}`, `max_active_runs=1`.

```
 WAIT  (G1) ───────────────────────────────────────────────────────────────────────────
   wait_for_file[sales | inventory | item_master | store_master]
        FileSensor, mapped x4, mode=reschedule, timeout  → alert, then fail
                                 │
 BRONZE · STG ───────────────────▼─────────────────────────────────────────────────────
   put_to_stage  (PUT → @my_int_stage/D/)  →  assert_files_on_stage (LIST, G1 count)
   load_stg.expand(source=[sales, inventory, item_master, store_master])
        DELETE _INGEST_DT = D, COPY INTO STG_* (inventory positional), G2 header check
   archive_files  (landing → archive)
                                 │
 SILVER · TMP ───────────────────▼─────────────────────────────────────────────────────
   tmp_sales_line   tmp_customer   tmp_item_loc_snapshot   tmp_item_hier   tmp_locn_hier
        cast · trim · NULLIF/COALESCE · dedupe · gates G3–G6, G12 outliers → TMP_QUARANTINE
   quarantine_summary  (counts per gate; run continues)
                                 │
 GOLD · TGT ─────────────────────▼─────────────────────────────────────────────────────
   dims (task group, parallel branches; parents before children)
     dim_department → dim_category → dim_subcategory → dim_item     (SCD2; ITEM_DESC SCD1)
     dim_division   → dim_region   → dim_city        → dim_locn     (SCD2)
     dim_customer (SCD1 MERGE)       dim_day (seed, departure)
   gate G7 (keys resolve → quarantine + alert)   gate G8 (inventory ±20% → fail)
   facts (after all dims)
     fct_sales (MERGE)   fct_inventory (soft-close)   fct_price (periods)
   pk_checker   gate G10 (no price overlap)
                                 │
 GOLD · AGGREGATE ───────────────▼─────────────────────────────────────────────────────
   agg_daily_item_locn_sales (recompute touched DAY_KY)   vw_daily_item_locn_sales
   gate G9 (AGG = FCT_SALES per DAY_KY → fail)
                                 │
 RECONCILE (G11) ────────────────▼─────────────────────────────────────────────────────
   reconcile → AUDIT.RECON_DAILY_NET_SALES, returns True/False
   branch_on_recon ──► recon_passed
                  └──► send_alert (EmailOperator) ──► fail_run (raises)
   on_failure_callback on every task → SMTP alert
```

## Curriculum

Theory column = sections of [`THEORY.md`](THEORY.md) to read before the lesson.

| # | Lesson | Data level | Theory | Status |
|---|---|---|---|---|
| 1 | [Environment, connections, variables](lessons/01_environment.md) | none | 1, 2.1, 2.3, 2.6, 3.1, 3.2, 8 | Done (17 Nov 2026) |
| 2 | [Simple data + STG tables + file formats](lessons/02_stg_tables.md) | Simple (1 day) | 1, 3.4, 3.7, 3.10 | Done (18 Nov 2026) |
| 3 | [Internal stage: `PUT`/`LIST`/`COPY`, positional load](lessons/03_stages_copy.md) | Simple (1 day) | 3.3, 3.4 | Done (18 Nov 2026) |
| 4 | [Waiting: FileSensor vs Assets, gate G1](lessons/04_waiting.md) | Simple (1 day) | 2.7, 2.8, 3.3, 5.2 | Done (18 Nov 2026) |
| 5 | [Bronze load: idempotent COPY, G2, archive](lessons/05_bronze_load.md) | Simple (1 day) | 2.4, 2.9, 3.4, 3.5, 5.2, 6 | Done (18 Nov 2026) |
| 6 | [Generator v2: five days (~810 lines, txns 12/14/16/18/20), sales⊆stock, D1–D5; history deferred](lessons/06_history_generator.md) | Complex | 4.1, 4.3 | **In progress** |
| 7 | Silver: the 5 TMP tables + `TMP_QUARANTINE`, gates G3–G6 and G12 (configurable outliers) | Complex | 3.6, 3.7, 3.10, 5.1, 5.2, 5.6 | Pending |
| 8 | Gold TGT dimensions: 8 hierarchy dims (SCD2), `DIM_CUSTOMER` (SCD1), `DIM_DAY` seed, surrogate keys, parallel loads | Complex | 4.2, 4.3, 4.4, 3.6, 2.9, 6 | Pending |
| 9 | Gold TGT facts: `FCT_SALES`, `FCT_INVENTORY` (soft-close), `FCT_PRICE`; gates G7, G8; full wait → gold DAG; history bulk load **only if** history is added later | Complex | 4.1, 4.5, 3.6, 5.2 | Pending |
| 10 | Prove planted defects (G3/G4/G6/G7 + SCD2 on `DIM_LOCN`), PK checker, gate G10 | Very complex | 4.4, 4.6, 3.8, 5 | Pending |
| 11 | Aggregate + view (G9), reconciliation 2×5 table (G11), return-flag branch, SMTP alert | Very complex | 4.7, 5.4, 2.10, 2.11 | Pending |
| 12 | Idempotency proof and the reload of one bad day | Very complex | 6, 5.5 | Pending |
| 13 | Five-day backfill, every task green, evidence | Very complex | 2.4 | Pending |
| 14 | README, acceptance-criteria walkthrough | Very complex | 7 | Pending |

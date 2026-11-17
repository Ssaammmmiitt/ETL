# Costco medallion architecture notes

Assignment 2.2 places the 2.1 Costco star schema into Bronze, Silver and Gold layers. Diagram: `architecture.drawio` (page *Medallion Architecture* has full table structures; page *Simple view* is a one-glance version, explained in `architecture_simple.md`). Inventory: `layer_inventory.csv`. Model DDL: `create_tables.sql`.

## Naming convention

| Table type | Layer | Purpose | Objects |
|---|---|---|---|
| **STG** | Bronze | Raw landing, no transformation | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | Silver | Clean and compare against target | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER`, `TMP_QUARANTINE` |
| **TGT** | Gold | The dimension or fact itself | the 10 `DIM_*` and 3 `FCT_*` tables, keeping their 2.1 names |
| **Aggregate** | Gold | Rolled-up marts on top of TGT | `AGG_DAILY_ITEM_LOCN_SALES` (+ view `VW_DAILY_ITEM_LOCN_SALES`) |

The incoming `*_YYYYMMDD.csv` files are source files, not tables, so they keep their file names. Each lands into the matching `STG_` table.

The diagram reuses the actual table shapes and 1:M relationships from `Costco.drawio`. The 10 DIM and 3 FCT tables sit in Gold TGT, and `AGG_DAILY_ITEM_LOCN_SALES` sits in Gold Aggregate. Bronze and Silver are drawn as column-level tables in the same style. Each layer has its own colour, and lineage arrows link each source file to its Bronze table, each Bronze table to its Silver table, and each Silver table to the Gold tables it loads. A badge above every table shows its format, partitioning and load pattern.

## Overview and assumptions

The 2.1 model does not name the source files, so four daily drops are defined to cover every table with no gaps:

| Drop | Grain | Load | Feeds |
|---|---|---|---|
| `sales_YYYYMMDD.csv` | One POS line per row, with member attributes when present | delta | `FCT_SALES`, `DIM_CUSTOMER` |
| `inventory_YYYYMMDD.csv` | One item at one location for the snapshot day, with on-hand, reserved, in-transit, unit price and promo price | full | `FCT_INVENTORY`; Silver also detects price changes to build `FCT_PRICE` periods |
| `item_master_YYYYMMDD.csv` | Flattened item, subcategory, category and department | full | `DIM_ITEM`, `DIM_SUBCATEGORY`, `DIM_CATEGORY`, `DIM_DEPARTMENT` |
| `store_master_YYYYMMDD.csv` | Flattened location, city, region and division | full | `DIM_LOCN`, `DIM_CITY`, `DIM_REGION`, `DIM_DIVISION` |

`DIM_DAY` is generated. It is not a daily drop. The seed file is `seed_dim_day.csv`, version controlled and loaded straight into Gold (see [Departure](#departure-from-the-reference-architecture)).

The assignment wording says 13 tables. The 2.1 model has **14**: 10 dimensions, 3 facts and 1 aggregate. All 14 appear exactly once in the inventory. `VW_DAILY_ITEM_LOCN_SALES` is a Gold Aggregate view (not a physical inventory row). On Gold tables, "partitioning" means a Snowflake clustering key.

## Layer contracts

### Bronze (STG) — raw landing, no transformation

**Guarantees**

- Byte-for-byte copy of the source file as received.
- Replayable: every file is kept under `ingest_dt` with `source_file` lineage.
- Arrival is recorded even when the file later fails a gate.

**Does not guarantee**

- Correct data types, non-null keys or unique business keys.
- Deduplication across re-sends of the same day.
- That foreign keys or measures will pass business rules.

### Silver (TMP) — clean and compare against target

**Guarantees**

- Typed columns, trimmed strings and normalised IDs.
- Each TMP row is compared with its TGT table to classify it as new, changed or unchanged before Gold is loaded.
- Deduplicated on the business key for that object.
- Bad rows are moved to `TMP_QUARANTINE` with a reason code; they are not silently dropped.
- Lineage columns (`ingest_dt`, `source_file`) are retained.

**Does not guarantee**

- Surrogate keys (`*_KY`).
- SCD2 history or effective dating.
- That published business totals are ready for finance.

### Gold TGT — the dimensions and facts themselves

**Guarantees**

- The 2.1 star-schema grain from `model_notes.md`.
- SCD1 / SCD2 rules as documented in 2.1.
- Fact foreign keys resolve to a dimension key (or the row stays quarantined).
- Facts are idempotent on their natural keys: `(TRANSACTION_ID, LINE_NUM)`, `(ITEM_KY, LOCN_KY, SNAPSHOT_DT)`, `(ITEM_KY, LOCN_KY, EFFECTIVE_START_DT)`.

**Does not guarantee**

- Real-time freshness (batch is as of D+1).
- That the layer is still raw evidence of what the source sent.
- Line-level reporting performance for every dashboard (that is the Aggregate's job).

### Gold Aggregate — rolled-up marts on top of TGT

**Guarantees**

- `AGG_DAILY_ITEM_LOCN_SALES` totals reconcile with `FCT_SALES` on `(DAY_KY, ITEM_KY, LOCN_KY)`.
- The aggregate can be rebuilt from TGT without reading Bronze.

**Does not guarantee**

- Transaction line detail (`TRANSACTION_ID`, `LINE_NUM`, `CUSTOMER_KY`).

## Object placement (exactly once for every 2.1 table)

| 2.1 object | Layer | Type | Why |
|---|---|---|---|
| (four daily drops → `STG_*`) | Bronze | STG | Raw landings |
| `DIM_DEPARTMENT` … `DIM_LOCN` (8 SCD2 dims) | Gold | TGT | Conformed dimensions with history |
| `DIM_CUSTOMER` | Gold | TGT | SCD1 conformed dimension |
| `DIM_DAY` | Gold | TGT | Seeded calendar |
| `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` | Gold | TGT | Atomic facts |
| `AGG_DAILY_ITEM_LOCN_SALES` | Gold | Aggregate | Rollup for store / item reports |
| `VW_DAILY_ITEM_LOCN_SALES` | Gold | Aggregate (view) | Live rollup; not in the CSV inventory |

Bronze and Silver hold only STG and TMP objects. They are not 2.1 model tables:

| STG table (Bronze) | Partitioning | Load | Source drop |
|---|---|---|---|
| `STG_SALES` | `ingest_dt` | delta | `sales_YYYYMMDD.csv` |
| `STG_INVENTORY` | `ingest_dt` | full | `inventory_YYYYMMDD.csv` |
| `STG_ITEM_MASTER` | `ingest_dt` | full | `item_master_YYYYMMDD.csv` |
| `STG_STORE_MASTER` | `ingest_dt` | full | `store_master_YYYYMMDD.csv` |

| TMP table (Silver) | Partitioning | Load | Source drop |
|---|---|---|---|
| `TMP_SALES_LINE` | `sale_dt` | delta | `sales_YYYYMMDD.csv` |
| `TMP_CUSTOMER` | none | delta | `sales_YYYYMMDD.csv` |
| `TMP_ITEM_LOC_SNAPSHOT` | `snapshot_dt` | full | `inventory_YYYYMMDD.csv` |
| `TMP_ITEM_HIER` | none | full | `item_master_YYYYMMDD.csv` |
| `TMP_LOCN_HIER` | none | full | `store_master_YYYYMMDD.csv` |
| `TMP_QUARANTINE` | `ingest_dt` | delta | all (failed rows / files) |

## Quality gates

At least six rules are required. Ten are defined. Each sits on a named layer boundary and has a failure action of **fail**, **quarantine** or **alert**.

| # | Boundary | Rule | Failure action |
|---|---|---|---|
| 1 | Source → STG | File arrives by SLA and row count matches the drop manifest | **alert**, then **fail** (do not land) |
| 2 | Source → STG | Header and column schema match the file contract | **quarantine** the whole file |
| 3 | STG → TMP | Business keys are not null (`ITEM_ID`, `LOCN_ID`, `TRANSACTION_ID`+`LINE_NUM`, etc.) | **quarantine** the row |
| 4 | STG → TMP | `(TRANSACTION_ID, LINE_NUM)` is unique within the load | keep latest; **quarantine** duplicates |
| 5 | STG → TMP | `sale_dt` is between D−7 and D (late-arriving window) | **quarantine** the row |
| 6 | STG → TMP | `NET_SALES_AMT = SALES_AMT − DISCOUNT_AMT` within 0.01 | **quarantine** the row |
| 6b | STG → TMP | `RESERVED_QTY ≤ ON_HAND_QTY` | **alert** only (still load) |
| 7 | TMP → TGT | Item and location IDs resolve to a current dimension key | **quarantine** and **alert** |
| 8 | TMP → TGT | Inventory snapshot row count is within ±20% of the prior day | **fail** (do not publish that day's inventory) |
| 9 | TGT → Aggregate | `AGG` totals equal `FCT_SALES` totals per `DAY_KY` | **fail** |
| 10 | TGT | Price periods for the same `(ITEM_KY, LOCN_KY)` do not overlap | **fail** |

## Retention policy

| Layer | Retention | Notes |
|---|---|---|
| Bronze (STG) | 400 days online, then cold archive | Covers year-over-year compares and one-day reloads |
| `TMP_QUARANTINE` | 90 days | Long enough to investigate; not a permanent store |
| Silver (TMP) | 3 years | Rebuild window for Gold without depending only on Bronze |
| Gold TGT | 7 years | Finance / audit horizon |
| Gold Aggregate | 3 years | Rebuildable from TGT; shorter online window is fine |
| Snowflake Time Travel | 1 day Bronze/Silver; 7 days Gold | Complements, does not replace, retention |

## Reload policy (one bad day D)

Partitioning is what makes a single-day reload safe and cheap.

1. **Land the correction.** Place the corrected file as a new version under `ingest_dt=D`. Keep the bad file; mark it superseded. Do not overwrite Bronze history.
2. **Rebuild Silver for that ingest.** Overwrite Silver partitions that came from ingest D (`sale_dt` / `snapshot_dt` / full-refresh masters). Lineage columns record which ingest produced each row.
3. `FCT_SALES`**.** Merge on `(TRANSACTION_ID, LINE_NUM)`. Then recompute `AGG_DAILY_ITEM_LOCN_SALES` only for the `DAY_KY` values touched by the merge (partition prune on `DAY_KY`).
4. `FCT_INVENTORY`**.** `DELETE` where `SNAPSHOT_DT = D`, then insert the corrected snapshot (partition prune on `SNAPSHOT_DT`).
5. `FCT_PRICE`**.** Re-derive price periods from snapshot D forward for the `(item, location)` pairs that changed. No date partition: a period spans many days, so rewrite by business key, not by day folder.
6. **SCD2 dimensions.** Roll back versions opened or closed on D (using `EFFECTIVE_START_DT` / `UPDATE_TS`), then re-apply the master files for D.
7. `DIM_CUSTOMER`**.** SCD1 merge from the corrected sales extract for D.
8. **Publish.** Re-run gates 7–10. Only promote if they pass.
9. **Idempotency.** Every step above can be re-run; natural keys and partition replaces prevent double counting.

Why this works with the chosen partitioning:

- Bronze `ingest_dt` keeps every landing version.
- Silver `sale_dt` / `snapshot_dt` lets one day be overwritten without scanning the whole history.
- Gold `DAY_KY` / `SNAPSHOT_DT` clustering prunes fact and aggregate reloads to day D.
- `FCT_PRICE` is intentionally unpartitioned by date so a multi-day effective period is not split across folders.

## Departure from the reference architecture

**Departure:** `DIM_DAY` is seeded directly into Gold. It does not flow through Bronze or Silver.

**Justification:** The calendar is generated deterministically (`DAY_KY = YYYYMMDD`). There is no external system of record and nothing "raw" to preserve or cleanse. Routing it through Bronze and Silver would create two extra copies with no audit value. Traceability is kept by version-controlling the generator and `seed_dim_day.csv`. On the diagram the seed arrow bypasses Bronze and Silver and is labelled as the departure.

## Acceptance checklist

| Criterion | How it is met |
|---|---|
| Sources, three layers and consumers all shown | `architecture.drawio`: four drops → Bronze (STG) → Silver (TMP) → Gold (TGT → Aggregate) → consumers |
| Naming convention STG / TMP / TGT / Aggregate | Naming convention section; table names and lane titles on both diagram pages; `Table type` column in `layer_inventory.csv` |
| Every 2.1 object appears exactly once | 10 DIM + 3 FCT in Gold TGT; 1 AGG in Gold Aggregate; see `layer_inventory.csv` |
| Format, partitioning and load pattern recorded | Every inventory row; also annotated on diagram boxes |
| Contracts written out | Layer contracts section above |
| At least six quality gates with fail / quarantine / alert | Ten gates in the quality table |
| Retention per layer | Retention policy table |
| Reload policy for one bad day | Reload policy section; tied to partitioning |
| One justified departure | `DIM_DAY` seed bypass |
| It works given the partitioning | Reload steps prune by `ingest_dt`, `sale_dt`, `snapshot_dt`, `DAY_KY`, `SNAPSHOT_DT` |

# Costco medallion architecture: simple view

A one-glance version of the 2.2 architecture. It lives on the **Simple view** page of `architecture.drawio` (use the page tabs at the bottom of draw.io). The **Medallion Architecture** page still holds the detailed version with every column. Full reasoning is in `architecture_notes.md`, and the object list is `layer_inventory.csv`.

## How to read the diagram

Data flows left to right through six coloured columns:

**Source files → Bronze (STG) → Silver (TMP) → Gold TGT → Gold Aggregate → Consumers**

### Naming convention

| Table type | Layer | Purpose | Tables |
|---|---|---|---|
| **STG** | Bronze | Raw landing, no transformation | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| **TMP** | Silver | Clean and compare against target | `TMP_SALES_LINE`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_CUSTOMER`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER`, `TMP_QUARANTINE` |
| **TGT** | Gold | The dimension or fact itself | 10 `DIM_*` + 3 `FCT_*` (2.1 names kept) |
| **Aggregate** | Gold | Rolled-up marts on top of TGT | `AGG_DAILY_ITEM_LOCN_SALES`, view `VW_DAILY_ITEM_LOCN_SALES`, report packs |

The `*_YYYYMMDD.csv` drops are source files, not tables, so they keep their file names.

| Symbol | Meaning |
|---|---|
| Document (wavy bottom) | Source file drop |
| Cylinder | Stored table or landed file |
| Red cylinder | `TMP_QUARANTINE` (rejected rows) |
| Rounded chip | Dimension table |
| Red hexagon | Quality gate on a layer boundary |
| Orange block arrow | Facts look up dimension surrogate keys (1:M) |
| Dashed teal box | View (`VW_DAILY_ITEM_LOCN_SALES`) |
| Stick figure | Consumer |
| Green dashed line | `DIM_DAY` seed, the one departure |

Every object carries a small line in the form **format · partitioning · load pattern**, for example `table · DAY_KY · delta`. A facts card at the bottom of each column summarises that layer.

## Layers at a glance

| Layer | Objects | Format | Partitioning | Load | Retention |
|---|---|---|---|---|---|
| Sources | 4 daily drops + 1 seed | CSV | — | sales delta; others full | owned by source |
| Bronze (STG) | 4 STG tables | CSV as landed | `ingest_dt` | sales delta; others full | 400 days |
| Silver (TMP) | 5 TMP tables + quarantine | Parquet | `sale_dt`, `snapshot_dt`, `ingest_dt` or none | delta / full | 3 years (quarantine 90 days) |
| Gold TGT | 10 DIM + 3 FCT | Snowflake table | `DAY_KY` (FCT_SALES), `SNAPSHOT_DT` (FCT_INVENTORY), else none | delta (SCD merge / insert); DIM_DAY seed | 7 years |
| Gold Aggregate | 1 AGG + 1 view | table / view | `DAY_KY` | delta (recompute touched days) | 3 years, rebuilt from TGT |

### What each layer guarantees

- **STG** keeps a byte-for-byte copy with no transformation, so it can be replayed. It does **not** guarantee types, valid keys or deduplication.
- **TMP** rows are typed and deduplicated, then compared against TGT to find new and changed rows. Bad rows go to quarantine instead of being dropped. TMP does **not** have surrogate keys, SCD2 history or totals.
- **TGT** keeps the 2.1 grain and SCD rules, and every FK resolves. It is **not** real-time (data is as of D+1) and holds no raw evidence.
- **Aggregate** totals reconcile to FCT_SALES. It does **not** hold line-level detail.
- **Consumers** read Aggregate for reporting and TGT for detail. They never read STG or TMP.

## Gold TGT in one picture

The diagram groups the 13 TGT tables instead of drawing every column:

- **Customer** (SCD1): `DIM_CUSTOMER`
- **Product hierarchy** (SCD2): `DIM_DEPARTMENT → DIM_CATEGORY → DIM_SUBCATEGORY → DIM_ITEM`
- **Location hierarchy** (SCD2): `DIM_DIVISION → DIM_REGION → DIM_CITY → DIM_LOCN`
- **Calendar** (seed): `DIM_DAY`
- **Facts**: `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` (derived from price changes in the snapshot)

Dimensions load first, in parallel. Facts load after them, because each fact row needs the dimensions' surrogate keys. `AGG_DAILY_ITEM_LOCN_SALES` in Gold Aggregate rolls up from `FCT_SALES`.

## TMP → TGT feeds

| TMP (Silver) | Feeds TGT (Gold) |
|---|---|
| `TMP_SALES_LINE` | `FCT_SALES` |
| `TMP_ITEM_LOC_SNAPSHOT` | `FCT_INVENTORY`, `FCT_PRICE` |
| `TMP_CUSTOMER` | `DIM_CUSTOMER` |
| `TMP_ITEM_HIER` | Product hierarchy dims |
| `TMP_LOCN_HIER` | Location hierarchy dims |

## Quality gates (on the diagram's boundary hexagons)

| Gate | Boundary | Rule | Action |
|---|---|---|---|
| G1 | Source → STG | file arrives by SLA; row count matches the manifest | alert, then fail |
| G2 | Source → STG | header matches the contract | quarantine file |
| G3 | STG → TMP | business keys not null | quarantine row |
| G4 | STG → TMP | `(TRANSACTION_ID, LINE_NUM)` unique | keep latest, quarantine rest |
| G5 | STG → TMP | sale date within D−7..D | quarantine row |
| G6 | STG → TMP | net = sales − discount (reserved > on hand only alerts) | quarantine / alert |
| G7 | TMP → TGT | item and location resolve to a surrogate key | quarantine + alert |
| G8 | TMP → TGT | inventory row count within ±20% of the prior day | fail |
| G9 | TGT → Aggregate | AGG totals = FCT_SALES per day | fail |
| G10 | TGT | no overlapping price periods | fail |

## Reload one bad day D

1. Land the corrected file into `STG_*` under `ingest_dt=D`. Keep the bad file, marked superseded.
2. Overwrite the `TMP_*` partitions that came from that ingest.
3. Merge `FCT_SALES` on `(TRANSACTION_ID, LINE_NUM)`, then recompute `AGG` for the touched `DAY_KY` values.
4. In `FCT_INVENTORY`, delete and re-insert `SNAPSHOT_DT = D`.
5. Re-derive `FCT_PRICE` periods from D onward for the item/location pairs that changed.
6. For the SCD2 dims, undo the versions opened or closed on D, then re-apply.
7. Re-run G7–G10, then publish. Every step is idempotent, so a rerun is safe.

## Departure from the reference

`DIM_DAY` is seeded straight into Gold TGT; on the diagram this is the green dashed line that skips STG and TMP. The calendar is generated (`DAY_KY = YYYYMMDD`) and has no source system and no raw data to keep or clean, so copies in Bronze and Silver would add no audit value. The generator and seed file are under version control.

## Exporting a PNG

In draw.io, open `architecture.drawio` and select the **Simple view** tab. Then choose **File → Export as → PNG**, leave *Selection only* unticked, and set zoom to 100–150%.

# Lesson 6: Complex data (five pipeline days)

| | |
|---|---|
| **Goal** | Generate the **five pipeline days** with hierarchies, stocked-only sales, and the **five planted defects** from the brief |
| **You build** | `scripts/generate_complex.py` (no new DAG — files only) |
| **Status** | In progress |
| **Not yet** | Silver/Gold gates that *prove* those defects (L7–L10); optional long history (deferred) |
| **Theory (optional)** | [4.1 Grain, facts, dimensions](../THEORY.md#41-grain-facts-and-dimensions) · [4.3 Hierarchies](../THEORY.md#43-hierarchies) |

---

## 6.1 The idea in one picture

```
generate_complex.py
        │
        └─► data/landing/*_YYYYMMDD.csv     10, 11, 12, 13, 16 Nov 2026
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
| Pipeline days | Days the daily DAG will process | 10, 11, 12, 13, 16 Nov 2026 |
| Grain | What one fact row means | Sales line = one till line; inventory = one item@store snapshot |
| Hierarchy (flattened file) | Parent attributes copied onto each child row | `item_master` and `store_master` CSVs |
| Hierarchy (snowflaked Gold) | One table per level, child holds parent key | Built in L8 from those flat files |
| Stocked set | Items a store carries on the snapshot | 60 items/store (premium forced at 1001 & 1013) |
| Returning customer | Same `CUSTOMER_ID` appears on many days | Fixed member pool `M000001`…`M000500` |

---

## 6.2 What the generator writes

| Path | Days | Sales density | Notes |
|---|---|---|---|
| `data/landing/` | 5 weekdays | **~810 lines/day** (400 txns; per-store **12/14/16/18/20**) | Same names/sensors as L4–L5 |
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

### Planted defects (brief)

| # | What | File / key | Later gate |
|---|---|---|---|
| D1 | Two null `ITEM_ID` | 11 Nov: `T20261111-1001-0003` L1, `T20261111-1001-0004` L1 | G3 |
| D2 | Negative `SOLD_QTY` | 11 Nov: `T20261111-1001-0005` L3 | G6 |
| D3 | Item missing from master | 12 Nov: `T20261112-1002-0003` L1 → `199999` | G7 |
| D4 | Duplicate txn line | 11 Nov: `T20261111-1001-0008` L2 twice (**811** rows) | G4 |
| D5 | Store city (“district”) change | From 13 Nov: `LOCN_ID=1008` Spokane→Portland | SCD2 `DIM_LOCN` |

Full copy also in [`ASSUMPTIONS.md`](../ASSUMPTIONS.md).

**What still changes across the five days:**

| Signal | Behaviour |
|---|---|
| Prices | November lift; Monday + holiday-style promos |
| Stock | Sold qty drains on-hand; replenish with ~12% day-over-day cap (G8 prep) |
| Customers | Same ids return; every 5th txn is a non-member |
| Item desc | Light `ITEM_DESC` tweak on 16 Nov (SCD1 practice later) |

`generate_simple.py` stays as the L2 artifact. From here on, **use** `generate_complex.py` for the five days.

---

## 6.3 Code walkthrough (`scripts/generate_complex.py`)

Open the file top → bottom once. There is **no DAG** in this lesson — only this script.

### a) Constants at the top — what they control

| Name | Value | What it does |
|---|---|---|
| `PIPELINE_DATES` | 10, 11, 12, 13, 16 Nov 2026 | The only days written to landing |
| `TXNS_PATTERN` | `(12, 14, 16, 18, 20)` | Cycles across stores → **400 txns/day** total, uneven traffic |
| `txns_for_store(i)` | `TXNS_PATTERN[i % 5]` | Store 1001=12, 1002=14, … 1005=20, then repeats |
| `STOCKED_PER_STORE` | `60` | How many items each store carries (inventory + sales pool) |
| `TAX_RATE` | `0.08` | `TAX_AMT = NET × 0.08` (same as L2) |
| `ORPHAN_ITEM_ID` | `"199999"` | Sold on 12 Nov; **never** in `item_master` (D3 / G7) |
| `SCD2_LOCN_ID` | `"1008"` | Store that changes city from 13 Nov (D5) |
| `DEPARTMENTS` / `CITIES` | nested lists | Product and location hierarchies (same shape as L2) |
| `*_HEADER` | column name lists | Written as the first CSV line (inventory uses `header=None`) |

`LANDING_DIR` is resolved from the script path (`…/data/landing`), so it works from any working directory.

### b) Flow of `main()` — how a run works

```python
items, base_prices = build_items(100)          # 100 items + premium 100101; base shelf prices
stores = build_stores()                        # 25 stores, one per city
stocked_by_store = build_stocked_by_store(...) # locn → list of 60 ITEM_IDs (+ premium at 1001/1013)
stock = init_stock(...)                        # starting ON_HAND per (item, store)

for d in PIPELINE_DATES:                       # five weekdays only
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

### c) Catalog and stocked set

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
# premium forced onto stores 1001 and 1013 if missing
```

Each store gets a **rotating window of 60 items**. Sales and inventory both use this list, so a clean sale cannot invent an item the store does not stock.

`build_stocked_by_store` builds `{ "1001": [item_ids…], "1002": […], … }` once; every day reuses it.

### d) Prices and sales

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

### e) Inventory across days

`init_stock` — dict keyed by `(ITEM_ID, LOCN_ID)` with starting on-hand; premium fixed at 4.0 in 1001 and 1013.

`build_inventory` — for each stocked pair, write the 8 positional columns  
(`SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND, RESERVED, IN_TRANSIT, UNIT_PRICE, PROMO`).  
No header. Promo empty string → `NULL` after COPY.

`evolve_stock(stock, sales_rows, d)` — after each day:

1. Sum sold qty per `(item, locn)` from that day’s sales
2. `on_hand − sold`, then add a small replenishment
3. Clamp the new value to roughly **88%–112%** of yesterday (prep for gate G8 ±20%)

`stable_mix(item, locn, day)` — deterministic stand-in for Python’s `hash()` (which is salted per process and would make two runs differ).

`item_master_for_day` — usually a copy of the catalog; on **16 Nov** appends `(rev Nov)` to one `ITEM_DESC` (SCD1 practice later).

### f) Helpers that edit rows

`set_sales_line(row, qty=…, price=…, item_id=…, discount=…)`  
Changes fields and **recomputes** SALES / TAX / NET so arithmetic stays consistent.  
Uses `if item_id is not None` so you can set `ITEM_ID` to `""` for D1.  
Allows negative qty (D2) without clamping discount against a negative sales amount.

`first_row_at(sales, locn_id, skip=0)` — finds the n-th sales line for a store. Used so premium / anomaly edits stay on stores that actually stock `100101`.

`write_csv(path, header, rows)` — `csv.writer` with `lineterminator="\n"` (same bytes on Windows/Mac). Inventory passes `header=None`.

### g) Outliers, anomalies, defects

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

### h) What you are *not* running

- No Airflow DAG, no Snowflake SQL in this lesson.  
- `generate_simple.py` is unchanged (L2 artifact). Prefer `generate_complex.py` for the five days going forward.  
- No `data/history/` writes (deferred).

---

## 6.4 Run it

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

# defects
Select-String -Path data\landing\sales_20261111.csv -Pattern 'T20261111-1001-0003,1,.*1001,,'
Select-String -Path data\landing\sales_20261112.csv -Pattern '199999'
Select-String -Path data\landing\store_master_20261112.csv, data\landing\store_master_20261113.csv -Pattern '^1008,'
```

---

## 6.5 Points to remember

- **Five days are enough** for the medallion pipeline + recon. Long history is optional later.
- **400 txns/day fixed**; stores vary (**12/14/16/18/20**) so traffic looks uneven → **~810 lines** (Nov 11 = **811** with D4).
- **Clean sales ⊆ stocked inventory**; only D3 is an intentional orphan.
- **Defects are data now; gates come later** (L7–L10).
- **Flat files → snowflaked Gold** in L8; D5 needs SCD2 on `DIM_LOCN`.
- Re-run the generator anytime; it overwrites landing.

---

## 6.6 Exercises

1. **Grain.** In one sentence each: grain of a `sales` row, an `inventory` row, and a future `FCT_SALES` row?
2. **Volume.** Why vary txns per store but keep **400** total? What would a flat `16` everywhere hide?
3. **Hierarchy.** From one `item_master` data row, list the four level ids. Which Gold tables in L8?
4. **Stocked set.** Why allow only the planted `199999` orphan, not ~40% accidental ones?
5. **D5.** Diff `store_master` for `1008` on 12 Nov vs 13 Nov. Which attributes changed, and why is that SCD2 (not SCD1)?

---

## 6.7 Checkpoint

Reply with:

- [ ] Generator finished; sales counts **810 / 811 / 810 / 810 / 810**; store `1001` has 12 txns, `1005` has 20
- [ ] Nov 10 net ≈ **2819814.46**; defect lines printable (D1–D5)
- [ ] `1008` city Spokane on 12 Nov, Portland on 13 Nov
- [ ] (Optional) old `data\history` removed
- [ ] Answers to exercises 1–5

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `RuntimeError: … unstocked items` | Stocked list and sales picker out of sync — don’t hand-edit mid-run |
| Landing still has 5000-line sales | Re-run the generator; confirm you ran `generate_complex.py` not `generate_simple.py` |
| Want L2 simple Nov 10 back | `python scripts\generate_simple.py` (L2 only) |
| Still see `data\history` | Leftover from the first draft; delete it |

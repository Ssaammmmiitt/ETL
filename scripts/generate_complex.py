"""Complex source files — landing CSVs + calendar-bucket defects.

Writes to data/landing/:
  {sales,inventory,item_master,store_master}_YYYYMMDD.csv

Transaction defects are deterministic from the calendar date alone
(``date.toordinal() % 5`` exclusive buckets) so one-day or multi-day
generate stays consistent for any logical date / backfill. Any five
consecutive days contain every bucket.

  0 outliers | 1 null ITEM_ID | 2 neg qty | 3 orphan | 4 dup line
  (10–14 Nov 2026 → outliers, null, neg, orphan, dup)

SCD2 city move on LOCN 1008 is separate: from ``SCD2_FROM`` (2026-11-13) onward.
SCD1 item description change: from ``SCD1_FROM`` (2026-11-14) onward.

Each planted defect is appended to ``data/store/defect_log.txt`` (store id in line).

Run from the project folder:
    python scripts\\generate_complex.py
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = ROOT / "data" / "landing"
STORE_DIR = ROOT / "data" / "store"
DEFECT_LOG_PATH = STORE_DIR / "defect_log.txt"
SCD2_FROM = date(2026, 11, 13)
SCD1_FROM = date(2026, 11, 14)
# Inventory stock is replayed from this day so any single day is reproducible.
STOCK_ANCHOR = date(2026, 11, 10)

PIPELINE_DATES = [
    date(2026, 11, 10),
    date(2026, 11, 11),
    date(2026, 11, 12),
    date(2026, 11, 13),
    date(2026, 11, 14),
]
# Per-store txn counts cycle 12,14,16,18,20 (sum 80 × 5 = 400 txns/day ≈ 800 lines).
# Busier stores get more tills; daily total stays the same every pipeline day.
TXNS_PATTERN = (12, 14, 16, 18, 20)
STOCKED_PER_STORE = 60
TAX_RATE = 0.08

DEPARTMENTS = [
    ("D01", "Grocery", "FOOD", [
        ("C10", "Dairy", [("SC101", "Milk"), ("SC102", "Eggs"), ("SC103", "Cheese")]),
        ("C20", "Bakery", [("SC201", "Bread"), ("SC202", "Pastry")]),
        ("C25", "Produce", [("SC251", "Fruit"), ("SC252", "Vegetables")]),
    ]),
    ("D02", "General Merchandise", "NON_FOOD", [
        ("C30", "Electronics Accessories", [("SC301", "Batteries"), ("SC302", "Cables")]),
        ("C40", "Household", [("SC401", "Paper Goods"), ("SC402", "Cleaning")]),
        ("C50", "Apparel", [("SC501", "Basics"), ("SC502", "Socks")]),
    ]),
    ("D03", "Fresh", "PERISHABLE", [
        ("C60", "Meat", [("SC601", "Beef"), ("SC602", "Chicken")]),
        ("C70", "Seafood", [("SC701", "Fish"), ("SC702", "Shellfish")]),
    ]),
]

BRANDS = ["Kirkland", "Great Value", "Organic Valley", "La Brea", "Duracell",
          "Bounty", "Tide", "Sony", "Apple", "Nike", "FreshCatch", "Ranchers"]
PRODUCT_WORDS = ["Classic", "Family", "Value", "Premium", "Daily", "Select",
                 "Fresh", "Home", "Essential", "Bundle"]

CITIES = [
    ("CT01", "Seattle", "Washington", "98101", "R01", "Puget Sound", "DV1", "Northwest"),
    ("CT02", "Bellevue", "Washington", "98004", "R01", "Puget Sound", "DV1", "Northwest"),
    ("CT03", "Tacoma", "Washington", "98402", "R01", "Puget Sound", "DV1", "Northwest"),
    ("CT04", "Portland", "Oregon", "97201", "R02", "Columbia River", "DV1", "Northwest"),
    ("CT05", "Salem", "Oregon", "97301", "R02", "Columbia River", "DV1", "Northwest"),
    ("CT06", "Eugene", "Oregon", "97401", "R02", "Columbia River", "DV1", "Northwest"),
    ("CT07", "Boise", "Idaho", "83702", "R03", "Snake River", "DV1", "Northwest"),
    ("CT08", "Spokane", "Washington", "99201", "R03", "Snake River", "DV1", "Northwest"),
    ("CT09", "San Francisco", "California", "94102", "R04", "Bay Area", "DV2", "West"),
    ("CT10", "Oakland", "California", "94607", "R04", "Bay Area", "DV2", "West"),
    ("CT11", "San Jose", "California", "95112", "R04", "Bay Area", "DV2", "West"),
    ("CT12", "Los Angeles", "California", "90012", "R05", "South Coast", "DV2", "West"),
    ("CT13", "San Diego", "California", "92101", "R05", "South Coast", "DV2", "West"),
    ("CT14", "Denver", "Colorado", "80202", "R06", "Front Range", "DV3", "Mountain"),
    ("CT15", "Boulder", "Colorado", "80301", "R06", "Front Range", "DV3", "Mountain"),
    ("CT16", "Phoenix", "Arizona", "85004", "R07", "Valley", "DV3", "Mountain"),
    ("CT17", "Tucson", "Arizona", "85701", "R07", "Valley", "DV3", "Mountain"),
    ("CT18", "Chicago", "Illinois", "60601", "R08", "Great Lakes", "DV4", "Midwest"),
    ("CT19", "Milwaukee", "Wisconsin", "53202", "R08", "Great Lakes", "DV4", "Midwest"),
    ("CT20", "Minneapolis", "Minnesota", "55401", "R09", "North Star", "DV4", "Midwest"),
    ("CT21", "Dallas", "Texas", "75201", "R10", "North Texas", "DV5", "South"),
    ("CT22", "Austin", "Texas", "78701", "R10", "North Texas", "DV5", "South"),
    ("CT23", "Houston", "Texas", "77002", "R11", "Gulf", "DV5", "South"),
    ("CT24", "Atlanta", "Georgia", "30303", "R12", "Piedmont", "DV5", "South"),
    ("CT25", "Miami", "Florida", "33130", "R12", "Piedmont", "DV5", "South"),
]
LOCN_TYPES = ["WAREHOUSE", "BUSINESS", "WAREHOUSE", "BUSINESS", "WAREHOUSE"]
CUSTOMER_TYPES = ["GOLD_STAR", "EXECUTIVE", "BUSINESS"]
FIRST_NAMES = ["Ava", "Liam", "Mia", "Noah", "Zoe", "Ethan", "Ivy", "Lucas", "Nora", "Owen"]
LAST_NAMES = ["Smith", "Garcia", "Chen", "Patel", "Brown", "Kim", "Lopez", "Nguyen", "Khan", "Silva"]

ITEM_HEADER = ["ITEM_ID", "ITEM_NM", "ITEM_DESC", "BRAND_NM", "ITEM_TYPE",
               "SUBCATEGORY_ID", "SUBCATEGORY_NM", "SUBCATEGORY_DESC",
               "CATEGORY_ID", "CATEGORY_NM", "CATEGORY_DESC",
               "DEPARTMENT_ID", "DEPARTMENT_NM", "DEPARTMENT_DESC", "ACTIVE_FLG"]
STORE_HEADER = ["LOCN_ID", "LOCN_NM", "LOCN_TYPE", "ADDRESS_TXT", "POSTAL_CD",
                "CITY_ID", "CITY_NM", "STATE_PROVINCE_NM", "COUNTRY_NM",
                "REGION_ID", "REGION_NM", "DIVISION_ID", "DIVISION_NM", "ACTIVE_FLG"]
SALES_HEADER = ["TRANSACTION_ID", "LINE_NUM", "SALE_DT", "LOCN_ID", "ITEM_ID",
                "CUSTOMER_ID", "CUSTOMER_NM", "CUSTOMER_TYPE", "EMAIL_TXT", "PHONE_NUM",
                "SOLD_QTY", "UNIT_PRICE_AMT", "DISCOUNT_AMT", "SALES_AMT", "TAX_AMT", "NET_SALES_AMT"]


def build_items(target=100):
    rows, base_prices = [], {}
    item_num = 1
    while len(rows) < target:
        for dept_id, dept_nm, item_type, categories in DEPARTMENTS:
            for cat_id, cat_nm, subcategories in categories:
                for sub_id, sub_nm in subcategories:
                    if len(rows) >= target:
                        break
                    item_id = f"{100000 + item_num}"
                    word = PRODUCT_WORDS[item_num % len(PRODUCT_WORDS)]
                    rows.append([
                        item_id, f"{word} {sub_nm} {item_num}", f"{word} {sub_nm}, pack {item_num % 6 + 1}",
                        BRANDS[item_num % len(BRANDS)], item_type,
                        sub_id, sub_nm, f"{sub_nm} products",
                        cat_id, cat_nm, f"{cat_nm} category",
                        dept_id, dept_nm, f"{dept_nm} department", "Y",
                    ])
                    unit_price = round(1.75 + (item_num % 40) * 0.75 + (item_num % 7) * 0.1, 2)
                    base_prices[item_id] = (unit_price, item_type, item_num)
                    item_num += 1
    rows.append(["100101", "Premium Home Theater Bundle", "Premium Home Theater Bundle, 7.1 channel",
                 "Sony", "NON_FOOD", "SC302", "Cables", "Cables products",
                 "C30", "Electronics Accessories", "Electronics Accessories category",
                 "D02", "General Merchandise", "General Merchandise department", "Y"])
    base_prices["100101"] = (2499.99, "NON_FOOD", 101)
    base_prices["100045"] = (0.05, base_prices["100045"][1], base_prices["100045"][2])
    return rows, base_prices


def build_stores():
    rows = []
    for i, (city_id, city, state, postal, reg_id, reg_nm, div_id, div_nm) in enumerate(CITIES, start=1):
        rows.append([
            f"{1000 + i}", f"{city} {'North' if i % 2 else 'Central'}", LOCN_TYPES[i % len(LOCN_TYPES)],
            f"{100 + i * 7} Main St", postal,
            city_id, city, state, "United States",
            reg_id, reg_nm, div_id, div_nm, "Y",
        ])
    return rows


def customer(n):
    first = FIRST_NAMES[n % len(FIRST_NAMES)]
    last = LAST_NAMES[(n // len(FIRST_NAMES)) % len(LAST_NAMES)]
    return [
        f"M{n:06d}", f"{first} {last}", CUSTOMER_TYPES[n % len(CUSTOMER_TYPES)],
        f"{first.lower()}.{last.lower()}{n}@example.com", f"555-{n:04d}",
    ]


def stocked_item_ids(items, store_index: int) -> list[str]:
    """Same rotating 60-item window inventory uses for this store."""
    start = (store_index * 7) % len(items)
    ids = [items[(start + k) % len(items)][0] for k in range(STOCKED_PER_STORE)]
    # Premium is always sellable at two stores (forced into stock + inventory).
    locn_id = f"{1000 + store_index + 1}"
    if locn_id in ("1001", "1013") and "100101" not in ids:
        ids.append("100101")
    return ids


def build_stocked_by_store(items, stores) -> dict[str, list[str]]:
    return {
        store[0]: stocked_item_ids(items, s_i)
        for s_i, store in enumerate(stores)
    }


def txns_for_store(store_index: int) -> int:
    """Vary traffic by store; pattern sums to 400 txns across 25 stores."""
    return TXNS_PATTERN[store_index % len(TXNS_PATTERN)]


def prices_for_day(base_prices: dict, d: date) -> dict:
    """Shelf + promo for day D. Light Nov lift + sparse promos (pipeline days only)."""
    inflate = 1.02 if d.month == 11 else 1.0
    out = {}
    for item_id, (unit, item_type, item_num) in base_prices.items():
        shelf = round(unit * inflate, 2)
        promo = None
        if item_num % 10 == 0:
            promo = round(shelf * 0.90, 2)
        elif item_num % 5 == 0 and (d.day % 2 == 0):
            promo = round(shelf * 0.92, 2)
        elif d.month == 11 and item_num % 7 == 0:
            promo = round(shelf * 0.88, 2)
        out[item_id] = (shelf, promo, item_type)
    return out


def build_sales(d: date, stores, prices, stocked_by_store: dict[str, list[str]]):
    """Sell only items in that store's stocked list (no accidental inventory orphans)."""
    rows = []
    date_nodash = d.strftime("%Y%m%d")
    iso = d.isoformat()
    for s_i, store in enumerate(stores):
        locn_id = store[0]
        stocked = stocked_by_store[locn_id]
        n_txn = txns_for_store(s_i)
        for txn_n in range(1, n_txn + 1):
            txn_id = f"T{date_nodash}-{locn_id}-{txn_n:04d}"
            member = ["", "", "", "", ""] if txn_n % 5 == 0 else customer((txn_n * 7 + int(locn_id)) % 500 + 1)
            for line in range(1, 2 + (txn_n % 3)):
                item_id = stocked[(txn_n * 3 + line + int(locn_id) + d.day) % len(stocked)]
                unit_price, promo_price, item_type = prices[item_id]
                price = promo_price if promo_price is not None else unit_price
                if item_type == "PERISHABLE":
                    qty = 0.5 + ((txn_n + line + d.day) % 5) * 0.25
                else:
                    qty = 1 + (txn_n + line + d.month) % 5
                if d.month == 11 and item_type != "PERISHABLE" and txn_n % 11 == 0:
                    qty += 2
                sales_amt = round(qty * price, 2)
                discount = 0.0 if (txn_n + line) % 4 else min(round(0.25 * (txn_n % 5 + 1), 2), sales_amt)
                net = round(sales_amt - discount, 2)
                tax = round(net * TAX_RATE, 2)
                rows.append([
                    txn_id, str(line), iso, locn_id, item_id, *member,
                    f"{qty:.3f}", f"{price:.2f}", f"{discount:.2f}",
                    f"{sales_amt:.2f}", f"{tax:.2f}", f"{net:.2f}",
                ])
    return rows


def init_stock(items, stores, stocked_by_store: dict[str, list[str]]) -> dict:
    stock = {}
    for s_i, store in enumerate(stores):
        locn_id = store[0]
        for k, item_id in enumerate(stocked_by_store[locn_id]):
            on_hand = 40.0 + ((s_i + k) * 3) % 160
            stock[(item_id, locn_id)] = on_hand
    stock[("100101", "1001")] = 4.0
    stock[("100101", "1013")] = 4.0
    return stock


def stable_mix(item_id: str, locn_id: str, day_ordinal: int) -> int:
    return (int(item_id) * 17 + int(locn_id) * 31 + day_ordinal) % 10_007


def evolve_stock(stock: dict, sales_rows: list, d: date) -> None:
    """Drain sold qty, then small replenishment. Cap day-over-day move for G8 later."""
    sold = {}
    for r in sales_rows:
        key = (r[4], r[3])
        sold[key] = sold.get(key, 0.0) + float(r[10])

    for key, on_hand in list(stock.items()):
        drain = sold.get(key, 0.0)
        after_sale = max(0.0, on_hand - drain)
        replenish = 2.0 + (stable_mix(key[0], key[1], d.toordinal()) % 5)
        if after_sale < 25:
            replenish += 8.0
        candidate = after_sale + replenish
        if on_hand > 0:
            candidate = min(max(candidate, on_hand * 0.88), on_hand * 1.12)
        stock[key] = round(candidate, 3)


def stock_as_of(d: date, items, stores, stocked_by_store, base_prices) -> dict:
    """Stock at the start of day d, replayed from STOCK_ANCHOR.

    Replaying (instead of carrying stock between loop days) makes day d's
    inventory file identical whether you generate one day or a whole window.
    """
    stock = init_stock(items, stores, stocked_by_store)
    day = STOCK_ANCHOR
    while day < d:
        prices = prices_for_day(base_prices, day)
        sales = build_sales(day, stores, prices, stocked_by_store)
        inventory = build_inventory(day, stock, prices, stocked_by_store)
        add_pipeline_outliers_and_anomalies(day, sales, inventory, prices)
        evolve_stock(
            stock,
            [r for r in sales if r[4] and r[4] != ORPHAN_ITEM_ID and float(r[10]) > 0],
            day,
        )
        day += timedelta(days=1)
    return stock


def build_inventory(d: date, stock: dict, prices: dict, stocked_by_store: dict[str, list[str]]) -> list:
    rows = []
    iso = d.isoformat()
    for locn_id, item_ids in stocked_by_store.items():
        for k, item_id in enumerate(item_ids):
            on_hand = stock.get((item_id, locn_id), 40.0)
            unit_price, promo_price, _ = prices[item_id]
            reserved = 0.0 if k % 3 else min(on_hand, round(on_hand / 10, 3))
            in_transit = 0.0 if k % 4 else 6.0 + (k % 5) * 3
            rows.append([
                iso, item_id, locn_id,
                f"{on_hand:.3f}", f"{reserved:.3f}", f"{in_transit:.3f}",
                f"{unit_price:.2f}", "" if promo_price is None else f"{promo_price:.2f}",
            ])
    return rows


def item_master_for_day(items: list, d: date) -> list:
    rows = [row[:] for row in items]
    # SCD1 desc change from SCD1_FROM onward (DIM_ITEM.ITEM_DESC updated in place).
    if d >= SCD1_FROM:
        rows[10][2] = f"{rows[10][2]} (rev Nov)"
    return rows


# Planted orphan ITEM_ID — never appears in item_master (G7).
ORPHAN_ITEM_ID = "199999"
# Store that moves city on/after SCD2_FROM (SCD2 on DIM_LOCN). Brief "district" = CITY_ID.
SCD2_LOCN_ID = "1008"


def defect_bucket(d: date) -> int:
    """Exclusive defect class for this calendar day (0..4)."""
    return d.toordinal() % 5


def append_defect_log(lines: list[str], *, log_path: Path | None = None) -> Path:
    """Append one timestamped line per defect to data/store/defect_log.txt."""
    if not lines:
        return log_path or DEFECT_LOG_PATH
    path = log_path or DEFECT_LOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(f"{ts}\t{line}\n")
    return path


def set_sales_line(row, qty=None, discount=None, item_id=None, price=None):
    if item_id is not None:
        row[4] = item_id
    qty = float(row[10]) if qty is None else qty
    price = float(row[11]) if price is None else price
    sales_amt = round(qty * price, 2)
    # Allow negative qty defects: do not clamp discount against a negative sales_amt.
    raw_discount = float(row[12]) if discount is None else discount
    discount = raw_discount if sales_amt < 0 else min(raw_discount, sales_amt)
    net = round(sales_amt - discount, 2)
    row[10], row[11], row[12], row[13] = f"{qty:.3f}", f"{price:.2f}", f"{discount:.2f}", f"{sales_amt:.2f}"
    row[14], row[15] = f"{round(net * TAX_RATE, 2):.2f}", f"{net:.2f}"


def first_row_at(sales, locn_id: str, skip: int = 0):
    seen = 0
    for row in sales:
        if row[3] == locn_id:
            if seen == skip:
                return row
            seen += 1
    raise RuntimeError(f"no sales row for store {locn_id}")


def add_pipeline_outliers_and_anomalies(d: date, sales, inventory, prices) -> list[str]:
    """Bucket 0 only. Premium edits stay on stores that stock 100101."""
    if defect_bucket(d) != 0:
        return []
    notes = []
    set_sales_line(first_row_at(sales, "1001", 0), item_id="100101", qty=1, price=2499.99)
    set_sales_line(first_row_at(sales, "1013", 0), item_id="100101", qty=1, price=2499.99)
    set_sales_line(sales[100], qty=480)
    set_sales_line(sales[200], discount=10_000)
    weighed = [r for r in sales if prices.get(r[4], (None, None, None))[2] == "PERISHABLE"]
    if weighed:
        set_sales_line(weighed[min(20, len(weighed) - 1)], qty=0.005, discount=0)
    inventory[10][3:5] = ["0.000", "0.000"]
    inventory[20][3] = "25000.000"
    inventory[30][4] = inventory[30][3]
    # Anomalies (G12) — indices fit ~800-line days
    set_sales_line(first_row_at(sales, "1001", 5), item_id="100101", qty=1, price=2_499_990.00)
    set_sales_line(sales[400], qty=10_000)
    normal = [r for r in sales[500:] if float(r[11]) >= 5]
    if normal:
        set_sales_line(normal[0], price=round(float(normal[0][11]) / 100, 2))
    inventory[40][3] = "9999999.000"
    notes.append(
        f"{d.isoformat()} store=1001,1013 bucket=0 outliers/anomalies "
        f"(premium/qty/discount/inventory spikes)"
    )
    return notes


def plant_defects(d: date, sales: list) -> list[str]:
    """One exclusive transaction-defect class from ``defect_bucket(d)``. Mutates sales."""
    notes = []
    b = defect_bucket(d)
    if b == 1:
        # null ITEM_ID → G3 (store 1001 has the fewest txns in the pattern = 12)
        r_a = first_row_at(sales, "1001", 5)
        r_b = first_row_at(sales, "1001", 6)
        r_a[4] = ""
        r_b[4] = ""
        notes.append(
            f"{d.isoformat()} store=1001 bucket=1 D1 null ITEM_ID "
            f"on {r_a[0]} L{r_a[1]} and {r_b[0]} L{r_b[1]}"
        )
    elif b == 2:
        r_neg = first_row_at(sales, "1001", 10)
        set_sales_line(r_neg, qty=-2.0, discount=0.0)
        notes.append(
            f"{d.isoformat()} store=1001 bucket=2 D2 negative qty "
            f"on {r_neg[0]} L{r_neg[1]}"
        )
    elif b == 3:
        r_orph = first_row_at(sales, "1002", 5)
        set_sales_line(r_orph, item_id=ORPHAN_ITEM_ID, qty=1.0, price=9.99, discount=0.0)
        notes.append(
            f"{d.isoformat()} store=1002 bucket=3 D3 orphan ITEM_ID {ORPHAN_ITEM_ID} "
            f"on {r_orph[0]} L{r_orph[1]}"
        )
    elif b == 4:
        r_dup = first_row_at(sales, "1001", 15)
        dup = r_dup[:]
        set_sales_line(dup, qty=float(dup[10]) + 1, discount=0.0)
        sales.append(dup)
        notes.append(
            f"{d.isoformat()} store=1001 bucket=4 D4 duplicate line "
            f"{dup[0]} L{dup[1]} (second physical row)"
        )
    return notes


def store_master_for_day(stores: list, d: date, *, scd2_from: date | None = None) -> list:
    """D5: LOCN 1008 moves CITY_ID from scd2_from onward (default SCD2_FROM) → SCD2 on DIM_LOCN."""
    rows = [r[:] for r in stores]
    start = scd2_from or SCD2_FROM
    if d >= start:
        for r in rows:
            if r[0] == SCD2_LOCN_ID:
                # Was Spokane CT08 / R03; move under Portland CT04 / R02 (same division).
                r[4] = "97201"
                r[5] = "CT04"
                r[6] = "Portland"
                r[7] = "Oregon"
                r[9] = "R02"
                r[10] = "Columbia River"
                r[1] = "Portland Relocated"
    return rows


def unexpected_unstocked_count(sales, stocked_by_store: dict[str, list[str]]) -> int:
    """Clean sales must be stocked; allow empty ITEM_ID and the planted orphan id."""
    bad = 0
    for r in sales:
        item_id = r[4]
        if item_id == "" or item_id == ORPHAN_ITEM_ID:
            continue
        if item_id not in stocked_by_store[r[3]]:
            bad += 1
    return bad


def write_csv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        if header:
            writer.writerow(header)
        writer.writerows(rows)


def generate_pipeline_days(
    dates: list[date] | None = None,
    *,
    landing_dir: Path | None = None,
    defect_log_path: Path | None = None,
    scd2_from: date | None = None,
) -> list[dict]:
    """Write four CSVs per day under landing_dir. Returns per-day summary dicts.

    Defects use ``toordinal() % 6`` (calendar), not window index. SCD2 uses
    ``scd2_from`` (default ``SCD2_FROM``). Appends to ``data/store/defect_log.txt``.
    """
    dates = list(dates) if dates is not None else list(PIPELINE_DATES)
    if len(dates) < 1:
        raise ValueError("dates must be non-empty")
    out_dir = landing_dir or LANDING_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    scd2_start = scd2_from or SCD2_FROM
    # Prefer data/store next to landing (Compose volume); else project default.
    log_file = defect_log_path or (out_dir.parent / "store" / "defect_log.txt")

    items, base_prices = build_items(100)
    stores = build_stores()
    stocked_by_store = build_stocked_by_store(items, stores)

    summary = []
    defect_log: list[str] = []
    for d in dates:
        stock = stock_as_of(d, items, stores, stocked_by_store, base_prices)
        prices = prices_for_day(base_prices, d)
        sales = build_sales(d, stores, prices, stocked_by_store)
        inventory = build_inventory(d, stock, prices, stocked_by_store)
        defect_log.extend(add_pipeline_outliers_and_anomalies(d, sales, inventory, prices))
        defect_log.extend(plant_defects(d, sales))
        bad = unexpected_unstocked_count(sales, stocked_by_store)
        if bad:
            raise RuntimeError(f"{d}: {bad} unexpected unstocked sales — fix stocked list")

        nodash = d.strftime("%Y%m%d")
        stores_out = store_master_for_day(stores, d, scd2_from=scd2_start)
        if d >= scd2_start:
            defect_log.append(
                f"{d.isoformat()} store={SCD2_LOCN_ID} D5 SCD2 CITY_ID move "
                f"(from {scd2_start.isoformat()} onward)"
            )
        write_csv(out_dir / f"sales_{nodash}.csv", SALES_HEADER, sales)
        write_csv(out_dir / f"inventory_{nodash}.csv", None, inventory)
        write_csv(
            out_dir / f"item_master_{nodash}.csv",
            ITEM_HEADER,
            item_master_for_day(items, d),
        )
        write_csv(out_dir / f"store_master_{nodash}.csv", STORE_HEADER, stores_out)

        net = round(sum(float(r[15]) for r in sales), 2)
        summary.append(
            {
                "ds": d.isoformat(),
                "ds_nodash": nodash,
                "sales_rows": len(sales),
                "inventory_rows": len(inventory),
                "net_sales_amt": net,
                "defect_bucket": defect_bucket(d),
            }
        )

    log_path = append_defect_log(defect_log, log_path=log_file)
    print("pipeline landing:")
    for row in summary:
        print(
            f"  {row['ds_nodash']}: sales={row['sales_rows']} "
            f"inventory={row['inventory_rows']} NET_SALES_AMT={row['net_sales_amt']:.2f} "
            f"bucket={row['defect_bucket']}"
        )
    print("planted defects:")
    for line in defect_log:
        print(f"  {line}")
    print(f"  D5 SCD2: {SCD2_LOCN_ID} CITY_ID move from {scd2_start.isoformat()} onward")
    print(f"defect log → {log_path}")
    print(f"wrote under {out_dir}")
    return summary


def main():
    generate_pipeline_days(PIPELINE_DATES)
    hist = ROOT / "data" / "history"
    if hist.exists():
        print(f"note: {hist} still exists from the old run — safe to delete; generator no longer writes it")


if __name__ == "__main__":
    main()

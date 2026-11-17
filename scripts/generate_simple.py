"""Lesson 2: write one day of source files to data/landing/.

Four daily drops, named and laid out as in context/architecture-Simple view.png:
    sales_YYYYMMDD.csv         POS lines + member attributes      -> STG_SALES
    inventory_YYYYMMDD.csv     item x location stock + prices     -> STG_INVENTORY  (no header)
    item_master_YYYYMMDD.csv   item > subcategory > category > department -> STG_ITEM_MASTER
    store_master_YYYYMMDD.csv  location > city > region > division        -> STG_STORE_MASTER

One business date, no defects, fully deterministic (same run = same files).

Run from the project folder:
    python scripts\\generate_simple.py
"""
import csv
from pathlib import Path

BUSINESS_DATE = "2026-11-10"
DATE_NODASH = BUSINESS_DATE.replace("-", "")
LANDING_DIR = Path(__file__).resolve().parent.parent / "data" / "landing"
TAX_RATE = 0.08

# --- product hierarchy: department > category > subcategory (items are built in a loop) ---
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

# --- location hierarchy: division > region > city (one store per city) ---
# (city_id, city, state, postal, region_id, region, division_id, division)
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
# inventory has NO header; this is the positional contract ($1..$8):
# SNAPSHOT_DT, ITEM_ID, LOCN_ID, ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY, UNIT_PRICE_AMT, PROMO_PRICE_AMT


def build_items(target=100):
    """100 items spread across the hierarchy. Also returns each item's shelf and promo price."""
    rows, prices = [], {}
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
                    promo_price = round(unit_price * 0.9, 2) if item_num % 10 == 0 else None
                    prices[item_id] = (unit_price, promo_price, item_type)
                    item_num += 1
    return rows, prices


def build_stores():
    """One store (LOCN_ID 1001..1025) per city."""
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
    """Member n (1..500). Returns the five member columns."""
    first = FIRST_NAMES[n % len(FIRST_NAMES)]
    last = LAST_NAMES[(n // len(FIRST_NAMES)) % len(LAST_NAMES)]
    return [
        f"M{n:06d}", f"{first} {last}", CUSTOMER_TYPES[n % len(CUSTOMER_TYPES)],
        f"{first.lower()}.{last.lower()}{n}@example.com", f"555-{n:04d}",
    ]


def build_sales(items, stores, prices):
    """
    100 transactions per store, 1-3 lines each -> 5,000 lines.
    Every 5th transaction is a non-member (customer columns empty -> NULL in Snowflake).
    PERISHABLE items are sold by weight, so SOLD_QTY can have decimals (e.g. 1.250).
    SALES_AMT = SOLD_QTY x UNIT_PRICE_AMT; NET_SALES_AMT = SALES_AMT - DISCOUNT_AMT; tax is separate.
    """
    rows = []
    for store in stores:
        locn_id = store[0]
        for txn_n in range(1, 101):
            txn_id = f"T{DATE_NODASH}-{locn_id}-{txn_n:04d}"
            member = ["", "", "", "", ""] if txn_n % 5 == 0 else customer((txn_n * 7 + int(locn_id)) % 500 + 1)
            for line in range(1, 2 + (txn_n % 3)):
                item_id = items[(txn_n * 3 + line + int(locn_id)) % len(items)][0]
                unit_price, promo_price, item_type = prices[item_id]
                price = promo_price if promo_price is not None else unit_price
                if item_type == "PERISHABLE":
                    qty = 0.5 + ((txn_n + line) % 5) * 0.25
                else:
                    qty = 1 + (txn_n + line) % 5
                sales_amt = round(qty * price, 2)
                discount = 0.0 if (txn_n + line) % 4 else min(round(0.25 * (txn_n % 5 + 1), 2), sales_amt)
                net = round(sales_amt - discount, 2)
                tax = round(net * TAX_RATE, 2)
                rows.append([
                    txn_id, str(line), BUSINESS_DATE, locn_id, item_id, *member,
                    f"{qty:.3f}", f"{price:.2f}", f"{discount:.2f}",
                    f"{sales_amt:.2f}", f"{tax:.2f}", f"{net:.2f}",
                ])
    return rows


def build_inventory(items, stores, prices):
    """Each store stocks 60 items (rotating window) -> 25 x 60 = 1,500 rows. RESERVED <= ON_HAND."""
    rows = []
    for s_i, store in enumerate(stores):
        start = (s_i * 7) % len(items)
        for k in range(60):
            item_id = items[(start + k) % len(items)][0]
            unit_price, promo_price, _ = prices[item_id]
            on_hand = 20 + ((s_i + k) * 3) % 200
            reserved = 0 if k % 3 else on_hand // 10
            in_transit = 0 if k % 4 else 6 + (k % 5) * 3
            rows.append([
                BUSINESS_DATE, item_id, store[0],
                f"{on_hand:.3f}", f"{reserved:.3f}", f"{in_transit:.3f}",
                f"{unit_price:.2f}", "" if promo_price is None else f"{promo_price:.2f}",
            ])
    return rows


def set_sales_line(row, qty=None, discount=None, item_id=None, price=None):
    """Change a sales row and recompute its amounts. The discount is capped so NET_SALES_AMT >= 0."""
    if item_id:
        row[4] = item_id
    qty = float(row[10]) if qty is None else qty
    price = float(row[11]) if price is None else price
    sales_amt = round(qty * price, 2)
    discount = min(float(row[12]) if discount is None else discount, sales_amt)
    net = round(sales_amt - discount, 2)
    row[10], row[11], row[12], row[13] = f"{qty:.3f}", f"{price:.2f}", f"{discount:.2f}", f"{sales_amt:.2f}"
    row[14], row[15] = f"{round(net * TAX_RATE, 2):.2f}", f"{net:.2f}"


def add_outliers(items, prices, sales, inventory):
    """
    Extreme but VALID values: they pass every gate, so the pipeline must keep them.
    Defects that must be quarantined come later, in Lesson 10.
    """
    # Premium item 100101 (2,499.99): stocked in 2 stores, sold on 2 lines only.
    items.append(["100101", "Premium Home Theater Bundle", "Premium Home Theater Bundle, 7.1 channel",
                  "Sony", "NON_FOOD", "SC302", "Cables", "Cables products",
                  "C30", "Electronics Accessories", "Electronics Accessories category",
                  "D02", "General Merchandise", "General Merchandise department", "Y"])
    prices["100101"] = (2499.99, None, "NON_FOOD")
    for locn_id in ["1001", "1013"]:
        inventory.append([BUSINESS_DATE, "100101", locn_id, "4.000", "1.000", "0.000", "2499.99", ""])
    set_sales_line(sales[0], item_id="100101", qty=1, price=2499.99)
    set_sales_line(sales[2400], item_id="100101", qty=1, price=2499.99)

    set_sales_line(sales[100], qty=480)                     # bulk business purchase
    set_sales_line(sales[200], discount=10_000)             # 100% discount: NET_SALES_AMT = 0.00
    weighed = [r for r in sales if prices[r[4]][2] == "PERISHABLE"]
    set_sales_line(weighed[50], qty=0.005, discount=0)      # tiny weight (5 g): smallest NUMBER(18,3) step
    inventory[10][3:5] = ["0.000", "0.000"]                 # out of stock
    inventory[20][3] = "25000.000"                          # huge pallet stock
    inventory[30][4] = inventory[30][3]                     # RESERVED = ON_HAND (edge of G6b)


def add_anomalies(sales, inventory):
    """
    Planted PROBLEMS that look fine to the arithmetic rule (G6 still holds) but are not real.
    Gate G12 (configurable limits in the medallion_config Variable) must find them in Lesson 7.
    """
    set_sales_line(sales[1000], item_id="100101", qty=1, price=2_499_990.00)  # A1 price x1000 (decimal slip)
    set_sales_line(sales[2000], qty=10_000)                                   # A2 fat-finger quantity
    normal = [r for r in sales[3000:] if float(r[11]) >= 5]
    set_sales_line(normal[0], price=round(float(normal[0][11]) / 100, 2))     # A3 price 100x too low
    inventory[40][3] = "9999999.000"                                          # A4 placeholder stock value


def write_csv(file_name, header, rows):
    path = LANDING_DIR / file_name
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        if header:
            writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {file_name}: {len(rows)} rows")


LANDING_DIR.mkdir(parents=True, exist_ok=True)
ITEMS, PRICES = build_items(100)
PRICES["100045"] = (0.05, None, PRICES["100045"][2])      # outlier: near-free item (price 0.05)
STORES = build_stores()
SALES = build_sales(ITEMS, STORES, PRICES)
INVENTORY = build_inventory(ITEMS, STORES, PRICES)
add_outliers(ITEMS, PRICES, SALES, INVENTORY)
add_anomalies(SALES, INVENTORY)

write_csv(f"sales_{DATE_NODASH}.csv", SALES_HEADER, SALES)
write_csv(f"inventory_{DATE_NODASH}.csv", None, INVENTORY)
write_csv(f"item_master_{DATE_NODASH}.csv", ITEM_HEADER, ITEMS)
write_csv(f"store_master_{DATE_NODASH}.csv", STORE_HEADER, STORES)

print(f"SALES_AMT total:     {sum(float(r[13]) for r in SALES):.2f}")
print(f"DISCOUNT_AMT total:  {sum(float(r[12]) for r in SALES):.2f}")
print(f"NET_SALES_AMT total: {sum(float(r[15]) for r in SALES):.2f}")
print(f"member lines: {sum(1 for r in SALES if r[5])} | non-member lines: {sum(1 for r in SALES if not r[5])}")
print(f"extremes: max SOLD_QTY {max(float(r[10]) for r in SALES):.3f} | min SOLD_QTY {min(float(r[10]) for r in SALES):.3f}"
      f" | max UNIT_PRICE_AMT {max(float(r[11]) for r in SALES):.2f} | zero-net lines {sum(1 for r in SALES if float(r[15]) == 0)}"
      f" | max ON_HAND_QTY {max(float(r[3]) for r in INVENTORY):.3f} | zero-stock rows {sum(1 for r in INVENTORY if float(r[3]) == 0)}")

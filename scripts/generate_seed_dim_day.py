"""Write the DIM_DAY calendar seed to data/seed/seed_dim_day.csv.

DIM_DAY is the one departure from the reference architecture: it has no source system, so it
is generated here and loaded straight into Gold (no STG or TMP copy). This script and its
output are version controlled, which is what keeps the calendar traceable.

Run from the project folder:
    python scripts\\generate_seed_dim_day.py
"""
import csv
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 1, 1)
END = date(2027, 12, 31)
SEED_FILE = Path(__file__).resolve().parent.parent / "data" / "seed" / "seed_dim_day.csv"

# US holidays the retailer closes or runs promos on (fixed list keeps it readable).
HOLIDAYS = {
    date(2026, 1, 1), date(2026, 5, 25), date(2026, 7, 4), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    date(2027, 1, 1), date(2027, 5, 31), date(2027, 7, 4), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 25),
}

HEADER = ["DAY_KY", "CALENDAR_DT", "DAY_OF_MONTH", "DAY_OF_WEEK_NUM", "DAY_OF_WEEK_NM",
          "WEEK_OF_YEAR_NUM", "MONTH_NUM", "MONTH_NM", "QUARTER_NUM", "YEAR_NUM",
          "WEEKEND_FLG", "HOLIDAY_FLG"]

rows = []
day = START
while day <= END:
    iso_year, iso_week, iso_weekday = day.isocalendar()
    rows.append([
        day.strftime("%Y%m%d"), day.isoformat(), day.day, iso_weekday, day.strftime("%A"),
        iso_week, day.month, day.strftime("%B"), (day.month - 1) // 3 + 1, day.year,
        "Y" if iso_weekday >= 6 else "N", "Y" if day in HOLIDAYS else "N",
    ])
    day += timedelta(days=1)

SEED_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(SEED_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerows(rows)
print(f"wrote {SEED_FILE.name}: {len(rows)} rows ({START} to {END})")

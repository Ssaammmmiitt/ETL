"""Names shared by every DAG.

Only ids live here. Credentials are in Airflow Connections; database and schema names are in
the `medallion_config` Variable; table names are in the SQL files.
"""
from pathlib import Path

SNOWFLAKE_CONN_ID = "snowflake_medallion"
LANDING_FS_CONN_ID = "fs_landing"
SMTP_CONN_ID = "smtp_default"
CONFIG_VARIABLE = "medallion_config"

# Folder with all SQL files, found relative to this file so no absolute path is hard-coded.
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# The four daily drops, by file prefix: <source>_YYYYMMDD.csv lands in STG_<SOURCE>
# (see context/architecture-Simple view.png). DAGs build one task per source from this list
# when the file is parsed, so it is code, not a Variable.
SOURCES = ["sales", "inventory", "item_master", "store_master"]

# FileSensor (gate G1). The timeout is the arrival deadline for a day's files; it is short so
# you can watch a timeout happen while learning (production would be hours).
SENSOR_POKE_SECONDS = 60
SENSOR_TIMEOUT_SECONDS = 10 * 60

# Gate G2: expected field counts (and whether the file has a header). Checked before COPY.
SOURCE_CONTRACTS = {
    "sales": {"columns": 16, "has_header": True},
    "inventory": {"columns": 8, "has_header": False},
    "item_master": {"columns": 15, "has_header": True},
    "store_master": {"columns": 14, "has_header": True},
}

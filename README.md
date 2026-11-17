# airflow-medallion

Airflow 3.3.2 DAG that loads five business days of retail files (10, 11, 12, 13 and 16 Nov 2026)
through Bronze (STG), Silver (TMP) and Gold (TGT + Aggregate) in Snowflake, and reconciles Gold
net sales back to source.

![Architecture, simple view](context/architecture-Simple%20view.png)

## Start here

All documentation is in [`context/`](context/):

1. [architecture-Simple view.png](context/architecture-Simple%20view.png) with
   [architecture_simple.md](context/architecture_simple.md): the target architecture and naming convention.
2. [LESSONS.md](context/LESSONS.md): goal, running checklist, lesson plan.
3. [ASSUMPTIONS.md](context/ASSUMPTIONS.md): project assumptions, gates and departures (short).
4. [THEORY.md](context/THEORY.md): read the sections listed for the current lesson.
5. The current lesson in [context/lessons/](context/lessons/) (now
   [02_stg_tables.md](context/lessons/02_stg_tables.md)): follow the steps, then run the checkpoint.

To continue in a new chat with an agent, start from [AGENT_CONTEXT.md](context/AGENT_CONTEXT.md).

## Layers and tables

| Table type | Schema | Tables |
|---|---|---|
| STG (Bronze) | `BRONZE` | `STG_SALES`, `STG_INVENTORY`, `STG_ITEM_MASTER`, `STG_STORE_MASTER` |
| TMP (Silver) | `SILVER` | `TMP_SALES_LINE`, `TMP_CUSTOMER`, `TMP_ITEM_LOC_SNAPSHOT`, `TMP_ITEM_HIER`, `TMP_LOCN_HIER`, `TMP_QUARANTINE` |
| TGT (Gold) | `GOLD` | 10 `DIM_*`, `FCT_SALES`, `FCT_INVENTORY`, `FCT_PRICE` |
| Aggregate (Gold) | `GOLD` | `AGG_DAILY_ITEM_LOCN_SALES`, `VW_DAILY_ITEM_LOCN_SALES` |

## How to run

Set-up steps are in [context/lessons/01_environment.md](context/lessons/01_environment.md):
Fernet key, Snowflake key pair and bootstrap, connections, Variable, `docker compose up`.

## Sections still to be written

| Section | Written in |
|---|---|
| How to backfill | Lesson 13 |
| Known limitations | Lesson 14 |
| Evidence: backfill screenshot description | Lesson 13 |
| Evidence: reconciliation output (2 columns × 5 rows) | Lesson 11 |

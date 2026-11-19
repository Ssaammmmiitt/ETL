# Progress snapshot

Last updated: **19 Nov 2026**.

## Where we are

Lessons **1–11 are done**. Lesson **12 is in progress** (idempotency proof + reload of day D).

Current lesson file: [`lessons/12_idempotency.md`](lessons/12_idempotency.md).

## Completed by lesson

| Lesson | What you can do now |
|---|---|
| **1** Environment | Airflow 3.3.2 Compose; Snowflake; key-pair; connections + Variable; `01_connection_check` |
| **2** Simple data + STG | Generators, file formats, 4 STG DDL; `02_stg_setup` |
| **3** Stage + COPY | Internal stage PUT/LIST/COPY; positional inventory; `03_stage_load` |
| **4** Waiting (G1) | Mapped FileSensor; assert on stage; `04_waiting` |
| **5** Bronze | Idempotent DELETE+FORCE, G2, archive; `05_bronze_load` |
| **6** Complex data | Five days ~810 lines; txns 12/14/16/18/20; D1–D5 planted; `generate_complex.py` |
| **7** Silver | TMP + quarantine; G3–G6, G12; identity; `07_silver` |
| **8** Gold dims | Sequences; 10 DIMs; SCD2/SCD1/seed; `08_gold_dims` |
| **9** Gold facts | `FCT_SALES` MERGE; soft-close inventory; price periods; G7/G8; `09_gold_facts` |
| **10** Prove defects | D1–D5 asserts; `pk_*.sql`; G10; `10_prove_defects` |
| **11** Agg/recon | AGG + VW; G9; G11 recon; return-flag branch; Mailpit optional |
| **12** Idempotency (in progress) | Fingerprint + `12_idempotency` BEFORE→re-apply→AFTER; reload policy |

## Facts to keep

| Item | Value |
|---|---|
| Pipeline dates | 2026-11-10, 11, 12, 13, 16 |
| Complex volume | ~810 sales lines (811 on Nov 11); 400 txns; inventory 1501 |
| Nov 10 after G12 | FCT_SALES 807; inventory/price current 1500 |
| History | Deferred |
| DAG naming | `dags/NN_<topic>.py` |

## Still ahead (short)

Finish L12 → 13 backfill → 14 README.

Full checklist: [`LESSONS.md`](LESSONS.md). Agent handoff: [`AGENT_CONTEXT.md`](AGENT_CONTEXT.md).

-- The four layer schemas. Run by setup_platform every day, so a dropped schema comes back on the next run.
-- MEDALLION_ROLE holds CREATE SCHEMA on the database (granted once in 00_bootstrap.sql).
CREATE SCHEMA IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}
    COMMENT = 'STG tables: raw landing, no transformation';
CREATE SCHEMA IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}
    COMMENT = 'TMP tables: clean and compare against target';
CREATE SCHEMA IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}
    COMMENT = 'TGT (DIM_, FCT_) and Aggregate (AGG_, VW_)';
CREATE SCHEMA IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}
    COMMENT = 'Control tables (C_), rejected rows and reconciliation';

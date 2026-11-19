-- Surrogate key sequences for Gold dimensions (one NEXTVAL per new SCD2 version / new SCD1 row).
-- We pick SEQUENCE (not HASH) so keys stay short NUMBER(38,0); inserts are guarded with NOT EXISTS
-- so a re-run does not burn new values. See THEORY 4.2.
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_DEPARTMENT_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CATEGORY_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_SUBCATEGORY_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_ITEM_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_DIVISION_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_REGION_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CITY_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_LOCN_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_CUSTOMER_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_SALES_LINE_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_INVENTORY_KY;
CREATE SEQUENCE IF NOT EXISTS {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_PRICE_KY;

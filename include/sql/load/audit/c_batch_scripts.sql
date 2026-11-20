-- C_BATCH_SCRIPTS: the run order of retail_medallion_daily. tests/run_all.py fails if a DAG task is missing here.
-- ON_FAIL: FAIL = task fails the run, REJECT = bad rows to C_REJ_TABLE and the run continues, ALERT = logged only.
MERGE INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_BATCH_SCRIPTS t
USING (
    SELECT * FROM (VALUES
        ( 10, 'setup_platform',                          'CONTROL', 'setup/*.sql + ddl/<layer>/*.sql',                 NULL,                                                      NULL,                    'FAIL'),
        ( 20, 'load_c_batch_scripts',                    'CONTROL', 'load/audit/c_batch_scripts.sql',                  'C_BATCH_SCRIPTS',                                         NULL,                    'FAIL'),
        ( 30, 'load_c_param',                            'CONTROL', 'load/audit/c_param.sql',                          'C_PARAM',                                                 NULL,                    'FAIL'),
        ( 40, 'generate_landing_day',                    'SOURCE',  'scripts/generate_complex.py',                     NULL,                                                      NULL,                    'FAIL'),
        ( 50, 'wait_for_files',                          'SOURCE',  'FileSensor <source>_YYYYMMDD.csv',                NULL,                                                      'G1 arrival',            'FAIL'),
        ( 60, 'bronze.ingest_one',                       'BRONZE',  'stage/put_file.sql + load/bronze/stg_<source>.sql', 'STG_SALES,STG_INVENTORY,STG_ITEM_MASTER,STG_STORE_MASTER', 'G2 columns, G1 rows', 'FAIL'),
        ( 70, 'bronze.assert_stage_g1',                  'BRONZE',  'checks/list_stage.sql',                           NULL,                                                      'G1 stage',              'FAIL'),
        ( 80, 'bronze.archive_landing',                  'BRONZE',  'python: move landing files to archive/<ds>/',     NULL,                                                      NULL,                    'FAIL'),
        ( 90, 'silver.load_tmp_sales_line',              'SILVER',  'load/silver/tmp_sales_line.sql',                  'TMP_SALES_LINE',                                          'G3,G4,G5,G6,G12',       'REJECT'),
        (100, 'silver.load_tmp_customer',                'SILVER',  'load/silver/tmp_customer.sql',                    'TMP_CUSTOMER',                                            NULL,                    'FAIL'),
        (110, 'silver.load_tmp_item_loc_snapshot',       'SILVER',  'load/silver/tmp_item_loc_snapshot.sql',           'TMP_ITEM_LOC_SNAPSHOT',                                   'G3,G12',                'REJECT'),
        (120, 'silver.g06b_alert',                       'SILVER',  'gates/g06b_reserved_vs_on_hand.sql',              NULL,                                                      'G6b',                   'ALERT'),
        (130, 'silver.load_tmp_item_hier',               'SILVER',  'load/silver/tmp_item_hier.sql',                   'TMP_ITEM_HIER',                                           NULL,                    'FAIL'),
        (140, 'silver.load_tmp_locn_hier',               'SILVER',  'load/silver/tmp_locn_hier.sql',                   'TMP_LOCN_HIER',                                           NULL,                    'FAIL'),
        (150, 'silver.assert_sales_identity',            'SILVER',  'checks/silver_identity_sales.sql',                NULL,                                                      'STG = TMP + QUAR',      'FAIL'),
        (160, 'gold_dims.rewind_gold_for_reload',        'GOLD',    'checks/gold_high_water.sql + load/gold/rewind_from_ds.sql', NULL,                                        'reload of a past day',  'FAIL'),
        (170, 'gold_dims.put_seed_dim_day',              'GOLD',    'stage/put_seed_dim_day.sql',                      NULL,                                                      NULL,                    'FAIL'),
        (180, 'gold_dims.load_dim_day',                  'GOLD',    'load/gold/dim_day.sql',                           'DIM_DAY',                                                 NULL,                    'FAIL'),
        (190, 'gold_dims.load_dim_department',           'GOLD',    'load/gold/dim_department.sql',                    'DIM_DEPARTMENT',                                          NULL,                    'FAIL'),
        (200, 'gold_dims.load_dim_category',             'GOLD',    'load/gold/dim_category.sql',                      'DIM_CATEGORY',                                            NULL,                    'FAIL'),
        (210, 'gold_dims.load_dim_subcategory',          'GOLD',    'load/gold/dim_subcategory.sql',                   'DIM_SUBCATEGORY',                                         NULL,                    'FAIL'),
        (220, 'gold_dims.load_dim_item',                 'GOLD',    'load/gold/dim_item.sql',                          'DIM_ITEM',                                                NULL,                    'FAIL'),
        (230, 'gold_dims.load_dim_division',             'GOLD',    'load/gold/dim_division.sql',                      'DIM_DIVISION',                                            NULL,                    'FAIL'),
        (240, 'gold_dims.load_dim_region',               'GOLD',    'load/gold/dim_region.sql',                        'DIM_REGION',                                              NULL,                    'FAIL'),
        (250, 'gold_dims.load_dim_city',                 'GOLD',    'load/gold/dim_city.sql',                          'DIM_CITY',                                                NULL,                    'FAIL'),
        (260, 'gold_dims.load_dim_locn',                 'GOLD',    'load/gold/dim_locn.sql',                          'DIM_LOCN',                                                NULL,                    'FAIL'),
        (270, 'gold_dims.load_dim_customer',             'GOLD',    'load/gold/dim_customer.sql',                      'DIM_CUSTOMER',                                            NULL,                    'FAIL'),
        (280, 'gold_facts.g07_keys_resolve',             'GOLD',    'gates/g07_keys_resolve.sql',                      NULL,                                                      'G7',                    'REJECT'),
        (290, 'gold_facts.g07_alert',                    'GOLD',    'checks/g07_orphan_summary.sql',                   NULL,                                                      'G7',                    'ALERT'),
        (300, 'gold_facts.load_fct_sales',               'GOLD',    'load/gold/fct_sales.sql',                         'FCT_SALES',                                               NULL,                    'FAIL'),
        (310, 'gold_facts.g08_inventory_count',          'GOLD',    'gates/g08_inventory_count.sql',                   NULL,                                                      'G8',                    'FAIL'),
        (320, 'gold_facts.load_fct_inventory',           'GOLD',    'load/gold/fct_inventory.sql',                     'FCT_INVENTORY',                                           NULL,                    'FAIL'),
        (330, 'gold_facts.load_fct_price',               'GOLD',    'load/gold/fct_price.sql',                         'FCT_PRICE',                                               NULL,                    'FAIL'),
        (340, 'gold_facts.g10_price_overlap',            'GOLD',    'gates/g10_price_overlap.sql',                     NULL,                                                      'G10',                   'FAIL'),
        (350, 'reconcile.load_agg_daily_item_locn_sales','GOLD',    'load/gold/agg_daily_item_locn_sales.sql',         'AGG_DAILY_ITEM_LOCN_SALES',                               NULL,                    'FAIL'),
        (360, 'reconcile.load_recon_daily_checks',       'AUDIT',   'load/audit/recon_daily_checks.sql',               'RECON_DAILY_CHECKS',                                      'G11',                   'FAIL'),
        (370, 'reconcile.assert_reconciliation',         'AUDIT',   'gates/g09_agg_equals_fct.sql + checks/recon_daily_checks.sql', NULL,                                         'G9,G11',                'FAIL'),
        (380, 'reconcile.assert_primary_keys',           'AUDIT',   'gates/pk_*.sql',                                  NULL,                                                      'PK',                    'FAIL'),
        (390, 'load_c_load_dates',                       'CONTROL', 'load/audit/c_load_dates.sql',                     'C_LOAD_DATES',                                            NULL,                    'FAIL')
    ) AS v (RUN_SEQ, TASK_ID, LAYER, SCRIPT_PATH, TARGET_TABLE, GATE_ID, ON_FAIL)
) s
ON t.TASK_ID = s.TASK_ID
WHEN MATCHED THEN UPDATE SET
    RUN_SEQ = s.RUN_SEQ, LAYER = s.LAYER, SCRIPT_PATH = s.SCRIPT_PATH, TARGET_TABLE = s.TARGET_TABLE,
    GATE_ID = s.GATE_ID, ON_FAIL = s.ON_FAIL, ACTIVE_FLG = TRUE, UPDATE_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RUN_SEQ, TASK_ID, LAYER, SCRIPT_PATH, TARGET_TABLE, GATE_ID, ON_FAIL, ACTIVE_FLG, UPDATE_TS)
    VALUES (s.RUN_SEQ, s.TASK_ID, s.LAYER, s.SCRIPT_PATH, s.TARGET_TABLE, s.GATE_ID, s.ON_FAIL, TRUE, CURRENT_TIMESTAMP());

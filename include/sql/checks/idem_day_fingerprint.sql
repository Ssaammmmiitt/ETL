-- One-row fingerprint for day D. Counts and nets must match after a second apply.
-- (Ignore STG _BATCH_ID — it may change on a Bronze reload; we do not include it here.)
SELECT
    TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD')) AS DAY_KY,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
      WHERE _INGEST_DT = '{{ ds }}') AS STG_SALES_CNT,

    (SELECT COALESCE(SUM(TRY_TO_NUMBER(TRIM(NET_SALES_AMT), 18, 2)), 0)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
      WHERE _INGEST_DT = '{{ ds }}') AS STG_SALES_NET,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE
      WHERE _INGEST_DT = '{{ ds }}') AS TMP_SALES_CNT,

    (SELECT COALESCE(SUM(NET_SALES_AMT), 0)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE
      WHERE _INGEST_DT = '{{ ds }}') AS TMP_SALES_NET,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_QUARANTINE
      WHERE INGEST_DT = '{{ ds }}'
        AND OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES')) AS QUAR_SALES_CNT,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
      WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))) AS FCT_SALES_CNT,

    (SELECT COALESCE(SUM(NET_SALES_AMT), 0)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
      WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))) AS FCT_SALES_NET,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
      WHERE CURRENT_FLG) AS FCT_INV_CURRENT_CNT,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
      WHERE CURRENT_FLG) AS FCT_PRICE_CURRENT_CNT,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
      WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))) AS AGG_CNT,

    (SELECT COALESCE(SUM(NET_SALES_AMT), 0)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.AGG_DAILY_ITEM_LOCN_SALES
      WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))) AS AGG_NET,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN
      WHERE CURRENT_FLG) AS DIM_LOCN_CURRENT_CNT,

    (SELECT COUNT(*)
       FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM
      WHERE CURRENT_FLG) AS DIM_ITEM_CURRENT_CNT
;

-- Accounting identity for sales on day D:
--   STG_SALES rows = TMP_SALES_LINE rows + C_REJ_TABLE rows (OBJECT_NM = TMP_SALES_LINE)
SELECT
    (SELECT COUNT(*) FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
      WHERE _INGEST_DT = '{{ ds }}') AS stg_cnt,
    (SELECT COUNT(*) FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_SALES_LINE
      WHERE _INGEST_DT = '{{ ds }}') AS tmp_cnt,
    (SELECT COUNT(*) FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
      WHERE INGEST_DT = '{{ ds }}' AND OBJECT_NM = 'TMP_SALES_LINE') AS quar_cnt;

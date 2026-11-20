-- G7 alert input: orphan sales lines quarantined before FCT_SALES on day D, by reason.
SELECT REASON_CD, COUNT(*) AS ROW_CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
WHERE INGEST_DT = '{{ ds }}'
  AND GATE_ID = 'G7'
  AND OBJECT_NM = 'FCT_SALES'
GROUP BY 1
ORDER BY 1;

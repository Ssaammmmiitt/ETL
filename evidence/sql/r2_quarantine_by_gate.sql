-- What each quality gate rejected per day, from C_REJ_TABLE (the run stays green).
SELECT
    INGEST_DT,
    GATE_ID,
    REASON_CD,
    OBJECT_NM,
    COUNT(*) AS ROW_CNT,
    COALESCE(SUM(TRY_TO_NUMBER(RAW_RECORD:NET_SALES_AMT::VARCHAR, 18, 2)), 0) AS NET_SALES_AMT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
WHERE INGEST_DT BETWEEN '{{ params.from_ds }}' AND '{{ params.to_ds }}'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

-- PK checker: FCT_SALES business key (TRANSACTION_ID, LINE_NUM) must be unique.
SELECT TRANSACTION_ID, LINE_NUM, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
GROUP BY 1, 2
HAVING COUNT(*) > 1;

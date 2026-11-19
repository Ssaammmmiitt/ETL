SELECT CUSTOMER_ID, COUNT(*) AS CNT
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_CUSTOMER
GROUP BY 1
HAVING COUNT(*) > 1;

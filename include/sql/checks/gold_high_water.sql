-- Newest business date Gold holds (NULL on a fresh account). ds < this value means a reload of a past day.
SELECT MAX(LAST_LOAD_DT)::VARCHAR
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_LOAD_DATES
WHERE LAYER = 'GOLD';

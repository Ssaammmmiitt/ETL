PUT file://{{ var.json.medallion_config.seed_path }}/seed_dim_day.csv
    @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/seed/
    AUTO_COMPRESS = TRUE
    OVERWRITE = TRUE
    SOURCE_COMPRESSION = NONE;

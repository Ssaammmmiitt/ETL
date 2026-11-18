PUT 'file://{{ var.json.medallion_config.landing_path }}/{{ params.source }}_{{ ds_nodash }}.csv'
    @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/
    AUTO_COMPRESS = TRUE
    OVERWRITE     = {{ params.get('overwrite', 'FALSE') }}

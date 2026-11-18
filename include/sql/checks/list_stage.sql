-- Lesson 3: show what is on the stage for the run's business date (name, size, md5, last_modified).
-- Lesson 4 turns this into a check that fails when a file is missing.
LIST @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/;

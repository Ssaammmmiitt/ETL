-- Gate G1 (row count half): every staged file for day D has at least one data row.
-- Reads the gzipped files straight from the stage, without loading them into a table.
-- FF_CSV_POSITIONAL skips nothing, so in a header file line 1 is the header and is not counted.
-- SQLCheckOperator fails the task if the single value returned is FALSE or NULL.
SELECT COUNT(*) = {{ params.expected_files }} AND MIN(data_rows) > 0 AS every_file_has_rows
FROM (
    SELECT METADATA$FILENAME AS file_name,
           COUNT_IF(METADATA$FILENAME ILIKE '%inventory_%' OR METADATA$FILE_ROW_NUMBER > 1) AS data_rows
    FROM @{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.MY_INT_STAGE/{{ ds }}/
         (FILE_FORMAT => '{{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.FF_CSV_POSITIONAL')
    GROUP BY METADATA$FILENAME
);

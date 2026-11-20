-- G11 assert input: the five reconciliation rows of day D.
SELECT CHECK_NM, ACTUAL_VAL, EXPECTED_VAL, STATUS
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_CHECKS
WHERE BUSINESS_DT = '{{ ds }}'::DATE
ORDER BY CHECK_SEQ;

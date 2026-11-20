-- G11 reconciliation for day D: five checks, one row each. Re-running D replaces its rows.
-- Sales lines of ingest D carry SALE_DT = D, so Gold rows for D are FCT_SALES.DAY_KY = D.
DELETE FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_CHECKS
WHERE BUSINESS_DT = '{{ ds }}'::DATE;

INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.RECON_DAILY_CHECKS (
    BUSINESS_DT, CHECK_SEQ, CHECK_NM, RULE_TXT, ACTUAL_VAL, EXPECTED_VAL, STATUS, BATCH_ID, CHECK_TS
)
WITH stg AS (
    SELECT COUNT(*) AS N, COALESCE(SUM(TRY_TO_NUMBER(TRIM(NET_SALES_AMT), 18, 2)), 0) AS AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.bronze }}.STG_SALES
    WHERE _INGEST_DT = '{{ ds }}'::DATE
),
rej AS (
    SELECT COUNT(*) AS N,
           COALESCE(SUM(TRY_TO_NUMBER(RAW_RECORD:NET_SALES_AMT::VARCHAR, 18, 2)), 0) AS AMT,
           COUNT_IF(GATE_ID = 'G7') AS ORPHANS
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.audit }}.C_REJ_TABLE
    WHERE INGEST_DT = '{{ ds }}'::DATE AND OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES')
),
fct AS (
    SELECT COUNT(*) AS N, COALESCE(SUM(NET_SALES_AMT), 0) AS AMT
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
    WHERE DAY_KY = TO_NUMBER(TO_CHAR('{{ ds }}'::DATE, 'YYYYMMDD'))
),
fresh AS (
    SELECT
        (SELECT MAX(SALE_DT) FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES) AS SALES_MAX,
        (SELECT MAX(SNAPSHOT_DT) FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY) AS INV_MAX
),
dup AS (
    SELECT
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_SALES
            GROUP BY TRANSACTION_ID, LINE_NUM HAVING COUNT(*) > 1)) AS SALES_DUP,
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY
            GROUP BY ITEM_KY, LOCN_KY, SNAPSHOT_DT HAVING COUNT(*) > 1)) AS INV_DUP,
        (SELECT COUNT(*) FROM (
            SELECT 1 FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_PRICE
            GROUP BY ITEM_KY, LOCN_KY, EFFECTIVE_START_DT HAVING COUNT(*) > 1)) AS PRICE_DUP
),
checks AS (
    SELECT 1 AS SEQ, 'ROW_COUNT' AS NM,
           'Staging rows equal target rows plus rejects' AS RULE_TXT,
           'STG ' || stg.N || ' = FCT ' || fct.N || ' + REJ ' || rej.N AS ACTUAL,
           'difference 0' AS EXPECTED,
           stg.N = fct.N + rej.N AS OK
    FROM stg, fct, rej
    UNION ALL
    SELECT 2, 'AMOUNT',
           'Warehouse sales total equals the source file total (net of rejected rows)',
           'source ' || stg.AMT || ' = FCT ' || fct.AMT || ' + REJ ' || rej.AMT,
           'variance 0.00',
           stg.AMT = fct.AMT + rej.AMT
    FROM stg, fct, rej
    UNION ALL
    SELECT 3, 'ORPHANS',
           'Facts on the unknown key stay under threshold',
           rej.ORPHANS || ' of ' || stg.N || ' lines (' || ROUND(100 * rej.ORPHANS / NULLIF(stg.N, 0), 2) || '%)',
           '<= ' || ROUND(100 * {{ params.orphan_max_pct }}, 2) || '%',
           rej.ORPHANS <= {{ params.orphan_max_pct }} * stg.N
    FROM stg, rej
    UNION ALL
    SELECT 4, 'FRESHNESS',
           'Latest fact date equals the run date',
           'FCT_SALES ' || COALESCE(fresh.SALES_MAX::VARCHAR, 'none') || ', FCT_INVENTORY ' || COALESCE(fresh.INV_MAX::VARCHAR, 'none'),
           '{{ ds }}',
           COALESCE(fresh.SALES_MAX = '{{ ds }}'::DATE AND fresh.INV_MAX = '{{ ds }}'::DATE, FALSE)
    FROM fresh
    UNION ALL
    SELECT 5, 'DUPLICATES',
           'No repeated business key at fact grain',
           'FCT_SALES ' || dup.SALES_DUP || ', FCT_INVENTORY ' || dup.INV_DUP || ', FCT_PRICE ' || dup.PRICE_DUP,
           '0 repeated keys',
           dup.SALES_DUP + dup.INV_DUP + dup.PRICE_DUP = 0
    FROM dup
)
SELECT '{{ ds }}'::DATE, SEQ, NM, RULE_TXT, ACTUAL, EXPECTED,
       IFF(OK, 'PASS', 'FAIL'), '{{ run_id }}', CURRENT_TIMESTAMP()
FROM checks;

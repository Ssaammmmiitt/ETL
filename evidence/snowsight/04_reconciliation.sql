-- 04 RECONCILIATION (G11): the five checks the DAG ran for each business day. All must be PASS.
-- ROW_COUNT  staging rows = target rows + rejects
-- AMOUNT     warehouse sales total = source file total (net of rejected rows)
-- ORPHANS    lines rejected for an unknown key stay under the threshold
-- FRESHNESS  latest fact date = run date (as it was when that day ran)
-- DUPLICATES no repeated business key at fact grain
USE ROLE MEDALLION_ROLE; USE WAREHOUSE MEDALLION_WH; USE DATABASE MEDALLION_DB;
SET FROM_DT = '2026-11-10';
SET TO_DT   = '2026-11-20';

SELECT BUSINESS_DT, CHECK_NM, RULE_TXT, ACTUAL_VAL, EXPECTED_VAL, STATUS, CHECK_TS
FROM AUDIT.RECON_DAILY_CHECKS
WHERE BUSINESS_DT BETWEEN $FROM_DT AND $TO_DT
ORDER BY BUSINESS_DT, CHECK_SEQ;

-- One-line summary per day (good for a single screenshot of the whole backfill).
SELECT BUSINESS_DT,
       COUNT(*)                       AS CHECKS,
       COUNT_IF(STATUS = 'PASS')      AS PASSED,
       LISTAGG(IFF(STATUS = 'PASS', NULL, CHECK_NM), ', ') AS FAILED_CHECKS
FROM AUDIT.RECON_DAILY_CHECKS
WHERE BUSINESS_DT BETWEEN $FROM_DT AND $TO_DT
GROUP BY 1
ORDER BY 1;

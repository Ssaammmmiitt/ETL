-- 02 PLANTED DEFECTS: each defect found in Bronze, the gate that caught it, and what reached Gold.
-- RESULT = SOLVED when every bad row was rejected and Gold holds none of it
-- (duplicates: one copy rejected, the latest kept; valid outliers: kept on purpose).
USE ROLE MEDALLION_ROLE; USE WAREHOUSE MEDALLION_WH; USE DATABASE MEDALLION_DB;
SET FROM_DT = '2026-11-10';
SET TO_DT   = '2026-11-14';

WITH bronze AS (
    SELECT _INGEST_DT AS DT, TRIM(TRANSACTION_ID) AS TXN, TRIM(LINE_NUM) AS LN, TRIM(ITEM_ID) AS ITEM_ID,
           TRY_TO_NUMBER(TRIM(SOLD_QTY), 18, 3) AS QTY, TRY_TO_NUMBER(TRIM(UNIT_PRICE_AMT), 18, 2) AS PRICE
    FROM BRONZE.STG_SALES
    WHERE _INGEST_DT BETWEEN $FROM_DT AND $TO_DT
),
rejects AS (
    SELECT INGEST_DT AS DT, TRIM(RAW_RECORD:TRANSACTION_ID::VARCHAR) AS TXN, TRIM(RAW_RECORD:LINE_NUM::VARCHAR) AS LN,
           GATE_ID || ' ' || REASON_CD AS RULE_HIT
    FROM AUDIT.C_REJ_TABLE
    WHERE INGEST_DT BETWEEN $FROM_DT AND $TO_DT AND OBJECT_NM IN ('TMP_SALES_LINE', 'FCT_SALES')
),
planted AS (
    SELECT DISTINCT DT, 'Null ITEM_ID' AS DEFECT, 'G3 (Silver)' AS EXPECTED_GATE, TXN, LN
    FROM bronze WHERE NULLIF(ITEM_ID, '') IS NULL
    UNION ALL
    SELECT DISTINCT DT, 'Negative SOLD_QTY', 'G6 (Silver)', TXN, LN FROM bronze WHERE QTY < 0
    UNION ALL
    SELECT DISTINCT DT, 'Orphan ITEM_ID 199999', 'G7 (before Gold fact)', TXN, LN FROM bronze WHERE ITEM_ID = '199999'
    UNION ALL
    SELECT DT, 'Duplicate TXN + LINE', 'G4 (Silver)', TXN, LN FROM bronze GROUP BY DT, TXN, LN HAVING COUNT(*) > 1
    UNION ALL
    SELECT DISTINCT DT, 'Sales anomaly', 'G12 (Silver)', TXN, LN FROM rejects WHERE RULE_HIT LIKE 'G12%'
    UNION ALL
    SELECT DISTINCT DT, 'Valid outlier (premium price)', 'none: kept', TXN, LN FROM bronze WHERE PRICE = 2499.99
),
bronze_n AS (SELECT DT, TXN, LN, COUNT(*) AS N FROM bronze GROUP BY 1, 2, 3),
rej_n    AS (SELECT DT, TXN, LN, COUNT(*) AS N, LISTAGG(DISTINCT RULE_HIT, ', ') AS HITS FROM rejects GROUP BY 1, 2, 3),
gold_n   AS (SELECT SALE_DT AS DT, TRANSACTION_ID AS TXN, LINE_NUM::VARCHAR AS LN, COUNT(*) AS N
             FROM GOLD.FCT_SALES WHERE SALE_DT BETWEEN $FROM_DT AND $TO_DT GROUP BY 1, 2, 3)
SELECT
    p.DT,
    p.DEFECT,
    SUM(b.N)                        AS BRONZE_ROWS,
    p.EXPECTED_GATE,
    LISTAGG(DISTINCT r.HITS, ', ')  AS CAUGHT_BY,
    SUM(COALESCE(r.N, 0))           AS REJECTED_ROWS,
    SUM(COALESCE(g.N, 0))           AS GOLD_ROWS,
    CASE
        WHEN p.DEFECT LIKE 'Duplicate%'
            THEN IFF(SUM(COALESCE(r.N, 0)) = SUM(b.N) - COUNT(*) AND SUM(COALESCE(g.N, 0)) = COUNT(*), 'SOLVED', 'CHECK')
        WHEN p.DEFECT LIKE 'Valid outlier%'
            THEN IFF(SUM(COALESCE(r.N, 0)) = 0 AND SUM(COALESCE(g.N, 0)) = SUM(b.N), 'SOLVED', 'CHECK')
        ELSE IFF(SUM(COALESCE(r.N, 0)) = SUM(b.N) AND SUM(COALESCE(g.N, 0)) = 0, 'SOLVED', 'CHECK')
    END AS RESULT
FROM planted p
JOIN bronze_n b      ON b.DT = p.DT AND b.TXN = p.TXN AND b.LN = p.LN
LEFT JOIN rej_n r    ON r.DT = p.DT AND r.TXN = p.TXN AND r.LN = p.LN
LEFT JOIN gold_n g   ON g.DT = p.DT AND g.TXN = p.TXN AND g.LN = p.LN
GROUP BY p.DT, p.DEFECT, p.EXPECTED_GATE
ORDER BY p.DT, p.DEFECT;

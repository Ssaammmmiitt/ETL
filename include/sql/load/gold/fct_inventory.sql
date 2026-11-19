-- FCT_INVENTORY soft-close for day D (departure 3). Never DELETE.
-- Same-day re-run: UPDATE the open SNAPSHOT_DT = D row in place (avoids PK clash).
-- New day: close older open rows that changed/disappeared, then INSERT.

CREATE OR REPLACE TEMPORARY TABLE TMP_INV_D AS
SELECT
    i.ITEM_KY,
    l.LOCN_KY,
    t.SNAPSHOT_DT,
    t.ON_HAND_QTY,
    t.RESERVED_QTY,
    t.IN_TRANSIT_QTY,
    t.AVAILABLE_QTY
FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.silver }}.TMP_ITEM_LOC_SNAPSHOT t
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_ITEM i
    ON i.ITEM_ID = t.ITEM_ID AND i.CURRENT_FLG
INNER JOIN {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.DIM_LOCN l
    ON l.LOCN_ID = t.LOCN_ID AND l.CURRENT_FLG
WHERE t._INGEST_DT = '{{ ds }}';

-- Same-day correction: refresh open row already dated D
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY f
SET
    ON_HAND_QTY = s.ON_HAND_QTY,
    RESERVED_QTY = s.RESERVED_QTY,
    IN_TRANSIT_QTY = s.IN_TRANSIT_QTY,
    AVAILABLE_QTY = s.AVAILABLE_QTY,
    UPDATE_TS = CURRENT_TIMESTAMP()
FROM TMP_INV_D s
WHERE f.CURRENT_FLG
  AND f.SNAPSHOT_DT = '{{ ds }}'::DATE
  AND f.ITEM_KY = s.ITEM_KY
  AND f.LOCN_KY = s.LOCN_KY
  AND HASH(f.ON_HAND_QTY, f.RESERVED_QTY, f.IN_TRANSIT_QTY, f.AVAILABLE_QTY)
   <> HASH(s.ON_HAND_QTY, s.RESERVED_QTY, s.IN_TRANSIT_QTY, s.AVAILABLE_QTY);

-- Close older open rows (SNAPSHOT_DT < D) that changed or disappeared
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY f
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
WHERE f.CURRENT_FLG
  AND f.SNAPSHOT_DT < '{{ ds }}'::DATE
  AND (
        NOT EXISTS (
            SELECT 1 FROM TMP_INV_D s
            WHERE s.ITEM_KY = f.ITEM_KY AND s.LOCN_KY = f.LOCN_KY
        )
     OR EXISTS (
            SELECT 1 FROM TMP_INV_D s
            WHERE s.ITEM_KY = f.ITEM_KY
              AND s.LOCN_KY = f.LOCN_KY
              AND HASH(s.ON_HAND_QTY, s.RESERVED_QTY, s.IN_TRANSIT_QTY, s.AVAILABLE_QTY)
               <> HASH(f.ON_HAND_QTY, f.RESERVED_QTY, f.IN_TRANSIT_QTY, f.AVAILABLE_QTY)
        )
      );

-- Close same-day open rows that disappeared from today's snapshot
UPDATE {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY f
SET
    EFFECTIVE_END_DT = DATEADD(day, -1, '{{ ds }}'::DATE),
    CURRENT_FLG = FALSE,
    UPDATE_TS = CURRENT_TIMESTAMP()
WHERE f.CURRENT_FLG
  AND f.SNAPSHOT_DT = '{{ ds }}'::DATE
  AND NOT EXISTS (
        SELECT 1 FROM TMP_INV_D s
        WHERE s.ITEM_KY = f.ITEM_KY AND s.LOCN_KY = f.LOCN_KY
      );

-- Insert where no current open row remains
INSERT INTO {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY (
    INVENTORY_KY, ITEM_KY, LOCN_KY, SNAPSHOT_DT,
    ON_HAND_QTY, RESERVED_QTY, IN_TRANSIT_QTY, AVAILABLE_QTY,
    EFFECTIVE_END_DT, CURRENT_FLG, INSERT_TS, UPDATE_TS
)
SELECT
    {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.SEQ_INVENTORY_KY.NEXTVAL,
    s.ITEM_KY,
    s.LOCN_KY,
    s.SNAPSHOT_DT,
    s.ON_HAND_QTY,
    s.RESERVED_QTY,
    s.IN_TRANSIT_QTY,
    s.AVAILABLE_QTY,
    '9999-12-31'::DATE,
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM TMP_INV_D s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ var.json.medallion_config.database }}.{{ var.json.medallion_config.schemas.gold }}.FCT_INVENTORY f
    WHERE f.ITEM_KY = s.ITEM_KY
      AND f.LOCN_KY = s.LOCN_KY
      AND f.CURRENT_FLG
);

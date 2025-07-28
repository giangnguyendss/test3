/* 
==========================================================================================
  Databricks SQL Test Suite for Enrichment of purgo_playground.patient_therapy_shipment
  Target Table: purgo_playground.enriched_patient_therapy_shipment
  Author: Databricks Test Automation
  All test logic, assertions, and validations are included per requirements.
==========================================================================================
*/

/* 
==========================================================================================
  SECTION: SETUP - Clean up and Prepare Test Data
==========================================================================================
*/

-- Drop and recreate the output table to ensure a clean test environment
DROP TABLE IF EXISTS purgo_playground.enriched_patient_therapy_shipment;

CREATE TABLE purgo_playground.enriched_patient_therapy_shipment (
  product STRING,
  ship_date DATE,
  days_supply STRING,
  qty STRING,
  treatment_id STRING,
  dob DATE,
  first_ship_date DATE,
  refill_status STRING,
  patient_id STRING,
  ship_type STRING,
  shipment_arrived_status STRING,
  delivery_ontime STRING,
  shipment_expiry DATE,
  discontinuation_date DATE,
  days_until_next_ship BIGINT,
  days_since_last_fill BIGINT,
  expected_refill_date DATE,
  prior_ship DATE,
  days_between BIGINT,
  days_since_supply_out BIGINT,
  age BIGINT,
  age_at_first_ship BIGINT,
  latest_therapy_ships BIGINT,
  discontinuation_type STRING
);

-- Drop and recreate the source table for test data
DROP TABLE IF EXISTS purgo_playground.patient_therapy_shipment;

CREATE TABLE purgo_playground.patient_therapy_shipment (
  product STRING,
  ship_date DATE,
  days_supply STRING,
  qty BIGINT,
  treatment_id STRING,
  dob DATE,
  first_ship_date DATE,
  refill_status STRING,
  patient_id STRING,
  ship_type STRING,
  shipment_arrived_status STRING,
  delivery_ontime STRING,
  calctime TIMESTAMP
);

-- Insert test data (from provided testdata.sql)
INSERT INTO purgo_playground.patient_therapy_shipment
WITH test_data AS (
  SELECT
    'DrugA' AS product,
    DATE'2024-01-01' AS ship_date,
    '21' AS days_supply,
    63 AS qty,
    'TREAT123' AS treatment_id,
    DATE'1980-06-15' AS dob,
    DATE'2023-12-01' AS first_ship_date,
    'DC - Standard' AS refill_status,
    'PAT001' AS patient_id,
    'commercial' AS ship_type,
    'arrived' AS shipment_arrived_status,
    'yes' AS delivery_ontime,
    TIMESTAMP'2024-01-15T00:00:00.000+0000' AS calctime
  UNION ALL
  SELECT 'DrugB', DATE'2024-02-10', NULL, 90, 'TREAT456', DATE'1975-03-10', DATE'2024-02-10', 'DC-PERMANENT', 'PAT002', 'commercial', 'arrived', 'no', TIMESTAMP'2024-02-20T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugC', DATE'2024-03-01', '14', 42, 'TREAT789', DATE'2000-01-01', DATE'2024-03-01', NULL, 'PAT003', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugD', DATE'2024-01-01', NULL, NULL, 'TREAT000', DATE'1990-01-01', DATE'2024-01-01', 'DC - Standard', 'PAT004', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugE', DATE'2024-01-01', 'abc', 30, 'TREAT001', DATE'1985-05-05', DATE'2024-01-01', 'DC - Standard', 'PAT005', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugF', DATE'2024-01-01', NULL, NULL, 'TREAT002', DATE'1985-05-05', DATE'2024-01-01', 'DC - Standard', 'PAT006', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugA', DATE'2024-01-15', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'sample', 'arrived', 'yes', TIMESTAMP'2024-01-20T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugA', DATE'2024-01-22', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-25T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugA', DATE'2024-02-12', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-02-15T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugG', DATE'2024-03-01', '30', 90, 'TREAT999', DATE'1970-12-31', DATE'2024-03-01', 'Other', 'PAT007', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugH', DATE'2024-03-01', '30', 90, 'TREAT998', DATE'1970-12-31', DATE'2024-03-01', NULL, 'PAT008', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugI', NULL, '30', 90, 'TREAT997', DATE'1970-12-31', DATE'2024-03-01', 'DC - Standard', 'PAT009', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugJ', DATE'2024-03-01', '30', 90, 'TREAT996', NULL, DATE'2024-03-01', 'DC - Standard', 'PAT010', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugK', DATE'2024-03-01', '30', 90, 'TREAT995', DATE'1970-12-31', NULL, 'DC - Standard', 'PAT011', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugL', DATE'2024-03-01', '30', 90, 'TREAT994', DATE'1970-12-31', DATE'2024-03-01', 'DC - Standard', 'PAT012', 'commercial', 'arrived', 'no', NULL
  UNION ALL
  SELECT 'DrügΩ', DATE'2024-04-01', '28', 84, 'TREATΩ', DATE'1995-07-07', DATE'2024-04-01', 'DC - Standard', 'PATΩΩ', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-04-10T00:00:00.000+0000'
  UNION ALL
  SELECT '药品A', DATE'2024-05-01', '30', 90, 'TREAT汉字', DATE'1990-01-01', DATE'2024-05-01', 'DC-PERMANENT', 'PAT汉字', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-05-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugM', DATE'2024-06-01', '30', 90, 'TREAT993', DATE'1970-12-31', DATE'2024-06-01', 'DC - Standard', 'PAT013', 'commercial', 'arrived', NULL, TIMESTAMP'2024-06-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugN', DATE'2024-07-01', '30', 90, 'TREAT992', DATE'1970-12-31', DATE'2024-07-01', 'DC - Standard', 'PAT014', 'commercial', NULL, 'yes', TIMESTAMP'2024-07-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugO', DATE'2024-08-01', '30', 90, 'TREAT991', DATE'1970-12-31', DATE'2024-08-01', 'DC - Standard', 'PAT015', NULL, 'arrived', 'yes', TIMESTAMP'2024-08-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugP', DATE'2024-09-01', '30', 90, 'TREAT990', DATE'1970-12-31', DATE'2024-09-01', 'dc - standard', 'PAT016', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-09-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugQ', DATE'2024-10-01', '0', 0, 'TREAT989', DATE'1970-12-31', DATE'2024-10-01', 'DC - Standard', 'PAT017', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-10-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugR', DATE'2024-11-01', NULL, 0, 'TREAT988', DATE'1970-12-31', DATE'2024-11-01', 'DC - Standard', 'PAT018', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-11-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugS', DATE'2024-12-01', '-7', 21, 'TREAT987', DATE'1970-12-31', DATE'2024-12-01', 'DC - Standard', 'PAT019', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-12-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugT', DATE'2025-01-01', NULL, -21, 'TREAT986', DATE'1970-12-31', DATE'2025-01-01', 'DC - Standard', 'PAT020', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-01-10T00:00:00.000+0000'
  UNION ALL
  SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  UNION ALL
  SELECT 'DrugU', DATE'2025-02-01', '30', 90, 'TREAT985', DATE'1970-12-31', DATE'2025-02-01', 'DC - 特殊', 'PAT021', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-02-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugV', DATE'2025-03-01', '30', 90, 'TREAT984', DATE'1970-12-31', DATE'2025-03-01', 'DC - Standard', 'PAT022', 'commercial', 'arrived', '是', TIMESTAMP'2025-03-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugW', DATE'2025-04-01', '30', 90, 'TREAT983', DATE'1970-12-31', DATE'2025-04-01', 'DC - Standard', 'PAT023', 'commercial', '到达', 'yes', TIMESTAMP'2025-04-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugX', DATE'2025-05-01', '30', 90, 'TREAT982', DATE'1970-12-31', DATE'2025-05-01', 'DC - Standard', 'PAT024', 'Commercial', 'arrived', 'yes', TIMESTAMP'2025-05-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugY', DATE'2025-06-01', '30', 90, 'TREAT981', DATE'1970-12-31', DATE'2025-06-01', 'DC - Standard', 'PAT025', 'COMMERCIAL', 'arrived', 'yes', TIMESTAMP'2025-06-10T00:00:00.000+0000'
  UNION ALL
  SELECT 'DrugZ', DATE'2025-07-01', '30', 90, 'TREAT980', DATE'1970-12-31', DATE'2025-07-01', 'DC - Standard', 'PAT026', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-07-10T00:00:00.000+0000'
)
SELECT * FROM test_data
;

/* 
==========================================================================================
  SECTION: ENRICHMENT LOGIC - Insert Derived Data into Output Table
==========================================================================================
*/

-- Insert enriched data into the output table
INSERT OVERWRITE TABLE purgo_playground.enriched_patient_therapy_shipment
WITH base AS (
  SELECT
    p.product,
    p.ship_date,
    p.days_supply,
    CAST(p.qty AS STRING) AS qty,
    p.treatment_id,
    p.dob,
    p.first_ship_date,
    p.refill_status,
    p.patient_id,
    p.ship_type,
    p.shipment_arrived_status,
    p.delivery_ontime,
    -- Derived: shipment_expiry
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST(p.days_supply AS INT))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT))
      ELSE NULL
    END AS shipment_expiry,
    -- Derived: discontinuation_date
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), 91)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), 91)
      ELSE NULL
    END AS discontinuation_date,
    -- Derived: days_until_next_ship
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1)
      ELSE NULL
    END AS days_until_next_ship,
    -- Derived: days_since_last_fill
    CASE
      WHEN p.calctime IS NULL OR p.ship_date IS NULL THEN NULL
      ELSE (DATEDIFF(CAST(p.calctime AS DATE), p.ship_date) + 1)
    END AS days_since_last_fill,
    -- Derived: expected_refill_date
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1))
      ELSE NULL
    END AS expected_refill_date,
    p.calctime
  FROM purgo_playground.patient_therapy_shipment p
),
windowed AS (
  SELECT
    *,
    -- Derived: prior_ship
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS prior_ship
  FROM base
),
windowed2 AS (
  SELECT
    *,
    -- Derived: days_between
    CASE
      WHEN prior_ship IS NULL OR ship_date IS NULL THEN NULL
      ELSE DATEDIFF(ship_date, prior_ship)
    END AS days_between
  FROM windowed
),
windowed3 AS (
  SELECT
    *,
    -- Derived: days_since_supply_out
    CASE
      WHEN calctime IS NULL OR ship_date IS NULL THEN NULL
      WHEN TRY_CAST(days_supply AS INT) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT)))
          ELSE NULL
        END
      WHEN days_supply IS NULL AND TRY_CAST(qty AS DOUBLE) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT)))
          ELSE NULL
        END
      ELSE NULL
    END AS days_since_supply_out
  FROM windowed2
),
windowed4 AS (
  SELECT
    *,
    -- Derived: age
    CASE
      WHEN dob IS NULL OR calctime IS NULL THEN NULL
      ELSE YEAR(calctime) - YEAR(dob) - CASE WHEN (MONTH(calctime) < MONTH(dob)) OR (MONTH(calctime) = MONTH(dob) AND DAY(calctime) < DAY(dob)) THEN 1 ELSE 0 END
    END AS age,
    -- Derived: age_at_first_ship
    CASE
      WHEN dob IS NULL OR first_ship_date IS NULL THEN NULL
      ELSE ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    END AS age_at_first_ship
  FROM windowed3
),
latest_ships AS (
  SELECT
    patient_id,
    treatment_id,
    COUNT(*) AS latest_therapy_ships
  FROM purgo_playground.patient_therapy_shipment
  WHERE LOWER(ship_type) = "commercial"
  GROUP BY patient_id, treatment_id
),
final_enriched AS (
  SELECT
    w4.product,
    w4.ship_date,
    w4.days_supply,
    w4.qty,
    w4.treatment_id,
    w4.dob,
    w4.first_ship_date,
    w4.refill_status,
    w4.patient_id,
    w4.ship_type,
    w4.shipment_arrived_status,
    w4.delivery_ontime,
    w4.shipment_expiry,
    w4.discontinuation_date,
    w4.days_until_next_ship,
    w4.days_since_last_fill,
    w4.expected_refill_date,
    w4.prior_ship,
    w4.days_between,
    w4.days_since_supply_out,
    w4.age,
    w4.age_at_first_ship,
    ls.latest_therapy_ships,
    -- Derived: discontinuation_type
    CASE
      WHEN w4.refill_status = "DC - Standard" THEN "STANDARD"
      WHEN w4.refill_status = "DC-PERMANENT" THEN "PERMANENT"
      ELSE NULL
    END AS discontinuation_type
  FROM windowed4 w4
  LEFT JOIN latest_ships ls
    ON w4.patient_id = ls.patient_id AND w4.treatment_id = ls.treatment_id
)
SELECT * FROM final_enriched
;

/* 
==========================================================================================
  SECTION: SCHEMA VALIDATION TESTS
==========================================================================================
*/

-- Validate output schema: column names and data types
WITH expected_schema AS (
  SELECT "product" AS col, "STRING" AS typ UNION ALL
  SELECT "ship_date", "DATE" UNION ALL
  SELECT "days_supply", "STRING" UNION ALL
  SELECT "qty", "STRING" UNION ALL
  SELECT "treatment_id", "STRING" UNION ALL
  SELECT "dob", "DATE" UNION ALL
  SELECT "first_ship_date", "DATE" UNION ALL
  SELECT "refill_status", "STRING" UNION ALL
  SELECT "patient_id", "STRING" UNION ALL
  SELECT "ship_type", "STRING" UNION ALL
  SELECT "shipment_arrived_status", "STRING" UNION ALL
  SELECT "delivery_ontime", "STRING" UNION ALL
  SELECT "shipment_expiry", "DATE" UNION ALL
  SELECT "discontinuation_date", "DATE" UNION ALL
  SELECT "days_until_next_ship", "BIGINT" UNION ALL
  SELECT "days_since_last_fill", "BIGINT" UNION ALL
  SELECT "expected_refill_date", "DATE" UNION ALL
  SELECT "prior_ship", "DATE" UNION ALL
  SELECT "days_between", "BIGINT" UNION ALL
  SELECT "days_since_supply_out", "BIGINT" UNION ALL
  SELECT "age", "BIGINT" UNION ALL
  SELECT "age_at_first_ship", "BIGINT" UNION ALL
  SELECT "latest_therapy_ships", "BIGINT" UNION ALL
  SELECT "discontinuation_type", "STRING"
),
actual_schema AS (
  SELECT
    col_name AS col,
    UPPER(data_type) AS typ
  FROM
    information_schema.columns
  WHERE
    table_schema = "purgo_playground"
    AND table_name = "enriched_patient_therapy_shipment"
)
SELECT
  e.col,
  e.typ AS expected_type,
  a.typ AS actual_type,
  CASE WHEN e.typ = a.typ THEN "PASS" ELSE "FAIL" END AS type_match
FROM expected_schema e
LEFT JOIN actual_schema a ON e.col = a.col
ORDER BY e.col
;

-- Assert all columns match expected types
WITH type_mismatches AS (
  SELECT
    e.col
  FROM (
    SELECT
      col_name AS col,
      UPPER(data_type) AS typ
    FROM
      information_schema.columns
    WHERE
      table_schema = "purgo_playground"
      AND table_name = "enriched_patient_therapy_shipment"
  ) a
  INNER JOIN (
    SELECT "product" AS col, "STRING" AS typ UNION ALL
    SELECT "ship_date", "DATE" UNION ALL
    SELECT "days_supply", "STRING" UNION ALL
    SELECT "qty", "STRING" UNION ALL
    SELECT "treatment_id", "STRING" UNION ALL
    SELECT "dob", "DATE" UNION ALL
    SELECT "first_ship_date", "DATE" UNION ALL
    SELECT "refill_status", "STRING" UNION ALL
    SELECT "patient_id", "STRING" UNION ALL
    SELECT "ship_type", "STRING" UNION ALL
    SELECT "shipment_arrived_status", "STRING" UNION ALL
    SELECT "delivery_ontime", "STRING" UNION ALL
    SELECT "shipment_expiry", "DATE" UNION ALL
    SELECT "discontinuation_date", "DATE" UNION ALL
    SELECT "days_until_next_ship", "BIGINT" UNION ALL
    SELECT "days_since_last_fill", "BIGINT" UNION ALL
    SELECT "expected_refill_date", "DATE" UNION ALL
    SELECT "prior_ship", "DATE" UNION ALL
    SELECT "days_between", "BIGINT" UNION ALL
    SELECT "days_since_supply_out", "BIGINT" UNION ALL
    SELECT "age", "BIGINT" UNION ALL
    SELECT "age_at_first_ship", "BIGINT" UNION ALL
    SELECT "latest_therapy_ships", "BIGINT" UNION ALL
    SELECT "discontinuation_type", "STRING"
  ) e ON a.col = e.col
  WHERE a.typ <> e.typ
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS schema_type_assertion
FROM type_mismatches
;

/* 
==========================================================================================
  SECTION: DATA QUALITY VALIDATION TESTS
==========================================================================================
*/

-- Assert output row count matches input row count
SELECT
  CASE WHEN
    (SELECT COUNT(*) FROM purgo_playground.enriched_patient_therapy_shipment) =
    (SELECT COUNT(*) FROM purgo_playground.patient_therapy_shipment)
  THEN "PASS" ELSE "FAIL" END AS row_count_match
;

-- Assert all records in output table have the same product, patient_id, and ship_date as input
WITH mismatches AS (
  SELECT
    e.product, e.patient_id, e.ship_date
  FROM purgo_playground.enriched_patient_therapy_shipment e
  FULL OUTER JOIN purgo_playground.patient_therapy_shipment p
    ON e.product = p.product AND e.patient_id = p.patient_id AND e.ship_date = p.ship_date
  WHERE p.product IS NULL OR e.product IS NULL
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN "PASS" ELSE "FAIL" END AS record_alignment_assertion
FROM mismatches
;

-- Assert output table is overwritten (no duplicate records after rerun)
-- (Re-run enrichment and check row count remains the same)
INSERT OVERWRITE TABLE purgo_playground.enriched_patient_therapy_shipment
WITH base AS (
  SELECT
    p.product,
    p.ship_date,
    p.days_supply,
    CAST(p.qty AS STRING) AS qty,
    p.treatment_id,
    p.dob,
    p.first_ship_date,
    p.refill_status,
    p.patient_id,
    p.ship_type,
    p.shipment_arrived_status,
    p.delivery_ontime,
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST(p.days_supply AS INT))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT))
      ELSE NULL
    END AS shipment_expiry,
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), 91)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), 91)
      ELSE NULL
    END AS discontinuation_date,
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1)
      ELSE NULL
    END AS days_until_next_ship,
    CASE
      WHEN p.calctime IS NULL OR p.ship_date IS NULL THEN NULL
      ELSE (DATEDIFF(CAST(p.calctime AS DATE), p.ship_date) + 1)
    END AS days_since_last_fill,
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1))
      ELSE NULL
    END AS expected_refill_date,
    p.calctime
  FROM purgo_playground.patient_therapy_shipment p
),
windowed AS (
  SELECT
    *,
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS prior_ship
  FROM base
),
windowed2 AS (
  SELECT
    *,
    CASE
      WHEN prior_ship IS NULL OR ship_date IS NULL THEN NULL
      ELSE DATEDIFF(ship_date, prior_ship)
    END AS days_between
  FROM windowed
),
windowed3 AS (
  SELECT
    *,
    CASE
      WHEN calctime IS NULL OR ship_date IS NULL THEN NULL
      WHEN TRY_CAST(days_supply AS INT) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT)))
          ELSE NULL
        END
      WHEN days_supply IS NULL AND TRY_CAST(qty AS DOUBLE) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT)))
          ELSE NULL
        END
      ELSE NULL
    END AS days_since_supply_out
  FROM windowed2
),
windowed4 AS (
  SELECT
    *,
    CASE
      WHEN dob IS NULL OR calctime IS NULL THEN NULL
      ELSE YEAR(calctime) - YEAR(dob) - CASE WHEN (MONTH(calctime) < MONTH(dob)) OR (MONTH(calctime) = MONTH(dob) AND DAY(calctime) < DAY(dob)) THEN 1 ELSE 0 END
    END AS age,
    CASE
      WHEN dob IS NULL OR first_ship_date IS NULL THEN NULL
      ELSE ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    END AS age_at_first_ship
  FROM windowed3
),
latest_ships AS (
  SELECT
    patient_id,
    treatment_id,
    COUNT(*) AS latest_therapy_ships
  FROM purgo_playground.patient_therapy_shipment
  WHERE LOWER(ship_type) = "commercial"
  GROUP BY patient_id, treatment_id
),
final_enriched AS (
  SELECT
    w4.product,
    w4.ship_date,
    w4.days_supply,
    w4.qty,
    w4.treatment_id,
    w4.dob,
    w4.first_ship_date,
    w4.refill_status,
    w4.patient_id,
    w4.ship_type,
    w4.shipment_arrived_status,
    w4.delivery_ontime,
    w4.shipment_expiry,
    w4.discontinuation_date,
    w4.days_until_next_ship,
    w4.days_since_last_fill,
    w4.expected_refill_date,
    w4.prior_ship,
    w4.days_between,
    w4.days_since_supply_out,
    w4.age,
    w4.age_at_first_ship,
    ls.latest_therapy_ships,
    CASE
      WHEN w4.refill_status = "DC - Standard" THEN "STANDARD"
      WHEN w4.refill_status = "DC-PERMANENT" THEN "PERMANENT"
      ELSE NULL
    END AS discontinuation_type
  FROM windowed4 w4
  LEFT JOIN latest_ships ls
    ON w4.patient_id = ls.patient_id AND w4.treatment_id = ls.treatment_id
)
SELECT * FROM final_enriched
;

SELECT
  CASE WHEN
    (SELECT COUNT(*) FROM purgo_playground.enriched_patient_therapy_shipment) =
    (SELECT COUNT(*) FROM purgo_playground.patient_therapy_shipment)
  THEN "PASS" ELSE "FAIL" END AS overwrite_row_count_assertion
;

/* 
==========================================================================================
  SECTION: DERIVED COLUMN LOGIC TESTS
==========================================================================================
*/

-- Test: shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date for days_supply NULL, qty fallback
WITH test_case AS (
  SELECT
    e.ship_date,
    e.days_supply,
    e.qty,
    e.shipment_expiry,
    e.discontinuation_date,
    e.days_until_next_ship,
    e.expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment e
  WHERE e.product = "DrugB"
)
SELECT
  CASE
    WHEN shipment_expiry = DATE'2024-01-22'
     AND discontinuation_date = DATE'2024-04-22'
     AND days_until_next_ship = 13
     AND expected_refill_date = DATE'2024-01-23'
    THEN "PASS"
    ELSE "FAIL"
  END AS derived_column_fallback_assertion
FROM test_case
;

-- Test: shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date for days_supply and qty NULL
WITH test_case AS (
  SELECT
    e.shipment_expiry,
    e.discontinuation_date,
    e.days_until_next_ship,
    e.expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment e
  WHERE e.product = "DrugD"
)
SELECT
  CASE
    WHEN shipment_expiry IS NULL
     AND discontinuation_date IS NULL
     AND days_until_next_ship IS NULL
     AND expected_refill_date IS NULL
    THEN "PASS"
    ELSE "FAIL"
  END AS derived_column_null_assertion
FROM test_case
;

-- Test: prior_ship and days_between for multiple shipments per treatment_id
WITH t AS (
  SELECT
    product, ship_date, prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE treatment_id = "TREAT123"
)
SELECT
  CASE
    WHEN
      MAX(CASE WHEN ship_date = DATE'2024-01-01' AND prior_ship IS NULL AND days_between IS NULL THEN 1 ELSE 0 END) = 1
      AND MAX(CASE WHEN ship_date = DATE'2024-01-22' AND prior_ship = DATE'2024-01-01' AND days_between = 21 THEN 1 ELSE 0 END) = 1
      AND MAX(CASE WHEN ship_date = DATE'2024-02-12' AND prior_ship = DATE'2024-01-22' AND days_between = 21 THEN 1 ELSE 0 END) = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS prior_ship_days_between_assertion
FROM t
;

-- Test: latest_therapy_ships for commercial shipments
WITH t AS (
  SELECT
    product, ship_type, latest_therapy_ships
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE treatment_id = "TREAT123" AND patient_id = "PAT001"
)
SELECT
  CASE
    WHEN SUM(CASE WHEN ship_type = "commercial" AND latest_therapy_ships = 4 THEN 1 ELSE 0 END) = 3
      AND SUM(CASE WHEN ship_type = "sample" AND latest_therapy_ships = 4 THEN 1 ELSE 0 END) = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS latest_therapy_ships_assertion
FROM t
;

-- Test: discontinuation_type based on refill_status
WITH t AS (
  SELECT
    product, refill_status, discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugA", "DrugB", "DrugC", "DrugG")
)
SELECT
  CASE
    WHEN SUM(CASE WHEN product = "DrugA" AND discontinuation_type = "STANDARD" THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugB" AND discontinuation_type = "PERMANENT" THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugC" AND discontinuation_type IS NULL THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugG" AND discontinuation_type IS NULL THEN 1 ELSE 0 END) = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS discontinuation_type_assertion
FROM t
;

-- Test: age and age_at_first_ship
WITH t AS (
  SELECT
    product, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugA", "DrugC")
)
SELECT
  CASE
    WHEN SUM(CASE WHEN product = "DrugA" AND age = 43 AND age_at_first_ship = 44 THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugC" AND age = 24 AND age_at_first_ship = 24 THEN 1 ELSE 0 END) = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS age_assertion
FROM t
;

-- Test: NULL handling for ship_date, dob, first_ship_date
WITH t AS (
  SELECT
    product, ship_date, dob, first_ship_date, shipment_expiry, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugI", "DrugJ", "DrugK")
)
SELECT
  CASE
    WHEN SUM(CASE WHEN product = "DrugI" AND shipment_expiry IS NULL THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugJ" AND age IS NULL THEN 1 ELSE 0 END) = 1
      AND SUM(CASE WHEN product = "DrugK" AND age_at_first_ship IS NULL THEN 1 ELSE 0 END) = 1
    THEN "PASS"
    ELSE "FAIL"
  END AS null_handling_assertion
FROM t
;

-- Test: Error handling for invalid days_supply (non-numeric)
WITH t AS (
  SELECT
    product, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugE"
)
SELECT
  CASE
    WHEN shipment_expiry IS NULL
      AND discontinuation_date IS NULL
      AND days_until_next_ship IS NULL
      AND expected_refill_date IS NULL
    THEN "PASS"
    ELSE "FAIL"
  END AS invalid_days_supply_assertion
FROM t
;

-- Test: Error handling for missing calctime
WITH t AS (
  SELECT
    product, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, days_since_last_fill, days_since_supply_out, age
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugL"
)
SELECT
  CASE
    WHEN days_until_next_ship IS NULL
      AND expected_refill_date IS NULL
      AND days_since_last_fill IS NULL
      AND days_since_supply_out IS NULL
      AND age IS NULL
    THEN "PASS"
    ELSE "FAIL"
  END AS missing_calctime_assertion
FROM t
;

-- Test: Output table is not partitioned or indexed
SELECT
  CASE WHEN
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND is_partitioning_column = "YES") = 0
  THEN "PASS" ELSE "FAIL" END AS partitioning_assertion
;

-- Test: No join with other tables is performed (all columns from source or derived from source)
SELECT
  CASE WHEN
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND col_name NOT IN (
      "product", "ship_date", "days_supply", "qty", "treatment_id", "dob", "first_ship_date", "refill_status", "patient_id", "ship_type", "shipment_arrived_status", "delivery_ontime",
      "shipment_expiry", "discontinuation_date", "days_until_next_ship", "days_since_last_fill", "expected_refill_date", "prior_ship", "days_between", "days_since_supply_out", "age", "age_at_first_ship", "latest_therapy_ships", "discontinuation_type"
    )) = 0
  THEN "PASS" ELSE "FAIL" END AS no_join_assertion
;

/* 
==========================================================================================
  SECTION: PERFORMANCE TESTS
==========================================================================================
*/

-- Performance: Ensure enrichment query runs within reasonable time for test data (should be < 10s for small set)
-- (This is a placeholder; in production, use query history or EXPLAIN/PROFILE for actual timing)
EXPLAIN
WITH base AS (
  SELECT
    p.product,
    p.ship_date,
    p.days_supply,
    CAST(p.qty AS STRING) AS qty,
    p.treatment_id,
    p.dob,
    p.first_ship_date,
    p.refill_status,
    p.patient_id,
    p.ship_type,
    p.shipment_arrived_status,
    p.delivery_ontime,
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST(p.days_supply AS INT))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT))
      ELSE NULL
    END AS shipment_expiry,
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), 91)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), 91)
      ELSE NULL
    END AS discontinuation_date,
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1)
      ELSE NULL
    END AS days_until_next_ship,
    CASE
      WHEN p.calctime IS NULL OR p.ship_date IS NULL THEN NULL
      ELSE (DATEDIFF(CAST(p.calctime AS DATE), p.ship_date) + 1)
    END AS days_since_last_fill,
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST((p.qty / 3) * 7 AS INT)), CAST(p.calctime AS DATE)) + 1))
      ELSE NULL
    END AS expected_refill_date,
    p.calctime
  FROM purgo_playground.patient_therapy_shipment p
),
windowed AS (
  SELECT
    *,
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS prior_ship
  FROM base
),
windowed2 AS (
  SELECT
    *,
    CASE
      WHEN prior_ship IS NULL OR ship_date IS NULL THEN NULL
      ELSE DATEDIFF(ship_date, prior_ship)
    END AS days_between
  FROM windowed
),
windowed3 AS (
  SELECT
    *,
    CASE
      WHEN calctime IS NULL OR ship_date IS NULL THEN NULL
      WHEN TRY_CAST(days_supply AS INT) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT)))
          ELSE NULL
        END
      WHEN days_supply IS NULL AND TRY_CAST(qty AS DOUBLE) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3) * 7 AS INT)))
          ELSE NULL
        END
      ELSE NULL
    END AS days_since_supply_out
  FROM windowed2
),
windowed4 AS (
  SELECT
    *,
    CASE
      WHEN dob IS NULL OR calctime IS NULL THEN NULL
      ELSE YEAR(calctime) - YEAR(dob) - CASE WHEN (MONTH(calctime) < MONTH(dob)) OR (MONTH(calctime) = MONTH(dob) AND DAY(calctime) < DAY(dob)) THEN 1 ELSE 0 END
    END AS age,
    CASE
      WHEN dob IS NULL OR first_ship_date IS NULL THEN NULL
      ELSE ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    END AS age_at_first_ship
  FROM windowed3
),
latest_ships AS (
  SELECT
    patient_id,
    treatment_id,
    COUNT(*) AS latest_therapy_ships
  FROM purgo_playground.patient_therapy_shipment
  WHERE LOWER(ship_type) = "commercial"
  GROUP BY patient_id, treatment_id
),
final_enriched AS (
  SELECT
    w4.product,
    w4.ship_date,
    w4.days_supply,
    w4.qty,
    w4.treatment_id,
    w4.dob,
    w4.first_ship_date,
    w4.refill_status,
    w4.patient_id,
    w4.ship_type,
    w4.shipment_arrived_status,
    w4.delivery_ontime,
    w4.shipment_expiry,
    w4.discontinuation_date,
    w4.days_until_next_ship,
    w4.days_since_last_fill,
    w4.expected_refill_date,
    w4.prior_ship,
    w4.days_between,
    w4.days_since_supply_out,
    w4.age,
    w4.age_at_first_ship,
    ls.latest_therapy_ships,
    CASE
      WHEN w4.refill_status = "DC - Standard" THEN "STANDARD"
      WHEN w4.refill_status = "DC-PERMANENT" THEN "PERMANENT"
      ELSE NULL
    END AS discontinuation_type
  FROM windowed4 w4
  LEFT JOIN latest_ships ls
    ON w4.patient_id = ls.patient_id AND w4.treatment_id = ls.treatment_id
)
SELECT * FROM final_enriched
;

/* 
==========================================================================================
  SECTION: FINAL OUTPUT - Display Enriched Table
==========================================================================================
*/

-- Display the enriched table for manual inspection
SELECT * FROM purgo_playground.enriched_patient_therapy_shipment
ORDER BY product, ship_date
;

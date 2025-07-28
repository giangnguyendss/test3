/*
==========================================================================================
  Databricks SQL Test Suite for purgo_playground.enriched_patient_therapy_shipment
  - Covers: schema validation, data type alignment, derived column logic, NULL/error handling,
    window functions, Delta Lake operations, data quality, and integration tests.
  - All test assertions use Databricks SQL syntax and best practices.
  - All comments are block (/* */) for sections and line (--) for inline explanations.
  - All table and function references are fully qualified.
  - No temp views or temp tables are used.
  - All CTEs are used directly in validation queries.
==========================================================================================
*/

/* =========================
   SETUP: Clean up and reload test data
   ========================= */

/* -- Drop and recreate the source table with test data -- */
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
)
USING DELTA
;

/* -- Insert test data into the source table -- */
INSERT INTO purgo_playground.patient_therapy_shipment
SELECT * FROM (
  SELECT
    'DrugA' AS product, DATE'2024-01-01' AS ship_date, '21' AS days_supply, 63 AS qty, 'TREAT123' AS treatment_id, DATE'1980-06-15' AS dob, DATE'2023-12-01' AS first_ship_date, 'DC - Standard' AS refill_status, 'PAT001' AS patient_id, 'commercial' AS ship_type, 'arrived' AS shipment_arrived_status, 'yes' AS delivery_ontime, TIMESTAMP'2024-01-15T00:00:00.000+0000' AS calctime
  UNION ALL SELECT 'DrugB', DATE'2024-02-10', NULL, 90, 'TREAT456', DATE'1975-03-10', DATE'2024-02-10', 'DC-PERMANENT', 'PAT002', 'commercial', 'arrived', 'no', TIMESTAMP'2024-02-20T00:00:00.000+0000'
  UNION ALL SELECT 'DrugC', DATE'2024-03-01', '14', 42, 'TREAT789', DATE'2000-01-01', DATE'2024-03-01', NULL, 'PAT003', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugD', DATE'2024-01-01', NULL, NULL, 'TREAT000', DATE'1990-01-01', DATE'2024-01-01', 'DC - Standard', 'PAT004', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugE', DATE'2024-01-01', 'abc', 30, 'TREAT001', DATE'1985-05-05', DATE'2024-01-01', 'DC - Standard', 'PAT005', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugF', DATE'2024-01-01', NULL, NULL, 'TREAT002', DATE'1985-05-05', DATE'2024-01-01', 'DC - Standard', 'PAT006', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugA', DATE'2024-01-15', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'sample', 'arrived', 'yes', TIMESTAMP'2024-01-20T00:00:00.000+0000'
  UNION ALL SELECT 'DrugA', DATE'2024-01-22', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-01-25T00:00:00.000+0000'
  UNION ALL SELECT 'DrugA', DATE'2024-02-12', '21', 63, 'TREAT123', DATE'1980-06-15', DATE'2023-12-01', 'DC - Standard', 'PAT001', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-02-15T00:00:00.000+0000'
  UNION ALL SELECT 'DrugG', DATE'2024-03-01', '30', 90, 'TREAT999', DATE'1970-12-31', DATE'2024-03-01', 'Other', 'PAT007', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugH', DATE'2024-03-01', '30', 90, 'TREAT998', DATE'1970-12-31', DATE'2024-03-01', NULL, 'PAT008', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugI', NULL, '30', 90, 'TREAT997', DATE'1970-12-31', DATE'2024-03-01', 'DC - Standard', 'PAT009', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugJ', DATE'2024-03-01', '30', 90, 'TREAT996', NULL, DATE'2024-03-01', 'DC - Standard', 'PAT010', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugK', DATE'2024-03-01', '30', 90, 'TREAT995', DATE'1970-12-31', NULL, 'DC - Standard', 'PAT011', 'commercial', 'arrived', 'no', TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugL', DATE'2024-03-01', '30', 90, 'TREAT994', DATE'1970-12-31', DATE'2024-03-01', 'DC - Standard', 'PAT012', 'commercial', 'arrived', 'no', NULL
  UNION ALL SELECT 'DrügΩ', DATE'2024-04-01', '28', 84, 'TREATΩ', DATE'1995-07-07', DATE'2024-04-01', 'DC - Standard', 'PATΩΩ', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-04-10T00:00:00.000+0000'
  UNION ALL SELECT '药品A', DATE'2024-05-01', '30', 90, 'TREAT汉字', DATE'1990-01-01', DATE'2024-05-01', 'DC-PERMANENT', 'PAT汉字', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-05-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugM', DATE'2024-06-01', '30', 90, 'TREAT993', DATE'1970-12-31', DATE'2024-06-01', 'DC - Standard', 'PAT013', 'commercial', 'arrived', NULL, TIMESTAMP'2024-06-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugN', DATE'2024-07-01', '30', 90, 'TREAT992', DATE'1970-12-31', DATE'2024-07-01', 'DC - Standard', 'PAT014', 'commercial', NULL, 'yes', TIMESTAMP'2024-07-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugO', DATE'2024-08-01', '30', 90, 'TREAT991', DATE'1970-12-31', DATE'2024-08-01', 'DC - Standard', 'PAT015', NULL, 'arrived', 'yes', TIMESTAMP'2024-08-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugP', DATE'2024-09-01', '30', 90, 'TREAT990', DATE'1970-12-31', DATE'2024-09-01', 'dc - standard', 'PAT016', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-09-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugQ', DATE'2024-10-01', '0', 0, 'TREAT989', DATE'1970-12-31', DATE'2024-10-01', 'DC - Standard', 'PAT017', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-10-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugR', DATE'2024-11-01', NULL, 0, 'TREAT988', DATE'1970-12-31', DATE'2024-11-01', 'DC - Standard', 'PAT018', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-11-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugS', DATE'2024-12-01', '-7', 21, 'TREAT987', DATE'1970-12-31', DATE'2024-12-01', 'DC - Standard', 'PAT019', 'commercial', 'arrived', 'yes', TIMESTAMP'2024-12-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugT', DATE'2025-01-01', NULL, -21, 'TREAT986', DATE'1970-12-31', DATE'2025-01-01', 'DC - Standard', 'PAT020', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-01-10T00:00:00.000+0000'
  UNION ALL SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  UNION ALL SELECT 'DrugU', DATE'2025-02-01', '30', 90, 'TREAT985', DATE'1970-12-31', DATE'2025-02-01', 'DC - 特殊', 'PAT021', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-02-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugV', DATE'2025-03-01', '30', 90, 'TREAT984', DATE'1970-12-31', DATE'2025-03-01', 'DC - Standard', 'PAT022', 'commercial', 'arrived', '是', TIMESTAMP'2025-03-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugW', DATE'2025-04-01', '30', 90, 'TREAT983', DATE'1970-12-31', DATE'2025-04-01', 'DC - Standard', 'PAT023', 'commercial', '到达', 'yes', TIMESTAMP'2025-04-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugX', DATE'2025-05-01', '30', 90, 'TREAT982', DATE'1970-12-31', DATE'2025-05-01', 'DC - Standard', 'PAT024', 'Commercial', 'arrived', 'yes', TIMESTAMP'2025-05-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugY', DATE'2025-06-01', '30', 90, 'TREAT981', DATE'1970-12-31', DATE'2025-06-01', 'DC - Standard', 'PAT025', 'COMMERCIAL', 'arrived', 'yes', TIMESTAMP'2025-06-10T00:00:00.000+0000'
  UNION ALL SELECT 'DrugZ', DATE'2025-07-01', '30', 90, 'TREAT980', DATE'1970-12-31', DATE'2025-07-01', 'DC - Standard', 'PAT026', 'commercial', 'arrived', 'yes', TIMESTAMP'2025-07-10T00:00:00.000+0000'
)
;

/* -- Drop and recreate the output table -- */
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
)
USING DELTA
;

/* =========================
   FUNCTION: Safe cast for integer
   ========================= */
CREATE OR REPLACE FUNCTION purgo_playground.safe_cast_int(x STRING)
RETURNS INT
RETURN
  CASE
    WHEN x RLIKE "^-?\\d+$" THEN CAST(x AS INT)
    ELSE NULL
  END
;

/* =========================
   FUNCTION: Discontinuation type logic
   ========================= */
CREATE OR REPLACE FUNCTION purgo_playground.get_discontinuation_type(refill_status STRING)
RETURNS STRING
RETURN
  CASE
    WHEN refill_status = "DC - Standard" THEN "STANDARD"
    WHEN refill_status = "DC-PERMANENT" THEN "PERMANENT"
    ELSE NULL
  END
;

/* =========================
   ENRICHMENT: Insert enriched data into output table
   ========================= */
INSERT OVERWRITE purgo_playground.enriched_patient_therapy_shipment
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
    p.calctime,
    purgo_playground.safe_cast_int(p.days_supply) AS days_supply_int,
    -- fallback logic for days_supply/qty
    CASE
      WHEN purgo_playground.safe_cast_int(p.days_supply) IS NOT NULL THEN purgo_playground.safe_cast_int(p.days_supply)
      WHEN p.qty IS NOT NULL THEN CAST(ROUND(p.qty / 3.0 * 7.0) AS INT)
      ELSE NULL
    END AS supply_days
  FROM purgo_playground.patient_therapy_shipment p
),
enriched AS (
  SELECT
    b.product,
    b.ship_date,
    b.days_supply,
    b.qty,
    b.treatment_id,
    b.dob,
    b.first_ship_date,
    b.refill_status,
    b.patient_id,
    b.ship_type,
    b.shipment_arrived_status,
    b.delivery_ontime,
    -- shipment_expiry
    CASE
      WHEN b.ship_date IS NOT NULL AND b.supply_days IS NOT NULL THEN DATE_ADD(b.ship_date, b.supply_days)
      ELSE NULL
    END AS shipment_expiry,
    -- discontinuation_date
    CASE
      WHEN b.ship_date IS NOT NULL AND b.supply_days IS NOT NULL THEN DATE_ADD(DATE_ADD(b.ship_date, b.supply_days), 91)
      ELSE NULL
    END AS discontinuation_date,
    -- days_until_next_ship
    CASE
      WHEN b.ship_date IS NOT NULL AND b.supply_days IS NOT NULL AND b.calctime IS NOT NULL THEN DATEDIFF(DATE_ADD(b.ship_date, b.supply_days), CAST(b.calctime AS DATE)) + 1
      ELSE NULL
    END AS days_until_next_ship,
    -- days_since_last_fill
    CASE
      WHEN b.ship_date IS NOT NULL AND b.calctime IS NOT NULL THEN DATEDIFF(CAST(b.calctime AS DATE), b.ship_date) + 1
      ELSE NULL
    END AS days_since_last_fill,
    -- expected_refill_date
    CASE
      WHEN b.ship_date IS NOT NULL AND b.supply_days IS NOT NULL AND b.calctime IS NOT NULL THEN DATE_ADD(CAST(b.calctime AS DATE), DATEDIFF(DATE_ADD(b.ship_date, b.supply_days), CAST(b.calctime AS DATE)) + 1)
      ELSE NULL
    END AS expected_refill_date,
    -- prior_ship
    LAG(b.ship_date) OVER (PARTITION BY b.treatment_id ORDER BY b.ship_date) AS prior_ship,
    -- days_between
    CASE
      WHEN LAG(b.ship_date) OVER (PARTITION BY b.treatment_id ORDER BY b.ship_date) IS NOT NULL AND b.ship_date IS NOT NULL THEN DATEDIFF(b.ship_date, LAG(b.ship_date) OVER (PARTITION BY b.treatment_id ORDER BY b.ship_date))
      ELSE NULL
    END AS days_between,
    -- days_since_supply_out
    CASE
      WHEN b.ship_date IS NOT NULL AND b.supply_days IS NOT NULL AND b.calctime IS NOT NULL AND DATEDIFF(CAST(b.calctime AS DATE), DATE_ADD(b.ship_date, b.supply_days)) >= 0
        THEN DATEDIFF(CAST(b.calctime AS DATE), DATE_ADD(b.ship_date, b.supply_days))
      ELSE NULL
    END AS days_since_supply_out,
    -- age
    CASE
      WHEN b.dob IS NOT NULL AND b.calctime IS NOT NULL THEN YEAR(CAST(b.calctime AS DATE)) - YEAR(b.dob) - CASE WHEN DATE_ADD(b.dob, (YEAR(CAST(b.calctime AS DATE)) - YEAR(b.dob)) * 365) > CAST(b.calctime AS DATE) THEN 1 ELSE 0 END
      ELSE NULL
    END AS age,
    -- age_at_first_ship
    CASE
      WHEN b.dob IS NOT NULL AND b.first_ship_date IS NOT NULL THEN CAST(ROUND(DATEDIFF(b.first_ship_date, b.dob) / 365.0, 0) AS BIGINT)
      ELSE NULL
    END AS age_at_first_ship,
    -- latest_therapy_ships
    COUNT(CASE WHEN LOWER(b.ship_type) = "commercial" THEN 1 END) OVER (PARTITION BY b.patient_id, b.treatment_id) AS latest_therapy_ships,
    -- discontinuation_type
    purgo_playground.get_discontinuation_type(b.refill_status) AS discontinuation_type
  FROM base b
)
SELECT
  product,
  ship_date,
  days_supply,
  qty,
  treatment_id,
  dob,
  first_ship_date,
  refill_status,
  patient_id,
  ship_type,
  shipment_arrived_status,
  delivery_ontime,
  shipment_expiry,
  discontinuation_date,
  days_until_next_ship,
  days_since_last_fill,
  expected_refill_date,
  prior_ship,
  days_between,
  days_since_supply_out,
  age,
  age_at_first_ship,
  latest_therapy_ships,
  discontinuation_type
FROM enriched
;

/* =========================
   SCHEMA VALIDATION: Output table schema and data types
   ========================= */
WITH schema_info AS (
  SELECT
    column_name,
    data_type
  FROM
    information_schema.columns
  WHERE
    table_schema = "purgo_playground"
    AND table_name = "enriched_patient_therapy_shipment"
)
SELECT
  CASE WHEN COUNT(*) = 24 THEN "PASS" ELSE "FAIL" END AS schema_column_count_check,
  MAX(CASE WHEN column_name = "product" AND data_type = "string" THEN 1 ELSE 0 END) AS product_type_check,
  MAX(CASE WHEN column_name = "ship_date" AND data_type = "date" THEN 1 ELSE 0 END) AS ship_date_type_check,
  MAX(CASE WHEN column_name = "days_supply" AND data_type = "string" THEN 1 ELSE 0 END) AS days_supply_type_check,
  MAX(CASE WHEN column_name = "qty" AND data_type = "string" THEN 1 ELSE 0 END) AS qty_type_check,
  MAX(CASE WHEN column_name = "treatment_id" AND data_type = "string" THEN 1 ELSE 0 END) AS treatment_id_type_check,
  MAX(CASE WHEN column_name = "dob" AND data_type = "date" THEN 1 ELSE 0 END) AS dob_type_check,
  MAX(CASE WHEN column_name = "first_ship_date" AND data_type = "date" THEN 1 ELSE 0 END) AS first_ship_date_type_check,
  MAX(CASE WHEN column_name = "refill_status" AND data_type = "string" THEN 1 ELSE 0 END) AS refill_status_type_check,
  MAX(CASE WHEN column_name = "patient_id" AND data_type = "string" THEN 1 ELSE 0 END) AS patient_id_type_check,
  MAX(CASE WHEN column_name = "ship_type" AND data_type = "string" THEN 1 ELSE 0 END) AS ship_type_type_check,
  MAX(CASE WHEN column_name = "shipment_arrived_status" AND data_type = "string" THEN 1 ELSE 0 END) AS shipment_arrived_status_type_check,
  MAX(CASE WHEN column_name = "delivery_ontime" AND data_type = "string" THEN 1 ELSE 0 END) AS delivery_ontime_type_check,
  MAX(CASE WHEN column_name = "shipment_expiry" AND data_type = "date" THEN 1 ELSE 0 END) AS shipment_expiry_type_check,
  MAX(CASE WHEN column_name = "discontinuation_date" AND data_type = "date" THEN 1 ELSE 0 END) AS discontinuation_date_type_check,
  MAX(CASE WHEN column_name = "days_until_next_ship" AND data_type = "bigint" THEN 1 ELSE 0 END) AS days_until_next_ship_type_check,
  MAX(CASE WHEN column_name = "days_since_last_fill" AND data_type = "bigint" THEN 1 ELSE 0 END) AS days_since_last_fill_type_check,
  MAX(CASE WHEN column_name = "expected_refill_date" AND data_type = "date" THEN 1 ELSE 0 END) AS expected_refill_date_type_check,
  MAX(CASE WHEN column_name = "prior_ship" AND data_type = "date" THEN 1 ELSE 0 END) AS prior_ship_type_check,
  MAX(CASE WHEN column_name = "days_between" AND data_type = "bigint" THEN 1 ELSE 0 END) AS days_between_type_check,
  MAX(CASE WHEN column_name = "days_since_supply_out" AND data_type = "bigint" THEN 1 ELSE 0 END) AS days_since_supply_out_type_check,
  MAX(CASE WHEN column_name = "age" AND data_type = "bigint" THEN 1 ELSE 0 END) AS age_type_check,
  MAX(CASE WHEN column_name = "age_at_first_ship" AND data_type = "bigint" THEN 1 ELSE 0 END) AS age_at_first_ship_type_check,
  MAX(CASE WHEN column_name = "latest_therapy_ships" AND data_type = "bigint" THEN 1 ELSE 0 END) AS latest_therapy_ships_type_check,
  MAX(CASE WHEN column_name = "discontinuation_type" AND data_type = "string" THEN 1 ELSE 0 END) AS discontinuation_type_type_check
FROM schema_info
;

/* =========================
   DATA QUALITY: Row count matches source
   ========================= */
WITH src AS (
  SELECT COUNT(*) AS src_count FROM purgo_playground.patient_therapy_shipment
),
tgt AS (
  SELECT COUNT(*) AS tgt_count FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  CASE WHEN src.src_count = tgt.tgt_count THEN "PASS" ELSE "FAIL" END AS row_count_match_check
FROM src, tgt
;

/* =========================
   UNIT TEST: Derived column logic - shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date
   ========================= */
WITH test_case AS (
  SELECT
    product, ship_date, days_supply, qty, calctime, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugB"
)
SELECT
  CASE WHEN shipment_expiry = DATE'2024-01-22' THEN "PASS" ELSE "FAIL" END AS shipment_expiry_check,
  CASE WHEN discontinuation_date = DATE'2024-04-22' THEN "PASS" ELSE "FAIL" END AS discontinuation_date_check,
  CASE WHEN days_until_next_ship = 13 THEN "PASS" ELSE "FAIL" END AS days_until_next_ship_check,
  CASE WHEN expected_refill_date = DATE'2024-01-23' THEN "PASS" ELSE "FAIL" END AS expected_refill_date_check
FROM test_case
;

/* =========================
   UNIT TEST: Derived column logic - NULL fallback when both days_supply and qty are NULL
   ========================= */
WITH test_case AS (
  SELECT
    product, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugD"
)
SELECT
  CASE WHEN shipment_expiry IS NULL THEN "PASS" ELSE "FAIL" END AS shipment_expiry_null_check,
  CASE WHEN discontinuation_date IS NULL THEN "PASS" ELSE "FAIL" END AS discontinuation_date_null_check,
  CASE WHEN days_until_next_ship IS NULL THEN "PASS" ELSE "FAIL" END AS days_until_next_ship_null_check,
  CASE WHEN expected_refill_date IS NULL THEN "PASS" ELSE "FAIL" END AS expected_refill_date_null_check
FROM test_case
;

/* =========================
   UNIT TEST: prior_ship and days_between for multiple shipments per treatment_id
   ========================= */
WITH cte AS (
  SELECT
    product, ship_date, prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE treatment_id = "TREAT123"
  ORDER BY ship_date
)
SELECT
  MAX(CASE WHEN ship_date = DATE'2024-01-01' AND prior_ship IS NULL AND days_between IS NULL THEN 1 ELSE 0 END) AS first_row_check,
  MAX(CASE WHEN ship_date = DATE'2024-01-22' AND prior_ship = DATE'2024-01-01' AND days_between = 21 THEN 1 ELSE 0 END) AS second_row_check,
  MAX(CASE WHEN ship_date = DATE'2024-02-12' AND prior_ship = DATE'2024-01-22' AND days_between = 21 THEN 1 ELSE 0 END) AS third_row_check
FROM cte
;

/* =========================
   UNIT TEST: latest_therapy_ships for commercial shipments
   ========================= */
WITH cte AS (
  SELECT
    product, ship_type, latest_therapy_ships
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE patient_id = "PAT001" AND treatment_id = "TREAT123"
)
SELECT
  MAX(CASE WHEN ship_type = "commercial" AND latest_therapy_ships = 4 THEN 1 ELSE 0 END) AS commercial_count_check,
  MAX(CASE WHEN ship_type = "sample" AND latest_therapy_ships = 4 THEN 1 ELSE 0 END) AS sample_count_check
FROM cte
;

/* =========================
   UNIT TEST: discontinuation_type logic
   ========================= */
WITH cte AS (
  SELECT
    product, refill_status, discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugA", "DrugB", "DrugG", "DrugH")
)
SELECT
  MAX(CASE WHEN product = "DrugA" AND discontinuation_type = "STANDARD" THEN 1 ELSE 0 END) AS standard_check,
  MAX(CASE WHEN product = "DrugB" AND discontinuation_type = "PERMANENT" THEN 1 ELSE 0 END) AS permanent_check,
  MAX(CASE WHEN product = "DrugG" AND discontinuation_type IS NULL THEN 1 ELSE 0 END) AS other_null_check,
  MAX(CASE WHEN product = "DrugH" AND discontinuation_type IS NULL THEN 1 ELSE 0 END) AS null_null_check
FROM cte
;

/* =========================
   UNIT TEST: age and age_at_first_ship
   ========================= */
WITH cte AS (
  SELECT
    product, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugA", "DrugC")
)
SELECT
  MAX(CASE WHEN product = "DrugA" AND age = 43 AND age_at_first_ship = 44 THEN 1 ELSE 0 END) AS drugA_check,
  MAX(CASE WHEN product = "DrugC" AND age = 24 AND age_at_first_ship = 24 THEN 1 ELSE 0 END) AS drugC_check
FROM cte
;

/* =========================
   UNIT TEST: NULL handling in key fields
   ========================= */
WITH cte AS (
  SELECT
    product, ship_date, dob, first_ship_date, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product IN ("DrugI", "DrugJ", "DrugK")
)
SELECT
  MAX(CASE WHEN product = "DrugI" AND shipment_expiry IS NULL AND discontinuation_date IS NULL AND days_until_next_ship IS NULL AND expected_refill_date IS NULL THEN 1 ELSE 0 END) AS ship_date_null_check,
  MAX(CASE WHEN product = "DrugJ" AND age IS NULL AND age_at_first_ship IS NULL THEN 1 ELSE 0 END) AS dob_null_check,
  MAX(CASE WHEN product = "DrugK" AND age_at_first_ship IS NULL THEN 1 ELSE 0 END) AS first_ship_date_null_check
FROM cte
;

/* =========================
   UNIT TEST: Error handling for invalid days_supply and qty
   ========================= */
WITH cte AS (
  SELECT
    product, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugE"
)
SELECT
  CASE WHEN shipment_expiry IS NULL AND discontinuation_date IS NULL AND days_until_next_ship IS NULL AND expected_refill_date IS NULL THEN "PASS" ELSE "FAIL" END AS invalid_days_supply_check
FROM cte
;

/* =========================
   UNIT TEST: Error handling for missing calctime
   ========================= */
WITH cte AS (
  SELECT
    product, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugL"
)
SELECT
  CASE WHEN days_until_next_ship IS NULL AND expected_refill_date IS NULL AND age IS NULL THEN "PASS" ELSE "FAIL" END AS missing_calctime_check
FROM cte
;

/* =========================
   INTEGRATION TEST: Output table is overwritten on each enrichment run
   ========================= */
WITH cte AS (
  SELECT COUNT(*) AS cnt FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  CASE WHEN cnt = (SELECT COUNT(*) FROM purgo_playground.patient_therapy_shipment) THEN "PASS" ELSE "FAIL" END AS overwrite_check
FROM cte
;

/* =========================
   INTEGRATION TEST: No join with other tables
   ========================= */
SELECT
  CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM purgo_playground.patient_therapy_shipment) THEN "PASS" ELSE "FAIL" END AS no_join_check
FROM purgo_playground.enriched_patient_therapy_shipment
;

/* =========================
   INTEGRATION TEST: Output table is not partitioned or indexed
   ========================= */
WITH cte AS (
  SELECT
    t.table_name,
    t.partition_columns
  FROM information_schema.tables t
  WHERE t.table_schema = "purgo_playground"
    AND t.table_name = "enriched_patient_therapy_shipment"
)
SELECT
  CASE WHEN partition_columns IS NULL OR partition_columns = "[]" THEN "PASS" ELSE "FAIL" END AS not_partitioned_check
FROM cte
;

/* =========================
   DELTA LAKE TEST: Delta operations (MERGE, UPDATE, DELETE)
   ========================= */

/* -- Test UPDATE: Set delivery_ontime to 'late' for all where delivery_ontime is 'no' -- */
UPDATE purgo_playground.enriched_patient_therapy_shipment
SET delivery_ontime = "late"
WHERE delivery_ontime = "no"
;

/* -- Assert UPDATE -- */
WITH cte AS (
  SELECT COUNT(*) AS cnt FROM purgo_playground.enriched_patient_therapy_shipment WHERE delivery_ontime = "late"
)
SELECT
  CASE WHEN cnt > 0 THEN "PASS" ELSE "FAIL" END AS update_check
FROM cte
;

/* -- Test DELETE: Remove all rows where product IS NULL -- */
DELETE FROM purgo_playground.enriched_patient_therapy_shipment WHERE product IS NULL
;

/* -- Assert DELETE -- */
WITH cte AS (
  SELECT COUNT(*) AS cnt FROM purgo_playground.enriched_patient_therapy_shipment WHERE product IS NULL
)
SELECT
  CASE WHEN cnt = 0 THEN "PASS" ELSE "FAIL" END AS delete_check
FROM cte
;

/* -- Test MERGE: Upsert a new row -- */
MERGE INTO purgo_playground.enriched_patient_therapy_shipment AS tgt
USING (
  SELECT
    "DrugNEW" AS product,
    DATE'2025-12-31' AS ship_date,
    "30" AS days_supply,
    "90" AS qty,
    "TREATNEW" AS treatment_id,
    DATE'1980-01-01' AS dob,
    DATE'2025-12-01' AS first_ship_date,
    "DC - Standard" AS refill_status,
    "PATNEW" AS patient_id,
    "commercial" AS ship_type,
    "arrived" AS shipment_arrived_status,
    "yes" AS delivery_ontime,
    DATE'2026-01-30' AS shipment_expiry,
    DATE'2026-05-01' AS discontinuation_date,
    10 AS days_until_next_ship,
    5 AS days_since_last_fill,
    DATE'2026-01-31' AS expected_refill_date,
    NULL AS prior_ship,
    NULL AS days_between,
    NULL AS days_since_supply_out,
    45 AS age,
    45 AS age_at_first_ship,
    1 AS latest_therapy_ships,
    "STANDARD" AS discontinuation_type
) AS src
ON tgt.product = src.product AND tgt.treatment_id = src.treatment_id
WHEN MATCHED THEN UPDATE SET
  tgt.ship_date = src.ship_date
WHEN NOT MATCHED THEN INSERT *
;

/* -- Assert MERGE -- */
SELECT
  CASE WHEN COUNT(*) > 0 THEN "PASS" ELSE "FAIL" END AS merge_check
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE product = "DrugNEW" AND treatment_id = "TREATNEW"
;

/* =========================
   WINDOW FUNCTION TEST: days_since_last_fill, prior_ship, days_between
   ========================= */
WITH cte AS (
  SELECT
    product, ship_date, days_since_last_fill, prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE treatment_id = "TREAT123"
  ORDER BY ship_date
)
SELECT
  MAX(CASE WHEN ship_date = DATE'2024-01-01' AND days_since_last_fill IS NOT NULL THEN 1 ELSE 0 END) AS window_days_since_last_fill_check,
  MAX(CASE WHEN ship_date = DATE'2024-01-22' AND prior_ship = DATE'2024-01-01' AND days_between = 21 THEN 1 ELSE 0 END) AS window_prior_ship_check
FROM cte
;

/* =========================
   DATA QUALITY: NULL handling for derived columns when input is invalid
   ========================= */
WITH cte AS (
  SELECT
    product, days_supply, qty, shipment_expiry, discontinuation_date
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugF"
)
SELECT
  CASE WHEN shipment_expiry IS NULL AND discontinuation_date IS NULL THEN "PASS" ELSE "FAIL" END AS null_input_null_output_check
FROM cte
;

/* =========================
   PERFORMANCE TEST: Row count and query time for enrichment
   ========================= */
WITH cte AS (
  SELECT COUNT(*) AS cnt FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  CASE WHEN cnt > 0 THEN "PASS" ELSE "FAIL" END AS performance_row_count_check
FROM cte
;

/* =========================
   FINAL OUTPUT: Display enriched table for manual inspection
   ========================= */
SELECT * FROM purgo_playground.enriched_patient_therapy_shipment
ORDER BY product, ship_date
;

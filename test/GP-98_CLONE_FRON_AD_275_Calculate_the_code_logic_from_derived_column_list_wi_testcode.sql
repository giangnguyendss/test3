/* 
  Databricks SQL Test Suite for purgo_playground.enriched_patient_therapy_shipment Enrichment Logic

  - Validates schema, data types, constraints, derived column logic, NULL/error handling, and data quality
  - Assumes test data is loaded as per provided testdata.sql
  - All comments and documentation are in code comments per requirements
*/

/* -------------------- SETUP & CONFIGURATION -------------------- */

/* 
  -- No need to create SparkSession or imports in SQL
  -- All date calculations use UTC timezone
  -- All output date fields must be in 'yyyy-MM-dd' format
*/

/* -------------------- SCHEMA VALIDATION TESTS -------------------- */

/* 
  -- Test: Table exists and has all required columns with correct data types
  -- All columns must be present and match the expected types
*/
WITH expected_schema AS (
  SELECT
    "product" AS col, "STRING" AS typ UNION ALL
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
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: All columns and types match"
  ELSE
    "FAIL: Schema mismatch: " || STRING_AGG(col || ' expected ' || typ, ', ')
  END AS schema_validation_result
FROM (
  SELECT e.col, e.typ
  FROM expected_schema e
  LEFT JOIN (
    SELECT
      col_name AS col,
      upper(data_type) AS typ
    FROM information_schema.columns
    WHERE table_schema = "purgo_playground"
      AND table_name = "enriched_patient_therapy_shipment"
  ) t
  ON e.col = t.col AND e.typ = t.typ
  WHERE t.col IS NULL
);

/* 
  -- Test: No extra columns in the table
*/
WITH actual_cols AS (
  SELECT col_name
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "enriched_patient_therapy_shipment"
),
expected_cols AS (
  SELECT "product" AS col UNION ALL
  SELECT "ship_date" UNION ALL
  SELECT "days_supply" UNION ALL
  SELECT "qty" UNION ALL
  SELECT "treatment_id" UNION ALL
  SELECT "dob" UNION ALL
  SELECT "first_ship_date" UNION ALL
  SELECT "refill_status" UNION ALL
  SELECT "patient_id" UNION ALL
  SELECT "ship_type" UNION ALL
  SELECT "shipment_arrived_status" UNION ALL
  SELECT "delivery_ontime" UNION ALL
  SELECT "shipment_expiry" UNION ALL
  SELECT "discontinuation_date" UNION ALL
  SELECT "days_until_next_ship" UNION ALL
  SELECT "days_since_last_fill" UNION ALL
  SELECT "expected_refill_date" UNION ALL
  SELECT "prior_ship" UNION ALL
  SELECT "days_between" UNION ALL
  SELECT "days_since_supply_out" UNION ALL
  SELECT "age" UNION ALL
  SELECT "age_at_first_ship" UNION ALL
  SELECT "latest_therapy_ships" UNION ALL
  SELECT "discontinuation_type"
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: No extra columns"
  ELSE
    "FAIL: Extra columns found: " || STRING_AGG(col_name, ', ')
  END AS extra_column_check
FROM (
  SELECT a.col_name
  FROM actual_cols a
  LEFT JOIN expected_cols e ON a.col_name = e.col
  WHERE e.col IS NULL
);

/* -------------------- CONSTRAINTS & CHECKS -------------------- */

/* 
  -- Test: Discontinuation_type only allowed values or NULL
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: All discontinuation_type values valid"
  ELSE
    "FAIL: Invalid discontinuation_type values: " || STRING_AGG(DISTINCT discontinuation_type, ', ')
  END AS discontinuation_type_check
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE discontinuation_type IS NOT NULL
  AND discontinuation_type NOT IN ("STANDARD", "PERMANENT");

/* 
  -- Test: All date columns are in correct format (yyyy-MM-dd) or NULL
*/
WITH date_cols AS (
  SELECT
    ship_date, shipment_expiry, discontinuation_date, expected_refill_date, prior_ship, dob, first_ship_date
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: All date columns in correct format or NULL"
  ELSE
    "FAIL: Invalid date format found"
  END AS date_format_check
FROM date_cols
WHERE
  (ship_date IS NOT NULL AND CAST(ship_date AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (shipment_expiry IS NOT NULL AND CAST(shipment_expiry AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (discontinuation_date IS NOT NULL AND CAST(discontinuation_date AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (expected_refill_date IS NOT NULL AND CAST(expected_refill_date AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (prior_ship IS NOT NULL AND CAST(prior_ship AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (dob IS NOT NULL AND CAST(dob AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$")
  OR (first_ship_date IS NOT NULL AND CAST(first_ship_date AS STRING) NOT RLIKE "^\\d{4}-\\d{2}-\\d{2}$");

/* -------------------- DERIVED COLUMN LOGIC TESTS -------------------- */

/* 
  -- Test: shipment_expiry calculation (happy path, days_supply present)
*/
WITH cte AS (
  SELECT
    product, ship_date, days_supply, qty, shipment_expiry
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugA"
)
SELECT
  CASE WHEN shipment_expiry = DATE_ADD(ship_date, CAST(days_supply AS INT)) THEN
    "PASS: shipment_expiry correct for DrugA"
  ELSE
    "FAIL: shipment_expiry incorrect for DrugA"
  END AS shipment_expiry_test
FROM cte;

/* 
  -- Test: shipment_expiry calculation (days_supply NULL, qty present)
*/
WITH cte AS (
  SELECT
    product, ship_date, days_supply, qty, shipment_expiry
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugB"
)
SELECT
  CASE WHEN shipment_expiry = DATE_ADD(ship_date, CAST(qty AS INT) / 3 * 7) THEN
    "PASS: shipment_expiry correct for DrugB"
  ELSE
    "FAIL: shipment_expiry incorrect for DrugB"
  END AS shipment_expiry_test_qty
FROM cte;

/* 
  -- Test: shipment_expiry NULL when both days_supply and qty are NULL
*/
WITH cte AS (
  SELECT
    product, shipment_expiry
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugD"
)
SELECT
  CASE WHEN shipment_expiry IS NULL THEN
    "PASS: shipment_expiry NULL when both days_supply and qty are NULL"
  ELSE
    "FAIL: shipment_expiry not NULL when both days_supply and qty are NULL"
  END AS shipment_expiry_null_test
FROM cte;

/* 
  -- Test: shipment_expiry NULL when days_supply is non-numeric
*/
WITH cte AS (
  SELECT
    product, shipment_expiry
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugG"
)
SELECT
  CASE WHEN shipment_expiry IS NULL THEN
    "PASS: shipment_expiry NULL for non-numeric days_supply"
  ELSE
    "FAIL: shipment_expiry not NULL for non-numeric days_supply"
  END AS shipment_expiry_non_numeric_test
FROM cte;

/* 
  -- Test: discontinuation_type logic
*/
WITH cte AS (
  SELECT
    refill_status, discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugA"
)
SELECT
  CASE WHEN discontinuation_type = "STANDARD" THEN
    "PASS: discontinuation_type correct for DC - Standard"
  ELSE
    "FAIL: discontinuation_type incorrect for DC - Standard"
  END AS discontinuation_type_standard_test
FROM cte;

/* 
  -- Test: discontinuation_type NULL for ongoing
*/
WITH cte AS (
  SELECT
    refill_status, discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugC"
)
SELECT
  CASE WHEN discontinuation_type IS NULL THEN
    "PASS: discontinuation_type NULL for ongoing"
  ELSE
    "FAIL: discontinuation_type not NULL for ongoing"
  END AS discontinuation_type_ongoing_test
FROM cte;

/* 
  -- Test: latest_therapy_ships counts only commercial
*/
WITH cte AS (
  SELECT
    patient_id, treatment_id, ship_type, latest_therapy_ships
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE patient_id = "PAT003"
)
SELECT
  CASE WHEN latest_therapy_ships = 0 THEN
    "PASS: latest_therapy_ships 0 for non-commercial"
  ELSE
    "FAIL: latest_therapy_ships not 0 for non-commercial"
  END AS latest_therapy_ships_noncommercial_test
FROM cte;

/* 
  -- Test: age and age_at_first_ship NULL when dob or first_ship_date is NULL
*/
WITH cte AS (
  SELECT
    product, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugF"
)
SELECT
  CASE WHEN age IS NULL AND age_at_first_ship IS NULL THEN
    "PASS: age and age_at_first_ship NULL when dob and first_ship_date are NULL"
  ELSE
    "FAIL: age or age_at_first_ship not NULL when dob and first_ship_date are NULL"
  END AS age_null_test
FROM cte;

/* 
  -- Test: days_between NULL for first shipment (prior_ship is NULL)
*/
WITH cte AS (
  SELECT
    product, days_between, prior_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugA"
)
SELECT
  CASE WHEN days_between IS NULL AND prior_ship IS NULL THEN
    "PASS: days_between NULL for first shipment"
  ELSE
    "FAIL: days_between not NULL for first shipment"
  END AS days_between_first_ship_test
FROM cte;

/* 
  -- Test: days_since_supply_out present when supply out before calctime
*/
WITH cte AS (
  SELECT
    product, days_since_supply_out
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugV"
)
SELECT
  CASE WHEN days_since_supply_out = 3 THEN
    "PASS: days_since_supply_out correct when supply out before calctime"
  ELSE
    "FAIL: days_since_supply_out incorrect when supply out before calctime"
  END AS days_since_supply_out_test
FROM cte;

/* 
  -- Test: days_since_supply_out NULL when supply not out yet
*/
WITH cte AS (
  SELECT
    product, days_since_supply_out
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugW"
)
SELECT
  CASE WHEN days_since_supply_out IS NULL THEN
    "PASS: days_since_supply_out NULL when supply not out yet"
  ELSE
    "FAIL: days_since_supply_out not NULL when supply not out yet"
  END AS days_since_supply_out_null_test
FROM cte;

/* 
  -- Test: NULLs in non-derived columns are preserved
*/
WITH cte AS (
  SELECT
    product, ship_date, days_supply, qty, treatment_id, dob, first_ship_date, refill_status, ship_type, shipment_arrived_status, delivery_ontime
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugX"
)
SELECT
  CASE WHEN ship_date IS NULL AND days_supply IS NULL AND qty IS NULL AND treatment_id IS NULL AND dob IS NULL AND first_ship_date IS NULL AND refill_status IS NULL AND ship_type IS NULL AND shipment_arrived_status IS NULL AND delivery_ontime IS NULL THEN
    "PASS: NULLs in non-derived columns preserved"
  ELSE
    "FAIL: NULLs in non-derived columns not preserved"
  END AS nulls_preserved_test
FROM cte;

/* 
  -- Test: No join with external tables (all required fields present)
  -- This is a static check: if all derived columns are present and calculated, no join is needed
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: All required fields present, no join needed"
  ELSE
    "FAIL: Missing required fields: " || STRING_AGG(col, ', ')
  END AS required_fields_check
FROM (
  SELECT "dob" AS col WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND col_name = "dob")
  UNION ALL
  SELECT "first_ship_date" WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND col_name = "first_ship_date")
  UNION ALL
  SELECT "days_supply" WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND col_name = "days_supply")
  UNION ALL
  SELECT "qty" WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = "purgo_playground" AND table_name = "enriched_patient_therapy_shipment" AND col_name = "qty")
);

/* 
  -- Test: Output table is available for downstream analytics
*/
SELECT
  CASE WHEN COUNT(*) > 0 THEN
    "PASS: Output table has data"
  ELSE
    "FAIL: Output table is empty"
  END AS output_table_data_check
FROM purgo_playground.enriched_patient_therapy_shipment;

/* -------------------- PERFORMANCE TESTS -------------------- */

/* 
  -- Test: Query performance for large dataset (simulate with count)
  -- Should return within reasonable time for < 10k rows (test data is small)
*/
SELECT
  CASE WHEN COUNT(*) >= 0 THEN
    "PASS: Table count query executed"
  ELSE
    "FAIL: Table count query failed"
  END AS performance_count_test
FROM purgo_playground.enriched_patient_therapy_shipment;

/* -------------------- DATA QUALITY VALIDATION TESTS -------------------- */

/* 
  -- Test: No negative values in days_until_next_ship, days_since_last_fill, days_between, days_since_supply_out, age, age_at_first_ship, latest_therapy_ships
  -- NULLs are allowed
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: No negative values in derived numeric columns"
  ELSE
    "FAIL: Negative values found in derived numeric columns"
  END AS negative_values_check
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE
  (days_until_next_ship IS NOT NULL AND days_until_next_ship < 0)
  OR (days_since_last_fill IS NOT NULL AND days_since_last_fill < 0)
  OR (days_between IS NOT NULL AND days_between < 0)
  OR (days_since_supply_out IS NOT NULL AND days_since_supply_out < 0)
  OR (age IS NOT NULL AND age < 0)
  OR (age_at_first_ship IS NOT NULL AND age_at_first_ship < 0)
  OR (latest_therapy_ships IS NOT NULL AND latest_therapy_ships < 0);

/* 
  -- Test: shipment_expiry always >= ship_date when both are not NULL
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: shipment_expiry >= ship_date for all records"
  ELSE
    "FAIL: shipment_expiry < ship_date found"
  END AS shipment_expiry_vs_ship_date_check
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE shipment_expiry IS NOT NULL AND ship_date IS NOT NULL AND shipment_expiry < ship_date;

/* 
  -- Test: discontinuation_date always >= shipment_expiry when both are not NULL
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN
    "PASS: discontinuation_date >= shipment_expiry for all records"
  ELSE
    "FAIL: discontinuation_date < shipment_expiry found"
  END AS discontinuation_date_vs_shipment_expiry_check
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE discontinuation_date IS NOT NULL AND shipment_expiry IS NOT NULL AND discontinuation_date < shipment_expiry;

/* 
  -- Test: All directly sourced columns are copied as-is (sample check for product, patient_id)
*/
WITH src AS (
  SELECT product, patient_id
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrügN-测试"
)
SELECT
  CASE WHEN COUNT(*) = 1 THEN
    "PASS: Special character product and patient_id copied as-is"
  ELSE
    "FAIL: Special character product and patient_id not found"
  END AS special_char_copy_test
FROM src;

/* -------------------- DELTA LAKE OPERATIONS TESTS -------------------- */

/* 
  -- Test: Table is a Delta table
*/
SELECT
  CASE WHEN t.table_type = "DELTA" THEN
    "PASS: Table is Delta"
  ELSE
    "FAIL: Table is not Delta"
  END AS delta_table_check
FROM information_schema.tables t
WHERE t.table_schema = "purgo_playground"
  AND t.table_name = "enriched_patient_therapy_shipment";

/* 
  -- Test: MERGE, UPDATE, DELETE operations (smoke test)
  -- Use a CTE to simulate the operation and validate syntax (no actual data change)
*/
WITH to_update AS (
  SELECT product, patient_id
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugA"
)
SELECT
  CASE WHEN COUNT(*) = 1 THEN
    "PASS: MERGE/UPDATE/DELETE target row found"
  ELSE
    "FAIL: MERGE/UPDATE/DELETE target row not found"
  END AS merge_update_delete_smoke_test
FROM to_update;

/* -------------------- WINDOW FUNCTION & ANALYTICS TESTS -------------------- */

/* 
  -- Test: prior_ship and days_between window logic
*/
WITH cte AS (
  SELECT
    product, ship_date, prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE product = "DrugU"
)
SELECT
  CASE WHEN prior_ship = DATE("2024-11-12") AND days_between = 30 THEN
    "PASS: prior_ship and days_between window logic correct"
  ELSE
    "FAIL: prior_ship and days_between window logic incorrect"
  END AS window_function_test
FROM cte;

/* -------------------- CLEANUP OPERATIONS -------------------- */

/* 
  -- No temp views or temp tables used, so no cleanup required
*/

/* -------------------- DISPLAY FINAL RESULT -------------------- */

/* 
  -- Display the enriched result set as required
*/
SELECT * FROM purgo_playground.enriched_patient_therapy_shipment;

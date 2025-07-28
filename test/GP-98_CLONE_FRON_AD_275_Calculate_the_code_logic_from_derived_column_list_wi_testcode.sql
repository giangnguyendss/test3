/* 
  Databricks SQL Test Suite for purgo_playground.enriched_patient_therapy_shipment Enrichment Logic

  - Validates schema, data types, constraints, derived column logic, NULL/error handling, and data quality
  - Assumes test data is loaded as per provided testdata.sql
  - All comments explain the test logic and intent
*/

/* -------------------- SETUP & CONFIGURATION -------------------- */

/* 
  -- No need to create SparkSession or import modules in SQL
  -- All operations use UTC timezone as per requirements
*/

/* -------------------- SCHEMA VALIDATION TESTS -------------------- */

/* 
  -- Test: Validate that the enriched table exists and has the correct schema and data types
*/
WITH expected_schema AS (
  SELECT
    "product" AS column_name, "STRING" AS data_type UNION ALL
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
  es.column_name,
  es.data_type,
  ts.data_type AS actual_data_type
FROM expected_schema es
LEFT JOIN (
  SELECT
    column_name,
    upper(data_type) AS data_type
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "enriched_patient_therapy_shipment"
) ts
  ON es.column_name = ts.column_name
WHERE es.data_type != ts.data_type OR ts.column_name IS NULL
;
/*
  -- Assertion: The above query should return 0 rows (all columns present and correct types)
*/

/* -------------------- CONSTRAINTS & CHECKS -------------------- */

/*
  -- Test: Validate allowed values for discontinuation_type
*/
WITH invalid_dc_type AS (
  SELECT discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
  WHERE discontinuation_type IS NOT NULL
    AND discontinuation_type NOT IN ("STANDARD", "PERMANENT")
)
SELECT * FROM invalid_dc_type
;
/*
  -- Assertion: Should return 0 rows (only allowed values or NULL)
*/

/* -------------------- DERIVED COLUMN LOGIC TESTS -------------------- */

/*
  -- Test: shipment_expiry calculation
  -- For each row, shipment_expiry = DATE_ADD(ship_date, cast(COALESCE(days_supply, qty/3*7) as int))
  -- Handles NULLs and non-numeric gracefully
*/
WITH cte AS (
  SELECT
    product, ship_date, days_supply, qty, shipment_expiry,
    TRY_CAST(days_supply AS INT) AS ds_int,
    TRY_CAST(qty AS INT) AS qty_int
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, ship_date, days_supply, qty, shipment_expiry,
  CASE
    WHEN ds_int IS NOT NULL THEN DATE_ADD(ship_date, ds_int)
    WHEN ds_int IS NULL AND qty_int IS NOT NULL THEN DATE_ADD(ship_date, CAST(qty_int / 3 * 7 AS INT))
    ELSE NULL
  END AS expected_shipment_expiry
FROM cte
WHERE
  (shipment_expiry IS DISTINCT FROM
    CASE
      WHEN ds_int IS NOT NULL THEN DATE_ADD(ship_date, ds_int)
      WHEN ds_int IS NULL AND qty_int IS NOT NULL THEN DATE_ADD(ship_date, CAST(qty_int / 3 * 7 AS INT))
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (all shipment_expiry values match logic)
*/

/*
  -- Test: discontinuation_date = DATE_ADD(shipment_expiry, 91)
*/
WITH cte AS (
  SELECT
    shipment_expiry, discontinuation_date
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT *
FROM cte
WHERE
  (discontinuation_date IS NOT NULL AND shipment_expiry IS NOT NULL AND discontinuation_date != DATE_ADD(shipment_expiry, 91))
  OR (discontinuation_date IS NULL AND shipment_expiry IS NOT NULL)
  OR (discontinuation_date IS NOT NULL AND shipment_expiry IS NULL)
;
/*
  -- Assertion: Should return 0 rows (discontinuation_date matches logic or both NULL)
*/

/*
  -- Test: days_until_next_ship = DATEDIFF(shipment_expiry, '{calctime}') + 1
  -- Use calctime from test data for each row
*/
WITH test_cases AS (
  SELECT
    product, ship_date, days_supply, qty, shipment_expiry, days_until_next_ship,
    CASE
      WHEN product = "DrugA" THEN DATE("2024-02-01")
      WHEN product = "DrugB" THEN DATE("2024-04-01")
      WHEN product = "DrugC" THEN DATE("2024-06-01")
      WHEN product = "DrugD" THEN DATE("2024-02-01")
      WHEN product = "DrugE" THEN DATE("2024-04-01")
      WHEN product = "DrugF" THEN DATE("2024-06-01")
      WHEN product = "DrugG" THEN DATE("2024-02-01")
      WHEN product = "DrugH" THEN DATE("2024-04-01")
      WHEN product = "DrugI" THEN DATE("2024-02-01")
      WHEN product = "DrugJ" THEN DATE("2024-02-02")
      WHEN product = "DrugK" THEN DATE("2024-01-01")
      WHEN product = "DrugL" THEN DATE("2024-03-01")
      WHEN product = "DrugM" THEN DATE("2024-04-01")
      WHEN product = "DrügN-测试" THEN DATE("2024-05-05")
      WHEN product = "💊DrugO" THEN DATE("2024-06-06")
      WHEN product = "DrugP" THEN DATE("2024-07-07")
      WHEN product = "DrugQ" THEN DATE("2024-08-08")
      WHEN product = "DrugR" THEN DATE("2024-09-09")
      WHEN product = "DrugS" THEN DATE("2024-10-10")
      WHEN product = "DrugT" THEN DATE("2024-11-11")
      WHEN product = "DrugU" THEN DATE("2024-12-12")
      WHEN product = "DrugV" THEN DATE("2024-02-01")
      WHEN product = "DrugW" THEN DATE("2024-12-01")
      WHEN product = "DrugX" THEN NULL
      WHEN product = "DrugY" THEN DATE("2024-06-15")
      WHEN product = "DrugZ" THEN DATE("2024-07-20")
      ELSE NULL
    END AS calctime
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, shipment_expiry, days_until_next_ship, calctime,
  CASE
    WHEN shipment_expiry IS NOT NULL AND calctime IS NOT NULL
      THEN DATEDIFF(shipment_expiry, calctime) + 1
    ELSE NULL
  END AS expected_days_until_next_ship
FROM test_cases
WHERE
  (days_until_next_ship IS DISTINCT FROM
    CASE
      WHEN shipment_expiry IS NOT NULL AND calctime IS NOT NULL
        THEN DATEDIFF(shipment_expiry, calctime) + 1
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (days_until_next_ship matches logic)
*/

/*
  -- Test: days_since_last_fill = DATEDIFF(calctime, ship_date) + 1
*/
WITH test_cases AS (
  SELECT
    product, ship_date, days_since_last_fill,
    CASE
      WHEN product = "DrugA" THEN DATE("2024-02-01")
      WHEN product = "DrugB" THEN DATE("2024-04-01")
      WHEN product = "DrugC" THEN DATE("2024-06-01")
      WHEN product = "DrugD" THEN DATE("2024-02-01")
      WHEN product = "DrugE" THEN DATE("2024-04-01")
      WHEN product = "DrugF" THEN DATE("2024-06-01")
      WHEN product = "DrugG" THEN DATE("2024-02-01")
      WHEN product = "DrugH" THEN DATE("2024-04-01")
      WHEN product = "DrugI" THEN DATE("2024-02-01")
      WHEN product = "DrugJ" THEN DATE("2024-02-02")
      WHEN product = "DrugK" THEN DATE("2024-01-01")
      WHEN product = "DrugL" THEN DATE("2024-03-01")
      WHEN product = "DrugM" THEN DATE("2024-04-01")
      WHEN product = "DrügN-测试" THEN DATE("2024-05-05")
      WHEN product = "💊DrugO" THEN DATE("2024-06-06")
      WHEN product = "DrugP" THEN DATE("2024-07-07")
      WHEN product = "DrugQ" THEN DATE("2024-08-08")
      WHEN product = "DrugR" THEN DATE("2024-09-09")
      WHEN product = "DrugS" THEN DATE("2024-10-10")
      WHEN product = "DrugT" THEN DATE("2024-11-11")
      WHEN product = "DrugU" THEN DATE("2024-12-12")
      WHEN product = "DrugV" THEN DATE("2024-02-01")
      WHEN product = "DrugW" THEN DATE("2024-12-01")
      WHEN product = "DrugX" THEN NULL
      WHEN product = "DrugY" THEN DATE("2024-06-15")
      WHEN product = "DrugZ" THEN DATE("2024-07-20")
      ELSE NULL
    END AS calctime
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, ship_date, days_since_last_fill, calctime,
  CASE
    WHEN ship_date IS NOT NULL AND calctime IS NOT NULL
      THEN DATEDIFF(calctime, ship_date) + 1
    ELSE NULL
  END AS expected_days_since_last_fill
FROM test_cases
WHERE
  (days_since_last_fill IS DISTINCT FROM
    CASE
      WHEN ship_date IS NOT NULL AND calctime IS NOT NULL
        THEN DATEDIFF(calctime, ship_date) + 1
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (days_since_last_fill matches logic)
*/

/*
  -- Test: expected_refill_date = DATE_ADD(calctime, days_until_next_ship)
*/
WITH test_cases AS (
  SELECT
    product, expected_refill_date, days_until_next_ship,
    CASE
      WHEN product = "DrugA" THEN DATE("2024-02-01")
      WHEN product = "DrugB" THEN DATE("2024-04-01")
      WHEN product = "DrugC" THEN DATE("2024-06-01")
      WHEN product = "DrugD" THEN DATE("2024-02-01")
      WHEN product = "DrugE" THEN DATE("2024-04-01")
      WHEN product = "DrugF" THEN DATE("2024-06-01")
      WHEN product = "DrugG" THEN DATE("2024-02-01")
      WHEN product = "DrugH" THEN DATE("2024-04-01")
      WHEN product = "DrugI" THEN DATE("2024-02-01")
      WHEN product = "DrugJ" THEN DATE("2024-02-02")
      WHEN product = "DrugK" THEN DATE("2024-01-01")
      WHEN product = "DrugL" THEN DATE("2024-03-01")
      WHEN product = "DrugM" THEN DATE("2024-04-01")
      WHEN product = "DrügN-测试" THEN DATE("2024-05-05")
      WHEN product = "💊DrugO" THEN DATE("2024-06-06")
      WHEN product = "DrugP" THEN DATE("2024-07-07")
      WHEN product = "DrugQ" THEN DATE("2024-08-08")
      WHEN product = "DrugR" THEN DATE("2024-09-09")
      WHEN product = "DrugS" THEN DATE("2024-10-10")
      WHEN product = "DrugT" THEN DATE("2024-11-11")
      WHEN product = "DrugU" THEN DATE("2024-12-12")
      WHEN product = "DrugV" THEN DATE("2024-02-01")
      WHEN product = "DrugW" THEN DATE("2024-12-01")
      WHEN product = "DrugX" THEN NULL
      WHEN product = "DrugY" THEN DATE("2024-06-15")
      WHEN product = "DrugZ" THEN DATE("2024-07-20")
      ELSE NULL
    END AS calctime
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, expected_refill_date, days_until_next_ship, calctime,
  CASE
    WHEN expected_refill_date IS NOT NULL AND days_until_next_ship IS NOT NULL AND calctime IS NOT NULL
      THEN DATE_ADD(calctime, days_until_next_ship)
    ELSE NULL
  END AS expected_expected_refill_date
FROM test_cases
WHERE
  (expected_refill_date IS DISTINCT FROM
    CASE
      WHEN expected_refill_date IS NOT NULL AND days_until_next_ship IS NOT NULL AND calctime IS NOT NULL
        THEN DATE_ADD(calctime, days_until_next_ship)
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (expected_refill_date matches logic)
*/

/*
  -- Test: prior_ship and days_between logic
  -- For each treatment_id, prior_ship is previous ship_date ordered by ship_date
  -- days_between = DATEDIFF(ship_date, prior_ship)
*/
WITH cte AS (
  SELECT
    treatment_id, ship_date, prior_ship, days_between,
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS expected_prior_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  treatment_id, ship_date, prior_ship, expected_prior_ship
FROM cte
WHERE
  (prior_ship IS DISTINCT FROM expected_prior_ship)
;
/*
  -- Assertion: Should return 0 rows (prior_ship matches window function)
*/

WITH cte AS (
  SELECT
    ship_date, prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  ship_date, prior_ship, days_between,
  CASE
    WHEN ship_date IS NOT NULL AND prior_ship IS NOT NULL
      THEN DATEDIFF(ship_date, prior_ship)
    ELSE NULL
  END AS expected_days_between
FROM cte
WHERE
  (days_between IS DISTINCT FROM
    CASE
      WHEN ship_date IS NOT NULL AND prior_ship IS NOT NULL
        THEN DATEDIFF(ship_date, prior_ship)
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (days_between matches logic)
*/

/*
  -- Test: days_since_supply_out
  -- CASE WHEN DATEDIFF(calctime, shipment_expiry) >= 0 THEN DATEDIFF(calctime, shipment_expiry) ELSE NULL
*/
WITH test_cases AS (
  SELECT
    product, shipment_expiry, days_since_supply_out,
    CASE
      WHEN product = "DrugA" THEN DATE("2024-02-01")
      WHEN product = "DrugB" THEN DATE("2024-04-01")
      WHEN product = "DrugC" THEN DATE("2024-06-01")
      WHEN product = "DrugD" THEN DATE("2024-02-01")
      WHEN product = "DrugE" THEN DATE("2024-04-01")
      WHEN product = "DrugF" THEN DATE("2024-06-01")
      WHEN product = "DrugG" THEN DATE("2024-02-01")
      WHEN product = "DrugH" THEN DATE("2024-04-01")
      WHEN product = "DrugI" THEN DATE("2024-02-01")
      WHEN product = "DrugJ" THEN DATE("2024-02-02")
      WHEN product = "DrugK" THEN DATE("2024-01-01")
      WHEN product = "DrugL" THEN DATE("2024-03-01")
      WHEN product = "DrugM" THEN DATE("2024-04-01")
      WHEN product = "DrügN-测试" THEN DATE("2024-05-05")
      WHEN product = "💊DrugO" THEN DATE("2024-06-06")
      WHEN product = "DrugP" THEN DATE("2024-07-07")
      WHEN product = "DrugQ" THEN DATE("2024-08-08")
      WHEN product = "DrugR" THEN DATE("2024-09-09")
      WHEN product = "DrugS" THEN DATE("2024-10-10")
      WHEN product = "DrugT" THEN DATE("2024-11-11")
      WHEN product = "DrugU" THEN DATE("2024-12-12")
      WHEN product = "DrugV" THEN DATE("2024-02-01")
      WHEN product = "DrugW" THEN DATE("2024-12-01")
      WHEN product = "DrugX" THEN NULL
      WHEN product = "DrugY" THEN DATE("2024-06-15")
      WHEN product = "DrugZ" THEN DATE("2024-07-20")
      ELSE NULL
    END AS calctime
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, shipment_expiry, days_since_supply_out, calctime,
  CASE
    WHEN shipment_expiry IS NOT NULL AND calctime IS NOT NULL AND DATEDIFF(calctime, shipment_expiry) >= 0
      THEN DATEDIFF(calctime, shipment_expiry)
    ELSE NULL
  END AS expected_days_since_supply_out
FROM test_cases
WHERE
  (days_since_supply_out IS DISTINCT FROM
    CASE
      WHEN shipment_expiry IS NOT NULL AND calctime IS NOT NULL AND DATEDIFF(calctime, shipment_expiry) >= 0
        THEN DATEDIFF(calctime, shipment_expiry)
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (days_since_supply_out matches logic)
*/

/*
  -- Test: age = DATEDIFF(YEAR, dob, calctime)
*/
WITH test_cases AS (
  SELECT
    product, dob, age,
    CASE
      WHEN product = "DrugA" THEN DATE("2024-02-01")
      WHEN product = "DrugB" THEN DATE("2024-04-01")
      WHEN product = "DrugC" THEN DATE("2024-06-01")
      WHEN product = "DrugD" THEN DATE("2024-02-01")
      WHEN product = "DrugE" THEN DATE("2024-04-01")
      WHEN product = "DrugF" THEN DATE("2024-06-01")
      WHEN product = "DrugG" THEN DATE("2024-02-01")
      WHEN product = "DrugH" THEN DATE("2024-04-01")
      WHEN product = "DrugI" THEN DATE("2024-02-01")
      WHEN product = "DrugJ" THEN DATE("2024-02-02")
      WHEN product = "DrugK" THEN DATE("2024-01-01")
      WHEN product = "DrugL" THEN DATE("2024-03-01")
      WHEN product = "DrugM" THEN DATE("2024-04-01")
      WHEN product = "DrügN-测试" THEN DATE("2024-05-05")
      WHEN product = "💊DrugO" THEN DATE("2024-06-06")
      WHEN product = "DrugP" THEN DATE("2024-07-07")
      WHEN product = "DrugQ" THEN DATE("2024-08-08")
      WHEN product = "DrugR" THEN DATE("2024-09-09")
      WHEN product = "DrugS" THEN DATE("2024-10-10")
      WHEN product = "DrugT" THEN DATE("2024-11-11")
      WHEN product = "DrugU" THEN DATE("2024-12-12")
      WHEN product = "DrugV" THEN DATE("2024-02-01")
      WHEN product = "DrugW" THEN DATE("2024-12-01")
      WHEN product = "DrugX" THEN NULL
      WHEN product = "DrugY" THEN DATE("2024-06-15")
      WHEN product = "DrugZ" THEN DATE("2024-07-20")
      ELSE NULL
    END AS calctime
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, dob, age, calctime,
  CASE
    WHEN dob IS NOT NULL AND calctime IS NOT NULL
      THEN YEAR(calctime) - YEAR(dob)
    ELSE NULL
  END AS expected_age
FROM test_cases
WHERE
  (age IS DISTINCT FROM
    CASE
      WHEN dob IS NOT NULL AND calctime IS NOT NULL
        THEN YEAR(calctime) - YEAR(dob)
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (age matches logic)
*/

/*
  -- Test: age_at_first_ship = ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
*/
WITH cte AS (
  SELECT
    dob, first_ship_date, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  dob, first_ship_date, age_at_first_ship,
  CASE
    WHEN dob IS NOT NULL AND first_ship_date IS NOT NULL
      THEN ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    ELSE NULL
  END AS expected_age_at_first_ship
FROM cte
WHERE
  (age_at_first_ship IS DISTINCT FROM
    CASE
      WHEN dob IS NOT NULL AND first_ship_date IS NOT NULL
        THEN ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
      ELSE NULL
    END
  )
;
/*
  -- Assertion: Should return 0 rows (age_at_first_ship matches logic)
*/

/*
  -- Test: latest_therapy_ships = count(ship_date) over (partition by patient_id, treatment_id where ship_type='commercial')
*/
WITH cte AS (
  SELECT
    patient_id, treatment_id, ship_type, latest_therapy_ships,
    COUNT(CASE WHEN ship_type = "commercial" THEN 1 END) OVER (PARTITION BY patient_id, treatment_id) AS expected_latest_therapy_ships
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  patient_id, treatment_id, ship_type, latest_therapy_ships, expected_latest_therapy_ships
FROM cte
WHERE
  (latest_therapy_ships IS DISTINCT FROM expected_latest_therapy_ships)
;
/*
  -- Assertion: Should return 0 rows (latest_therapy_ships matches logic)
*/

/*
  -- Test: discontinuation_type logic
  -- CASE WHEN refill_status = 'DC - Standard' THEN 'STANDARD' WHEN refill_status= 'DC-PERMANENT' THEN 'PERMANENT' ELSE NULL
*/
WITH cte AS (
  SELECT
    refill_status, discontinuation_type,
    CASE
      WHEN refill_status = "DC - Standard" THEN "STANDARD"
      WHEN refill_status = "DC-PERMANENT" THEN "PERMANENT"
      ELSE NULL
    END AS expected_discontinuation_type
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  refill_status, discontinuation_type, expected_discontinuation_type
FROM cte
WHERE
  (discontinuation_type IS DISTINCT FROM expected_discontinuation_type)
;
/*
  -- Assertion: Should return 0 rows (discontinuation_type matches logic)
*/

/* -------------------- NULL & ERROR HANDLING TESTS -------------------- */

/*
  -- Test: If days_supply and qty are both NULL or non-numeric, all dependent derived fields are NULL
*/
WITH cte AS (
  SELECT
    product, days_supply, qty,
    TRY_CAST(days_supply AS INT) AS ds_int,
    TRY_CAST(qty AS INT) AS qty_int,
    shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, days_since_supply_out
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, days_supply, qty, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, days_since_supply_out
FROM cte
WHERE
  (ds_int IS NULL AND qty_int IS NULL)
  AND (
    shipment_expiry IS NOT NULL
    OR discontinuation_date IS NOT NULL
    OR days_until_next_ship IS NOT NULL
    OR expected_refill_date IS NOT NULL
    OR days_since_supply_out IS NOT NULL
  )
;
/*
  -- Assertion: Should return 0 rows (all dependent fields are NULL if both source fields are NULL/non-numeric)
*/

/*
  -- Test: If dob or first_ship_date is NULL, age and age_at_first_ship are NULL
*/
WITH cte AS (
  SELECT
    dob, first_ship_date, age, age_at_first_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  dob, first_ship_date, age, age_at_first_ship
FROM cte
WHERE
  (dob IS NULL OR first_ship_date IS NULL)
  AND (age IS NOT NULL OR age_at_first_ship IS NOT NULL)
;
/*
  -- Assertion: Should return 0 rows (age and age_at_first_ship are NULL if dob or first_ship_date is NULL)
*/

/*
  -- Test: If prior_ship is NULL, days_between is NULL
*/
WITH cte AS (
  SELECT
    prior_ship, days_between
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  prior_ship, days_between
FROM cte
WHERE
  prior_ship IS NULL AND days_between IS NOT NULL
;
/*
  -- Assertion: Should return 0 rows (days_between is NULL if prior_ship is NULL)
*/

/*
  -- Test: Error handling for non-numeric days_supply or qty
  -- If non-numeric, all dependent derived fields are NULL
*/
WITH cte AS (
  SELECT
    product, days_supply, qty,
    TRY_CAST(days_supply AS INT) AS ds_int,
    TRY_CAST(qty AS INT) AS qty_int,
    shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, days_since_supply_out
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT
  product, days_supply, qty, shipment_expiry, discontinuation_date, days_until_next_ship, expected_refill_date, days_since_supply_out
FROM cte
WHERE
  (
    (days_supply IS NOT NULL AND ds_int IS NULL)
    OR (qty IS NOT NULL AND qty_int IS NULL)
  )
  AND (
    shipment_expiry IS NOT NULL
    OR discontinuation_date IS NOT NULL
    OR days_until_next_ship IS NOT NULL
    OR expected_refill_date IS NOT NULL
    OR days_since_supply_out IS NOT NULL
  )
;
/*
  -- Assertion: Should return 0 rows (all dependent fields are NULL if non-numeric)
*/

/* -------------------- DATA QUALITY & INTEGRATION TESTS -------------------- */

/*
  -- Test: All directly sourced columns are copied as-is (compare with source if available)
  -- Not possible here as only enriched table is present, but can check for NULL preservation
*/

/*
  -- Test: NULLs in non-derived columns are preserved
*/
SELECT *
FROM purgo_playground.enriched_patient_therapy_shipment
WHERE product = "DrugX"
  AND ship_date IS NULL
  AND days_supply IS NULL
  AND qty IS NULL
  AND treatment_id IS NULL
  AND dob IS NULL
  AND first_ship_date IS NULL
  AND refill_status IS NULL
  AND ship_type IS NULL
  AND shipment_arrived_status IS NULL
  AND delivery_ontime IS NULL
;
/*
  -- Assertion: Should return 1 row (NULLs preserved)
*/

/* -------------------- DELTA LAKE & PERFORMANCE TESTS -------------------- */

/*
  -- Test: Table is Delta and supports UPDATE, DELETE, MERGE
*/
DESCRIBE DETAIL purgo_playground.enriched_patient_therapy_shipment
;
/*
  -- Assertion: format should be 'delta'
*/

/*
  -- Test: UPDATE operation
*/
UPDATE purgo_playground.enriched_patient_therapy_shipment
SET delivery_ontime = "test_update"
WHERE product = "DrugA"
;
/*
  -- Assertion: delivery_ontime for DrugA is now "test_update"
*/
SELECT delivery_ontime FROM purgo_playground.enriched_patient_therapy_shipment WHERE product = "DrugA";
/*
  -- Assertion: Should return "test_update"
*/

/*
  -- Test: DELETE operation
*/
DELETE FROM purgo_playground.enriched_patient_therapy_shipment WHERE product = "DrugZ";
/*
  -- Assertion: DrugZ no longer exists
*/
SELECT * FROM purgo_playground.enriched_patient_therapy_shipment WHERE product = "DrugZ";
/*
  -- Assertion: Should return 0 rows
*/

/*
  -- Test: MERGE operation (upsert)
*/
MERGE INTO purgo_playground.enriched_patient_therapy_shipment AS target
USING (SELECT "DrugA" AS product, "yes" AS delivery_ontime) AS source
ON target.product = source.product
WHEN MATCHED THEN UPDATE SET target.delivery_ontime = source.delivery_ontime
WHEN NOT MATCHED THEN INSERT (product, delivery_ontime) VALUES (source.product, source.delivery_ontime)
;
/*
  -- Assertion: delivery_ontime for DrugA is "yes"
*/
SELECT delivery_ontime FROM purgo_playground.enriched_patient_therapy_shipment WHERE product = "DrugA";
/*
  -- Assertion: Should return "yes"
*/

/* -------------------- WINDOW FUNCTION TESTS -------------------- */

/*
  -- Test: Window function for prior_ship and latest_therapy_ships
*/
WITH cte AS (
  SELECT
    treatment_id, ship_date, prior_ship,
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS expected_prior_ship
  FROM purgo_playground.enriched_patient_therapy_shipment
)
SELECT * FROM cte WHERE prior_ship IS DISTINCT FROM expected_prior_ship;
/*
  -- Assertion: Should return 0 rows
*/

/* -------------------- CLEANUP -------------------- */

/*
  -- Clean up test updates and deletes
*/
UPDATE purgo_playground.enriched_patient_therapy_shipment
SET delivery_ontime = "yes"
WHERE product = "DrugA"
;

-- No DROP TABLE or temp view cleanup needed as per requirements

/* -------------------- END OF TEST SUITE -------------------- */

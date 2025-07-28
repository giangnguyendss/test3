USE CATALOG purgo_databricks;

/* 
==========================================================================================
  Databricks SQL: Enrich purgo_playground.patient_therapy_shipment with derived metrics
  Target Table: purgo_playground.enriched_patient_therapy_shipment
  Author: Databricks SQL Automation
  All logic, error handling, and documentation are included per requirements.
==========================================================================================
*/

/*
==========================================================================================
  SECTION: ENRICHMENT LOGIC - Insert Derived Data into Output Table
==========================================================================================
*/

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
    /* Derived: shipment_expiry
       - If days_supply is numeric, use it
       - Else if days_supply is NULL and qty is numeric, use (qty / 3) * 7
       - Else NULL
    */
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST(p.days_supply AS INT))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(p.ship_date, CAST((CAST(p.qty AS DOUBLE) / 3 * 7) AS INT))
      ELSE NULL
    END AS shipment_expiry,
    /* Derived: discontinuation_date
       - shipment_expiry + 91 days
    */
    CASE
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), 91)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(DATE_ADD(p.ship_date, CAST((CAST(p.qty AS DOUBLE) / 3 * 7) AS INT)), 91)
      ELSE NULL
    END AS discontinuation_date,
    /* Derived: days_until_next_ship
       - (DATEDIFF(shipment_expiry, calctime) + 1)
    */
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1)
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN (DATEDIFF(DATE_ADD(p.ship_date, CAST((CAST(p.qty AS DOUBLE) / 3 * 7) AS INT)), CAST(p.calctime AS DATE)) + 1)
      ELSE NULL
    END AS days_until_next_ship,
    /* Derived: days_since_last_fill
       - (DATEDIFF(calctime, ship_date) + 1)
    */
    CASE
      WHEN p.calctime IS NULL OR p.ship_date IS NULL THEN NULL
      ELSE (DATEDIFF(CAST(p.calctime AS DATE), p.ship_date) + 1)
    END AS days_since_last_fill,
    /* Derived: expected_refill_date
       - DATE_ADD(calctime, days_until_next_ship)
    */
    CASE
      WHEN p.calctime IS NULL THEN NULL
      WHEN p.ship_date IS NULL THEN NULL
      WHEN TRY_CAST(p.days_supply AS INT) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST(p.days_supply AS INT)), CAST(p.calctime AS DATE)) + 1))
      WHEN p.days_supply IS NULL AND TRY_CAST(p.qty AS DOUBLE) IS NOT NULL THEN DATE_ADD(CAST(p.calctime AS DATE), (DATEDIFF(DATE_ADD(p.ship_date, CAST((CAST(p.qty AS DOUBLE) / 3 * 7) AS INT)), CAST(p.calctime AS DATE)) + 1))
      ELSE NULL
    END AS expected_refill_date,
    p.calctime
  FROM purgo_playground.patient_therapy_shipment p
),
windowed AS (
  SELECT
    *,
    /* Derived: prior_ship
       - LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date)
    */
    LAG(ship_date) OVER (PARTITION BY treatment_id ORDER BY ship_date) AS prior_ship
  FROM base
),
windowed2 AS (
  SELECT
    *,
    /* Derived: days_between
       - DATEDIFF(ship_date, prior_ship)
    */
    CASE
      WHEN prior_ship IS NULL OR ship_date IS NULL THEN NULL
      ELSE DATEDIFF(ship_date, prior_ship)
    END AS days_between
  FROM windowed
),
windowed3 AS (
  SELECT
    *,
    /* Derived: days_since_supply_out
       - If calctime or ship_date is NULL, then NULL
       - If days_supply is numeric, DATEDIFF(calctime, shipment_expiry) if >=0 else NULL
       - Else if days_supply is NULL and qty is numeric, DATEDIFF(calctime, shipment_expiry) if >=0 else NULL
       - Else NULL
    */
    CASE
      WHEN calctime IS NULL OR ship_date IS NULL THEN NULL
      WHEN TRY_CAST(days_supply AS INT) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST(days_supply AS INT)))
          ELSE NULL
        END
      WHEN days_supply IS NULL AND TRY_CAST(qty AS DOUBLE) IS NOT NULL THEN
        CASE WHEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3 * 7) AS INT))) >= 0
          THEN DATEDIFF(CAST(calctime AS DATE), DATE_ADD(ship_date, CAST((CAST(qty AS DOUBLE) / 3 * 7) AS INT)))
          ELSE NULL
        END
      ELSE NULL
    END AS days_since_supply_out
  FROM windowed2
),
windowed4 AS (
  SELECT
    *,
    /* Derived: age
       - Integer years between dob and calctime
    */
    CASE
      WHEN dob IS NULL OR calctime IS NULL THEN NULL
      ELSE YEAR(calctime) - YEAR(dob) - CASE WHEN (MONTH(calctime) < MONTH(dob)) OR (MONTH(calctime) = MONTH(dob) AND DAY(calctime) < DAY(dob)) THEN 1 ELSE 0 END
    END AS age,
    /* Derived: age_at_first_ship
       - Rounded integer of (DATEDIFF(DAY, dob, first_ship_date) / 365.0)
    */
    CASE
      WHEN dob IS NULL OR first_ship_date IS NULL THEN NULL
      ELSE ROUND(DATEDIFF(first_ship_date, dob) / 365.0, 0)
    END AS age_at_first_ship
  FROM windowed3
),
latest_ships AS (
  /* Derived: latest_therapy_ships
     - Count of records with ship_type = 'commercial' (case-insensitive) for patient_id, treatment_id
  */
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
    /* Derived: discontinuation_type
       - 'STANDARD' if refill_status = 'DC - Standard'
       - 'PERMANENT' if refill_status = 'DC-PERMANENT'
       - Else NULL
    */
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

SELECT * FROM purgo_playground.enriched_patient_therapy_shipment
ORDER BY product, ship_date
;

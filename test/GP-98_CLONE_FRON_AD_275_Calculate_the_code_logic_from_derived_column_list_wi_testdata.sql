-- Test Data Generation for purgo_playground.patient_therapy_shipment
-- Covers: happy path, edge, error, NULL, special/multibyte chars, data type alignment

WITH test_data AS (
  SELECT
    -- Happy path: all fields populated, valid numeric days_supply/qty, standard scenario
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
    -- Happy path: days_supply NULL, qty fallback logic
    SELECT
    'DrugB',
    DATE'2024-02-10',
    NULL,
    90,
    'TREAT456',
    DATE'1975-03-10',
    DATE'2024-02-10',
    'DC-PERMANENT',
    'PAT002',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-02-20T00:00:00.000+0000'
  UNION ALL
    -- Happy path: all fields, refill_status NULL
    SELECT
    'DrugC',
    DATE'2024-03-01',
    '14',
    42,
    'TREAT789',
    DATE'2000-01-01',
    DATE'2024-03-01',
    NULL,
    'PAT003',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Error: days_supply and qty both NULL
    SELECT
    'DrugD',
    DATE'2024-01-01',
    NULL,
    NULL,
    'TREAT000',
    DATE'1990-01-01',
    DATE'2024-01-01',
    'DC - Standard',
    'PAT004',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: days_supply non-numeric (invalid)
    SELECT
    'DrugE',
    DATE'2024-01-01',
    'abc',
    30,
    'TREAT001',
    DATE'1985-05-05',
    DATE'2024-01-01',
    'DC - Standard',
    'PAT005',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: qty non-numeric (invalid, will be cast to NULL)
    SELECT
    'DrugF',
    DATE'2024-01-01',
    NULL,
    NULL,
    'TREAT002',
    DATE'1985-05-05',
    DATE'2024-01-01',
    'DC - Standard',
    'PAT006',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-01-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_type sample, should not count for latest_therapy_ships
    SELECT
    'DrugA',
    DATE'2024-01-15',
    '21',
    63,
    'TREAT123',
    DATE'1980-06-15',
    DATE'2023-12-01',
    'DC - Standard',
    'PAT001',
    'sample',
    'arrived',
    'yes',
    TIMESTAMP'2024-01-20T00:00:00.000+0000'
  UNION ALL
    -- Edge: multiple shipments for same treatment_id (prior_ship/days_between)
    SELECT
    'DrugA',
    DATE'2024-01-22',
    '21',
    63,
    'TREAT123',
    DATE'1980-06-15',
    DATE'2023-12-01',
    'DC - Standard',
    'PAT001',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-01-25T00:00:00.000+0000'
  UNION ALL
    -- Edge: multiple shipments for same treatment_id (prior_ship/days_between)
    SELECT
    'DrugA',
    DATE'2024-02-12',
    '21',
    63,
    'TREAT123',
    DATE'1980-06-15',
    DATE'2023-12-01',
    'DC - Standard',
    'PAT001',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-02-15T00:00:00.000+0000'
  UNION ALL
    -- Edge: refill_status = Other (should yield NULL discontinuation_type)
    SELECT
    'DrugG',
    DATE'2024-03-01',
    '30',
    90,
    'TREAT999',
    DATE'1970-12-31',
    DATE'2024-03-01',
    'Other',
    'PAT007',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: refill_status NULL
    SELECT
    'DrugH',
    DATE'2024-03-01',
    '30',
    90,
    'TREAT998',
    DATE'1970-12-31',
    DATE'2024-03-01',
    NULL,
    'PAT008',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_date NULL
    SELECT
    'DrugI',
    NULL,
    '30',
    90,
    'TREAT997',
    DATE'1970-12-31',
    DATE'2024-03-01',
    'DC - Standard',
    'PAT009',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: dob NULL
    SELECT
    'DrugJ',
    DATE'2024-03-01',
    '30',
    90,
    'TREAT996',
    NULL,
    DATE'2024-03-01',
    'DC - Standard',
    'PAT010',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: first_ship_date NULL
    SELECT
    'DrugK',
    DATE'2024-03-01',
    '30',
    90,
    'TREAT995',
    DATE'1970-12-31',
    NULL,
    'DC - Standard',
    'PAT011',
    'commercial',
    'arrived',
    'no',
    TIMESTAMP'2024-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: calctime NULL
    SELECT
    'DrugL',
    DATE'2024-03-01',
    '30',
    90,
    'TREAT994',
    DATE'1970-12-31',
    DATE'2024-03-01',
    'DC - Standard',
    'PAT012',
    'commercial',
    'arrived',
    'no',
    NULL
  UNION ALL
    -- Special: special characters in product, patient_id, etc.
    SELECT
    'DrügΩ',
    DATE'2024-04-01',
    '28',
    84,
    'TREATΩ',
    DATE'1995-07-07',
    DATE'2024-04-01',
    'DC - Standard',
    'PATΩΩ',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-04-10T00:00:00.000+0000'
  UNION ALL
    -- Special: multi-byte unicode in product
    SELECT
    '药品A',
    DATE'2024-05-01',
    '30',
    90,
    'TREAT汉字',
    DATE'1990-01-01',
    DATE'2024-05-01',
    'DC-PERMANENT',
    'PAT汉字',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-05-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: delivery_ontime NULL
    SELECT
    'DrugM',
    DATE'2024-06-01',
    '30',
    90,
    'TREAT993',
    DATE'1970-12-31',
    DATE'2024-06-01',
    'DC - Standard',
    'PAT013',
    'commercial',
    'arrived',
    NULL,
    TIMESTAMP'2024-06-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: shipment_arrived_status NULL
    SELECT
    'DrugN',
    DATE'2024-07-01',
    '30',
    90,
    'TREAT992',
    DATE'1970-12-31',
    DATE'2024-07-01',
    'DC - Standard',
    'PAT014',
    'commercial',
    NULL,
    'yes',
    TIMESTAMP'2024-07-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_type NULL
    SELECT
    'DrugO',
    DATE'2024-08-01',
    '30',
    90,
    'TREAT991',
    DATE'1970-12-31',
    DATE'2024-08-01',
    'DC - Standard',
    'PAT015',
    NULL,
    'arrived',
    'yes',
    TIMESTAMP'2024-08-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: refill_status with lower case (should not match discontinuation_type)
    SELECT
    'DrugP',
    DATE'2024-09-01',
    '30',
    90,
    'TREAT990',
    DATE'1970-12-31',
    DATE'2024-09-01',
    'dc - standard',
    'PAT016',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-09-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: days_supply = 0
    SELECT
    'DrugQ',
    DATE'2024-10-01',
    '0',
    0,
    'TREAT989',
    DATE'1970-12-31',
    DATE'2024-10-01',
    'DC - Standard',
    'PAT017',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-10-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: qty = 0, days_supply NULL
    SELECT
    'DrugR',
    DATE'2024-11-01',
    NULL,
    0,
    'TREAT988',
    DATE'1970-12-31',
    DATE'2024-11-01',
    'DC - Standard',
    'PAT018',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-11-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: days_supply negative
    SELECT
    'DrugS',
    DATE'2024-12-01',
    '-7',
    21,
    'TREAT987',
    DATE'1970-12-31',
    DATE'2024-12-01',
    'DC - Standard',
    'PAT019',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2024-12-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: qty negative, days_supply NULL
    SELECT
    'DrugT',
    DATE'2025-01-01',
    NULL,
    -21,
    'TREAT986',
    DATE'1970-12-31',
    DATE'2025-01-01',
    'DC - Standard',
    'PAT020',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2025-01-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: all fields NULL
    SELECT
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
  UNION ALL
    -- Edge: special characters in refill_status
    SELECT
    'DrugU',
    DATE'2025-02-01',
    '30',
    90,
    'TREAT985',
    DATE'1970-12-31',
    DATE'2025-02-01',
    'DC - 特殊',
    'PAT021',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2025-02-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: special characters in delivery_ontime
    SELECT
    'DrugV',
    DATE'2025-03-01',
    '30',
    90,
    'TREAT984',
    DATE'1970-12-31',
    DATE'2025-03-01',
    'DC - Standard',
    'PAT022',
    'commercial',
    'arrived',
    '是',
    TIMESTAMP'2025-03-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: special characters in shipment_arrived_status
    SELECT
    'DrugW',
    DATE'2025-04-01',
    '30',
    90,
    'TREAT983',
    DATE'1970-12-31',
    DATE'2025-04-01',
    'DC - Standard',
    'PAT023',
    'commercial',
    '到达',
    'yes',
    TIMESTAMP'2025-04-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_type = 'Commercial' (case sensitivity)
    SELECT
    'DrugX',
    DATE'2025-05-01',
    '30',
    90,
    'TREAT982',
    DATE'1970-12-31',
    DATE'2025-05-01',
    'DC - Standard',
    'PAT024',
    'Commercial',
    'arrived',
    'yes',
    TIMESTAMP'2025-05-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_type = 'COMMERCIAL' (case sensitivity)
    SELECT
    'DrugY',
    DATE'2025-06-01',
    '30',
    90,
    'TREAT981',
    DATE'1970-12-31',
    DATE'2025-06-01',
    'DC - Standard',
    'PAT025',
    'COMMERCIAL',
    'arrived',
    'yes',
    TIMESTAMP'2025-06-10T00:00:00.000+0000'
  UNION ALL
    -- Edge: ship_type = 'commercial' (lowercase, should count for latest_therapy_ships)
    SELECT
    'DrugZ',
    DATE'2025-07-01',
    '30',
    90,
    'TREAT980',
    DATE'1970-12-31',
    DATE'2025-07-01',
    'DC - Standard',
    'PAT026',
    'commercial',
    'arrived',
    'yes',
    TIMESTAMP'2025-07-10T00:00:00.000+0000'
)

SELECT * FROM test_data
;

-- Databricks SQL script
-- Purpose: Create and alter the 'Account' table based on provided specifications
-- Author: Giang Nguyen
-- Date: 2025-07-21
-- Description: This script creates the 'Account' table if it does not exist and adds new columns from an external specification. It also drops the table if it already exists to ensure schema consistency.

-- Drop 'Account' table if already exists
DROP TABLE IF EXISTS purgo_playground.Account;

-- Create 'Account' table with initial columns from patient_reg.xlsx
CREATE TABLE purgo_playground.Account (
    Organization_Corporate_Parent__c STRING,
    Organization_Level__c STRING,
    Address__c STRING,
    Health_Industry_Number__c STRING,
    Class_of_Trade_Facility_Type__c STRING,
    Classification_type__c STRING,
    Classification_sub_type__c STRING,
    Contracted_340B__c BOOLEAN
);

-- Alter 'Account' table to add new columns from patient_addition_field.xlsx
ALTER TABLE purgo_playground.Account 
ADD COLUMNS (
    Operational_Status STRING,
    Provider_Network_Type STRING,
    Regulatory_Compliance_Code STRING,
    Tax_Identification_Number STRING
);

-- Validate the structure of the 'Account' table after alterations
WITH validation_cte AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'Account'
)
SELECT * FROM validation_cte;

/* 
  -- Ensure no schema mismatches or errors.
  -- Validate inclusion of new columns along with their data types.
*/

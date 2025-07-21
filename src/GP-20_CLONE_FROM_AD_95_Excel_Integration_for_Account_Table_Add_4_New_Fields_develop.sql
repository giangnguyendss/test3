-- Databricks SQL script
-- Purpose: Create and alter the 'Account' table based on provided specifications
-- Author: Giang Nguyen
-- Date: 2025-07-21
-- Description: This script creates the 'Account' table if it does not exist and adds new columns from an external specification. It also drops the table if it already exists to ensure schema consistency.

-- Drop the 'Account' table if it exists to prevent conflicts from previous runs
DROP TABLE IF EXISTS purgo_playground.Account;

-- Create the 'Account' table with initial columns from patient_reg.xlsx
CREATE TABLE purgo_playground.Account (
    Organization_Corporate_Parent__c STRING COMMENT 'Organization Corporate Parent',
    Organization_Level__c STRING COMMENT 'Organization Level',
    Address__c STRING COMMENT 'Address',
    Health_Industry_Number__c STRING COMMENT 'Health Industry Number',
    Class_of_Trade_Facility_Type__c STRING COMMENT 'Class of Trade Facility Type',
    Classification_type__c STRING COMMENT 'Classification Type',
    Classification_sub_type__c STRING COMMENT 'Classification Sub Type',
    Contracted_340B__c BOOLEAN COMMENT 'Contracted 340B Status'
);

-- Alter the 'Account' table to add new columns specified in patient_addition_field.xlsx
ALTER TABLE purgo_playground.Account 
ADD COLUMNS (
    Operational_Status STRING COMMENT 'Operational Status',
    Provider_Network_Type STRING COMMENT 'Valid values: Network1, Network2',
    Regulatory_Compliance_Code STRING COMMENT 'Regulatory Compliance Code',
    Tax_Identification_Number STRING COMMENT 'Tax Identification Number'
);

-- Validate the structure of the 'Account' table after adding new columns
-- Use a Common Table Expression (CTE) to fetch table column details
WITH validation_cte AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'Account' AND table_schema = 'purgo_playground'
)
SELECT * FROM validation_cte;

/* 
  -- Validate the successful addition of new columns.
  -- Ensures the correct data types for all existing and new columns.
  -- The CTE fetches column information for verification purposes.
*/

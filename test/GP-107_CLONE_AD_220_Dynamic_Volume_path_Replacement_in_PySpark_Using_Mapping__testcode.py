%pip install openpyxl

# ----------------------------------------------------------------------------------------
# Databricks PySpark Test Suite for Dynamic Volume Path Replacement in PySpark Script
# ----------------------------------------------------------------------------------------
# This test suite validates the logic that updates only the volume paths in pyspark_script.py
# using the mapping from volume_mapping_sheet.xlsx, as per the provided requirements.
# It covers:
#   - File existence and error handling
#   - Mapping sheet validation (columns, duplicates, empties, whitespace, special chars, etc.)
#   - Path replacement logic (case sensitivity, substrings, multiple occurrences, etc.)
#   - Output script integrity and Python syntax validation
#   - Data type and schema validation for mapping
#   - Edge cases (relative paths, environment variables, unicode, etc.)
#   - Cleanup operations
#   - Performance (large mapping sheet)
#   - Databricks best practices
# ----------------------------------------------------------------------------------------

# --- SETUP & IMPORTS ---
from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType  
import os  
import sys  
import shutil  
import tempfile  
import pandas as pd  
import openpyxl  
import re  
import ast  

# --- TEST CONFIGURATION ---
# Block comment: Test file and mapping locations are set to temp directories for isolation
TEST_DIR = tempfile.mkdtemp()
SCRIPT_PATH = os.path.join(TEST_DIR, "pyspark_script.py")
MAPPING_SHEET_PATH = os.path.join(TEST_DIR, "volume_mapping_sheet.xlsx")
MAPPING_SHEET_NAME = "Sheet1"

# --- TEST UTILITIES ---

def write_file(path, content):
    # Write content to file, create parent dirs if needed
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def read_file(path):
    # Read content from file
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_mapping_excel(path, data, columns=None):
    # Write mapping data to Excel file
    df = pd.DataFrame(data, columns=columns)
    df.to_excel(path, index=False, sheet_name=MAPPING_SHEET_NAME, engine="openpyxl")

def remove_file(path):
    # Remove file if exists
    try:
        os.remove(path)
    except Exception:
        pass

def cleanup_test_dir():
    # Remove test directory and all contents
    try:
        shutil.rmtree(TEST_DIR)
    except Exception:
        pass

def is_valid_python_code(code):
    # Validate Python syntax
    try:
        ast.parse(code)
        return True
    except Exception:
        return False

def run_update_logic(script_path, mapping_sheet_path, mapping_sheet_name):
    # Inline the update logic from the provided implementation
    # Returns (success, error_message)
    try:
        # Load mapping
        if not os.path.exists(mapping_sheet_path):
            return False, "Mapping sheet not found at specified location"
        try:
            df = pd.read_excel(mapping_sheet_path, sheet_name=mapping_sheet_name, engine="openpyxl")
        except Exception as e:
            return False, f"Failed to read mapping sheet: {e}"
        required_cols = {"old_volume_path", "new_volume_path"}
        if not required_cols.issubset(set(df.columns.str.strip())):
            return False, "Mapping sheet must contain columns: old_volume_path, new_volume_path"
        df["old_volume_path"] = df["old_volume_path"].astype(str).str.strip()
        df["new_volume_path"] = df["new_volume_path"].astype(str).str.strip()
        if df["old_volume_path"].eq("").any():
            return False, "old_volume_path cannot be empty in mapping sheet"
        empty_new = df[df["new_volume_path"].eq("")]
        if not empty_new.empty:
            return False, f"new_volume_path cannot be empty for old_volume_path: {empty_new.iloc[0]['old_volume_path']}"
        if df["old_volume_path"].duplicated().any():
            return False, "Duplicate old_volume_path entries found in mapping sheet"
        mapping = dict(zip(df["old_volume_path"], df["new_volume_path"]))
        # Update script
        if not os.path.exists(script_path):
            return False, "pyspark_script.py not found at specified location"
        try:
            with open(script_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as e:
            return False, f"Failed to read script: {e}"
        for old_path, new_path in mapping.items():
            content = content.replace(old_path, new_path)
        try:
            with open(script_path, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            return False, f"Failed to write updated script: {e}"
        return True, ""
    except Exception as e:
        return False, str(e)

# --- TEST DATA ---

# Block comment: Standard script content with three volume paths
ORIGINAL_SCRIPT = """
from pyspark.sql.functions import col, avg, countDistinct

# Read CSV Files
biomarker_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv")
patient_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/patient_demographics.csv")
site_df = spark.read.option("header", True).csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/site_details.csv")

# Data Cleaning: Handling Missing Values (Fill NaN with Mean)
biomarker_df = biomarker_df.fillna({
    "Biomarker_A (mg/dL)": biomarker_df.select(avg("Biomarker_A (mg/dL)")).collect()[0][0],
    "Biomarker_B (ng/mL)": biomarker_df.select(avg("Biomarker_B (ng/mL)")).collect()[0][0],
    "Biomarker_C (pg/mL)": biomarker_df.select(avg("Biomarker_C (pg/mL)")).collect()[0][0]
})

# Count Distinct Patients
distinct_patient_count = patient_df.select(countDistinct("Patient_ID").alias("Unique_Patients"))
display(distinct_patient_count)

# Compute Average Biomarker Levels Per Patient
biomarker_avg_df = biomarker_df.groupBy("Patient_ID").agg(
    avg("Biomarker_A (mg/dL)").alias("Avg_Biomarker_A"),
    avg("Biomarker_B (ng/mL)").alias("Avg_Biomarker_B"),
    avg("Biomarker_C (pg/mL)").alias("Avg_Biomarker_C")
)
display(biomarker_avg_df)

# Join Biomarker Data with Patient Demographics
patient_biomarker_df = biomarker_avg_df.join(patient_df, "Patient_ID", "left")

# Join with Site Details
final_df = patient_biomarker_df.join(biomarker_df.select("Patient_ID", "Site"), "Patient_ID", "left") \\
    .join(site_df, "Site", "left") \\
    .select("Patient_ID", "Age", "Gender", "Weight (kg)", "Location", "Site_Type",
            "Avg_Biomarker_A", "Avg_Biomarker_B", "Avg_Biomarker_C")

# Show Final Processed Data
display(final_df)
"""

# Block comment: Standard mapping sheet data
STANDARD_MAPPING = [
    ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv",
     "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv"],
    ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/patient_demographics.csv",
     "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Patient_Demographics.csv"],
    ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/site_details.csv",
     "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Site_Data.csv"]
]

# --- TEST CASES ---

def test_happy_path():
    # Block comment: Test that all three volume paths are replaced as per mapping
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    for old, new in STANDARD_MAPPING:
        assert old not in updated, f"Old path {old} still present"
        assert new in updated, f"New path {new} not present"
    # Block comment: Ensure all other content remains unchanged except for the updated paths
    for line in ORIGINAL_SCRIPT.splitlines():
        if any(old in line for old, _ in STANDARD_MAPPING):
            continue
        assert line in updated, f"Line lost in update: {line}"
    # Block comment: Validate output is valid Python code
    assert is_valid_python_code(updated), "Output script is not valid Python syntax"

def test_no_change_for_unmapped_path():
    # Block comment: Test that unmapped volume paths remain unchanged
    script = ORIGINAL_SCRIPT + '\n# Extra path\nspark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/unknown_file.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/unknown_file.csv" in updated, "Unmapped path was changed"

def test_missing_required_columns():
    # Block comment: Test error when mapping sheet is missing required columns
    mapping = [
        ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv", ""]
    ]
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    # Only one column
    df = pd.DataFrame(mapping, columns=["old_volume_path"])
    df.to_excel(MAPPING_SHEET_PATH, index=False, sheet_name=MAPPING_SHEET_NAME, engine="openpyxl")
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "Mapping sheet must contain columns: old_volume_path, new_volume_path" in error

def test_missing_mapping_sheet():
    # Block comment: Test error when mapping sheet is missing
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    remove_file(MAPPING_SHEET_PATH)
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "Mapping sheet not found at specified location" in error

def test_missing_script():
    # Block comment: Test error when script is missing
    remove_file(SCRIPT_PATH)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "pyspark_script.py not found at specified location" in error

def test_no_matching_paths():
    # Block comment: Test that script remains unchanged if no paths match
    script = "# No volume paths here\nprint('Hello')\n"
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert updated == script, "Script changed when it should not"

def test_multiple_occurrences():
    # Block comment: Test that all occurrences of a path are replaced
    script = ORIGINAL_SCRIPT + '\n# Repeat\nspark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert updated.count("/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv") == 2
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv" not in updated

def test_no_substring_replacement():
    # Block comment: Test that substrings are not replaced
    script = '# Substring\nspark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv_extra")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv_extra" in updated

def test_duplicate_old_volume_path():
    # Block comment: Test error on duplicate old_volume_path in mapping
    mapping = STANDARD_MAPPING + [STANDARD_MAPPING[0]]
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "Duplicate old_volume_path entries found in mapping sheet" in error

def test_empty_new_volume_path():
    # Block comment: Test error on empty new_volume_path
    mapping = [STANDARD_MAPPING[0][:1] + [""]]
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "new_volume_path cannot be empty for old_volume_path" in error

def test_empty_old_volume_path():
    # Block comment: Test error on empty old_volume_path
    mapping = [["", "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv"]]
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert not success and "old_volume_path cannot be empty in mapping sheet" in error

def test_output_script_format_validation():
    # Block comment: Test that output script is valid Python and only paths are changed
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert is_valid_python_code(updated), "Output script is not valid Python syntax"
    for old, new in STANDARD_MAPPING:
        assert old not in updated
        assert new in updated

def test_case_sensitive_replacement():
    # Block comment: Test that replacement is case-sensitive
    script = 'spark.read.csv("/Volumes/Agilisium_Playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/Agilisium_Playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv" in updated

def test_whitespace_in_paths():
    # Block comment: Test that whitespace is trimmed before replacement
    mapping = [
        ["  /Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv  ",
         "  /Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv  "]
    ]
    write_file(SCRIPT_PATH, ORIGINAL_SCRIPT)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv" in updated

def test_non_csv_file_paths():
    # Block comment: Test that non-CSV file paths are replaced
    mapping = [
        ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.txt",
         "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.txt"]
    ]
    script = 'spark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.txt")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.txt" in updated

def test_special_characters_in_paths():
    # Block comment: Test that special character paths are replaced
    mapping = [
        ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker@readings$.csv",
         "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker@$.csv"]
    ]
    script = 'spark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker@readings$.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker@$.csv" in updated

def test_relative_paths():
    # Block comment: Test that relative paths are replaced
    mapping = [
        ["./biomarker_readings.csv", "./clinical_trail/19_03_2025_Biomarker.csv"]
    ]
    script = 'spark.read.csv("./biomarker_readings.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "./clinical_trail/19_03_2025_Biomarker.csv" in updated

def test_absolute_and_relative_paths():
    # Block comment: Test that both absolute and relative paths are replaced
    mapping = [
        ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv", "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv"],
        ["./biomarker_readings.csv", "./clinical_trail/19_03_2025_Biomarker.csv"]
    ]
    script = 'spark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/biomarker_readings.csv")\nspark.read.csv("./biomarker_readings.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_Biomarker.csv" in updated
    assert "./clinical_trail/19_03_2025_Biomarker.csv" in updated

def test_env_variable_paths():
    # Block comment: Test that environment variable paths are replaced
    mapping = [
        ["${VOLUME_PATH}/biomarker_readings.csv", "${VOLUME_PATH}/clinical_trail/19_03_2025_Biomarker.csv"]
    ]
    script = 'spark.read.csv("${VOLUME_PATH}/biomarker_readings.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "${VOLUME_PATH}/clinical_trail/19_03_2025_Biomarker.csv" in updated

def test_unicode_paths():
    # Block comment: Test that unicode paths are replaced
    mapping = [
        ["/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/生物标记.csv", "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_生物标记.csv"]
    ]
    script = 'spark.read.csv("/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/生物标记.csv")\n'
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    assert "/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_生物标记.csv" in updated

def test_performance_large_mapping():
    # Block comment: Test performance with large mapping sheet (1000 entries)
    mapping = []
    script_lines = []
    for i in range(1000):
        old = f"/Volumes/agilisium_playground/purgo_playground/de_dq/AD-218/file_{i}.csv"
        new = f"/Volumes/agilisium_playground/purgo_playground/de_dq/clinical_trail/19_03_2025_file_{i}.csv"
        mapping.append([old, new])
        script_lines.append(f'spark.read.csv("{old}")')
    script = "\n".join(script_lines)
    write_file(SCRIPT_PATH, script)
    write_mapping_excel(MAPPING_SHEET_PATH, mapping, columns=["old_volume_path", "new_volume_path"])
    success, error = run_update_logic(SCRIPT_PATH, MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    assert success, f"Update logic failed: {error}"
    updated = read_file(SCRIPT_PATH)
    for old, new in mapping:
        assert old not in updated
        assert new in updated

def test_schema_validation():
    # Block comment: Test that mapping sheet schema is as expected (STRING, STRING)
    write_mapping_excel(MAPPING_SHEET_PATH, STANDARD_MAPPING, columns=["old_volume_path", "new_volume_path"])
    df = pd.read_excel(MAPPING_SHEET_PATH, sheet_name=MAPPING_SHEET_NAME, engine="openpyxl")
    assert df["old_volume_path"].dtype == object, "old_volume_path is not string"
    assert df["new_volume_path"].dtype == object, "new_volume_path is not string"
    # Block comment: Validate with PySpark schema
    schema = StructType([
        StructField("old_volume_path", StringType(), False),
        StructField("new_volume_path", StringType(), False)
    ])
    spark_df = spark.createDataFrame([Row(old_volume_path=row[0], new_volume_path=row[1]) for row in STANDARD_MAPPING], schema=schema)
    assert spark_df.schema == schema, "PySpark schema does not match expected"

# --- RUN ALL TESTS ---

def run_all_tests():
    # Block comment: Run all test cases and cleanup
    try:
        test_happy_path()
        test_no_change_for_unmapped_path()
        test_missing_required_columns()
        test_missing_mapping_sheet()
        test_missing_script()
        test_no_matching_paths()
        test_multiple_occurrences()
        test_no_substring_replacement()
        test_duplicate_old_volume_path()
        test_empty_new_volume_path()
        test_empty_old_volume_path()
        test_output_script_format_validation()
        test_case_sensitive_replacement()
        test_whitespace_in_paths()
        test_non_csv_file_paths()
        test_special_characters_in_paths()
        test_relative_paths()
        test_absolute_and_relative_paths()
        test_env_variable_paths()
        test_unicode_paths()
        test_performance_large_mapping()
        test_schema_validation()
        print("All tests passed.")
    finally:
        cleanup_test_dir()

# --- MAIN ---
if __name__ == "__main__":
    run_all_tests()

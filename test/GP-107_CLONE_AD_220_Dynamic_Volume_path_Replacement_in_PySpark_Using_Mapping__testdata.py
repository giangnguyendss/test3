%pip install openpyxl

# The following script updates only the volume paths in pyspark_script.py using the mapping from volume_mapping_sheet.xlsx.
# All other script logic and content remain unchanged.

import os  
import sys  
import pandas as pd  
import openpyxl  

# --- CONFIGURATION ---
SCRIPT_PATH = "pyspark_script.py"
MAPPING_SHEET_PATH = "volume_mapping_sheet.xlsx"
MAPPING_SHEET_NAME = "Sheet1"

# --- HELPER FUNCTIONS ---

def error_exit(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def load_mapping(sheet_path, sheet_name):
    # Check mapping sheet existence
    if not os.path.exists(sheet_path):
        error_exit("Mapping sheet not found at specified location")
    try:
        df = pd.read_excel(sheet_path, sheet_name=sheet_name, engine="openpyxl")
    except Exception as e:
        error_exit(f"Failed to read mapping sheet: {e}")
    # Check required columns
    required_cols = {"old_volume_path", "new_volume_path"}
    if not required_cols.issubset(set(df.columns.str.strip())):
        error_exit("Mapping sheet must contain columns: old_volume_path, new_volume_path")
    # Trim whitespace
    df["old_volume_path"] = df["old_volume_path"].astype(str).str.strip()
    df["new_volume_path"] = df["new_volume_path"].astype(str).str.strip()
    # Check for empty old_volume_path
    if df["old_volume_path"].eq("").any():
        error_exit("old_volume_path cannot be empty in mapping sheet")
    # Check for empty new_volume_path
    empty_new = df[df["new_volume_path"].eq("")]
    if not empty_new.empty:
        error_exit(f"new_volume_path cannot be empty for old_volume_path: {empty_new.iloc[0]['old_volume_path']}")
    # Check for duplicates
    if df["old_volume_path"].duplicated().any():
        error_exit("Duplicate old_volume_path entries found in mapping sheet")
    # Return as dict
    return dict(zip(df["old_volume_path"], df["new_volume_path"]))

def update_script(script_path, mapping):
    # Check script existence
    if not os.path.exists(script_path):
        error_exit("pyspark_script.py not found at specified location")
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        error_exit(f"Failed to read script: {e}")
    # Replace all occurrences of old_volume_path with new_volume_path (case-sensitive, full match)
    for old_path, new_path in mapping.items():
        content = content.replace(old_path, new_path)
    # Write back to file
    try:
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        error_exit(f"Failed to write updated script: {e}")

def main():
    mapping = load_mapping(MAPPING_SHEET_PATH, MAPPING_SHEET_NAME)
    update_script(SCRIPT_PATH, mapping)
    print("Volume path replacement completed successfully.")

if __name__ == "__main__":
    main()

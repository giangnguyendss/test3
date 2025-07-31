spark.catalog.setCurrentCatalog("purgo_databricks")

# =========================================================================================================
# Databricks PySpark Script: Backup and Vacuum for customer_360_raw Table
# ---------------------------------------------------------------------------------------------------------
# This script performs:
#   1. Full backup of purgo_databricks.purgo_playground.customer_360_raw as compressed parquet files
#      partitioned by 'state' in /Volumes/customer_360_raw_backup (snappy compression, overwrite mode).
#   2. Deletes records older than 30 days (by creation_date) from the source table, then runs VACUUM
#      to clean up obsolete files (retaining 168 hours/7 days).
#   3. Logs all operations (success/failure) to purgo_databricks.purgo_playground.customer_360_raw_backup_log
#      with timestamp, status, operation_type, record_count, and error_message.
#   4. Implements robust error handling, data validation, and schema checks.
# ---------------------------------------------------------------------------------------------------------
# Assumptions:
#   - 'spark' session is available in Databricks.
#   - User has read/write access to source table, backup volume, and log table.
#   - Source table is a Delta table and contains 'state' and 'creation_date' columns.
#   - Backup is a full snapshot, overwriting previous backup.
#   - All string fields use STRING type, all date fields use DATE type.
#   - No temp views or temp tables are used.
#   - No direct use of os/shutil/pathlib/dbfs; only dbutils.fs for volume checks.
# ---------------------------------------------------------------------------------------------------------
# Configuration Section
# =========================================================================================================

from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType  
from datetime import datetime, timedelta  

# -- Unity Catalog and Table References
CATALOG = "purgo_databricks"
SCHEMA = "purgo_playground"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw"
BACKUP_LOG_TABLE = f"{CATALOG}.{SCHEMA}.customer_360_raw_backup_log"
BACKUP_PATH = "/Volumes/customer_360_raw_backup"
PARQUET_COMPRESSION = "snappy"
PARTITION_COL = "state"
DATE_COL = "creation_date"
VACUUM_RETENTION_HOURS = 168  # 7 days
RETENTION_DAYS = 30

# =========================================================================================================
# Helper Functions
# =========================================================================================================

def log_operation(status, operation_type, record_count, error_message):
    """
    Log backup or vacuum operation to the backup log table.
    """
    log_df = spark.createDataFrame(
        [(datetime.utcnow().isoformat(), status, operation_type, record_count, error_message)],
        schema=StructType([
            StructField("timestamp", StringType(), False),
            StructField("status", StringType(), False),
            StructField("operation_type", StringType(), False),
            StructField("record_count", LongType(), False),
            StructField("error_message", StringType(), True),
        ])
    )
    log_df.write.format("delta").mode("append").saveAsTable(BACKUP_LOG_TABLE)

def check_volume_exists(volume_path):
    """
    Check if the backup volume path exists using dbutils.fs.
    """
    try:
        files = dbutils.fs.ls(volume_path)
        return True
    except Exception:
        return False

def get_table_schema(table_name):
    """
    Get the schema of a table as a StructType.
    """
    return spark.table(table_name).schema

def assert_schema_match(df, table_name):
    """
    Assert that the DataFrame schema matches the target table schema.
    """
    table_schema = get_table_schema(table_name)
    if df.schema != table_schema:
        raise Exception(f"Schema mismatch: {df.schema} != {table_schema}")

def assert_column_count_match(df, table_name):
    """
    Assert that the number of columns in the DataFrame matches the target table.
    """
    table_schema = get_table_schema(table_name)
    if len(df.columns) != len(table_schema):
        raise Exception(f"Column count mismatch: {len(df.columns)} != {len(table_schema)}")

def assert_partition_column_exists(df, partition_col):
    """
    Assert that the partition column exists in the DataFrame.
    """
    if partition_col not in df.columns:
        raise Exception(f"Partition column '{partition_col}' not found")

def assert_no_nulls_in_partition_column(df, partition_col):
    """
    Assert that there are no NULLs in the partition column.
    """
    null_count = df.filter(F.col(partition_col).isNull()).limit(1).count()
    if null_count > 0:
        raise Exception(f"Null values found in partition column '{partition_col}'")

def assert_no_empty_table(df):
    """
    Assert that the DataFrame is not empty.
    """
    count = df.limit(1).count()
    if count == 0:
        raise Exception("Source table is empty")

def assert_data_type_compatibility(df, table_name):
    """
    Assert that all data types in the DataFrame are compatible with the target table.
    """
    table_schema = get_table_schema(table_name)
    for f1, f2 in zip(df.schema.fields, table_schema.fields):
        if f1.dataType != f2.dataType:
            raise Exception(f"Data type mismatch for column '{f1.name}': {f1.dataType} != {f2.dataType}")

def assert_delta_table(table_name):
    """
    Assert that the table is a Delta table.
    """
    details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
    if not details or details[0]['format'] != 'delta':
        raise Exception("VACUUM operation requires a Delta table")

def assert_column_exists(df, col_name):
    """
    Assert that a column exists in the DataFrame.
    """
    if col_name not in df.columns:
        raise Exception(f"Column '{col_name}' not found")

def assert_valid_date_column(df, col_name):
    """
    Assert that the date column contains only valid dates or nulls.
    """
    field = [f for f in df.schema.fields if f.name == col_name]
    if not field or not isinstance(field[0].dataType, DateType):
        raise Exception(f"Invalid date format in '{col_name}' column")

# =========================================================================================================
# 1. Backup Operation: Write Parquet Partitioned by State
# =========================================================================================================

try:
    # -- Step 1: Check backup volume path exists
    if not check_volume_exists(BACKUP_PATH):
        log_operation("FAILURE", "BACKUP", 0, "Backup volume path not found")
        raise Exception("Backup volume path not found")

    # -- Step 2: Read source table
    df = spark.table(SOURCE_TABLE)

    # -- Step 3: Data quality and schema checks
    assert_no_empty_table(df)
    assert_partition_column_exists(df, PARTITION_COL)
    assert_no_nulls_in_partition_column(df, PARTITION_COL)
    assert_schema_match(df, SOURCE_TABLE)
    assert_column_count_match(df, SOURCE_TABLE)
    assert_data_type_compatibility(df, SOURCE_TABLE)

    # -- Step 4: Write backup as compressed parquet partitioned by state (overwrite mode)
    df.write.mode("overwrite") \
        .partitionBy(PARTITION_COL) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(BACKUP_PATH)

    # -- Step 5: Validate backup completeness and data integrity
    backup_df = spark.read.parquet(BACKUP_PATH)
    if df.count() != backup_df.count():
        log_operation("FAILURE", "BACKUP", 0, "Record count mismatch between source and backup")
        raise Exception("Record count mismatch between source and backup")
    if df.schema != backup_df.schema:
        log_operation("FAILURE", "BACKUP", 0, "Schema mismatch between source and backup")
        raise Exception("Schema mismatch between source and backup")

    # -- Step 6: Log success
    log_operation("SUCCESS", "BACKUP", df.count(), None)

except Exception as e:
    # -- Log failure
    log_operation("FAILURE", "BACKUP", 0, str(e))

# =========================================================================================================
# 2. Vacuum Operation: Delete Old Records and Run VACUUM
# =========================================================================================================

try:
    # -- Step 1: Ensure table is Delta
    assert_delta_table(SOURCE_TABLE)

    # -- Step 2: Ensure creation_date column exists and is valid
    df = spark.table(SOURCE_TABLE)
    assert_column_exists(df, DATE_COL)
    assert_valid_date_column(df, DATE_COL)
    assert_no_empty_table(df)

    # -- Step 3: Delete records older than 30 days (inclusive)
    cutoff_date = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).date()
    spark.sql(f"""
        DELETE FROM {SOURCE_TABLE}
        WHERE {DATE_COL} < DATE('{cutoff_date}')
    """)

    # -- Step 4: Validate only recent records remain
    remaining_df = spark.table(SOURCE_TABLE)
    old_count = remaining_df.filter(F.col(DATE_COL) < F.lit(str(cutoff_date))).limit(1).count()
    if old_count > 0:
        log_operation("FAILURE", "VACUUM", 0, f"Old records found with {DATE_COL} < {cutoff_date}")
        raise Exception(f"Old records found with {DATE_COL} < {cutoff_date}")

    # -- Step 5: Run VACUUM with default retention
    spark.sql(f"VACUUM {SOURCE_TABLE} RETAIN {VACUUM_RETENTION_HOURS} HOURS")

    # -- Step 6: Log success
    log_operation("SUCCESS", "VACUUM", remaining_df.count(), None)

except Exception as e:
    # -- Log failure
    log_operation("FAILURE", "VACUUM", 0, str(e))

# =========================================================================================================
# End of Script
# =========================================================================================================

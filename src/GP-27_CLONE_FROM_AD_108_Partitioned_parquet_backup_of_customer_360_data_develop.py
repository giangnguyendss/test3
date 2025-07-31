spark.catalog.setCurrentCatalog("purgo_databricks")

# ============================================================
# PySpark Script: Backup and Vacuum for customer_360_raw Table
# ============================================================
#
# This script performs:
#   - Full backup of purgo_databricks.purgo_playground.customer_360_raw as compressed parquet files
#   - Partitioning by 'state' column
#   - Snappy compression
#   - Backup to /Volumes/customer_360_raw_backup (Databricks volume)
#   - Hard vacuum: delete records older than 30 days (720 hours) based on 'creation_date'
#   - Logging of all operations to purgo_playground.customer_360_raw_backup_log
#   - Retention cleanup: delete backup files older than 90 days
#
# Assumptions:
#   - 'spark' session is available in Databricks
#   - All referenced tables exist in Unity Catalog
#   - All columns are included in backup, no masking required
#   - Backup is full (not incremental)
#   - Backup and vacuum are scheduled externally (not handled here)
#
# Security/Performance:
#   - Uses dbutils.fs for distributed file operations (Databricks best practice)
#   - Validates schema and data types before backup
#   - Handles errors and logs all outcomes
#   - Efficient partitioning and compression for storage optimization
#
# =========================
# Imports and Configuration
# =========================

from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType  
import datetime  

# =========================
# Constants and Parameters
# =========================

# Unity Catalog table and schema names
SOURCE_TABLE = "purgo_databricks.purgo_playground.customer_360_raw"
BACKUP_LOG_TABLE = "purgo_playground.customer_360_raw_backup_log"

# Databricks volume path for backup
BACKUP_PATH = "/Volumes/customer_360_raw_backup"

# Parquet compression codec
PARQUET_COMPRESSION = "snappy"

# Partition column for backup
PARTITION_COL = "state"

# Retention policy for backup files (in days)
BACKUP_RETENTION_DAYS = 90

# Vacuum retention (in days)
VACUUM_RETENTION_DAYS = 30

# =========================
# Utility Functions
# =========================

def log_operation(status, operation_type, record_count, error_message):
    """
    Log backup, vacuum, or retention operation to backup log table.
    """
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    log_row = [(now, status, operation_type, record_count, error_message)]
    log_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("record_count", LongType(), True),
        StructField("error_message", StringType(), True)
    ])
    log_df = spark.createDataFrame(log_row, schema=log_schema)
    log_df.write.format("delta").mode("append").saveAsTable(BACKUP_LOG_TABLE)

def check_volume_exists(volume_path):
    """
    Check if the Databricks volume path exists using dbutils.fs.
    """
    try:
        files = dbutils.fs.ls(volume_path)
        return True
    except Exception:
        return False

def get_free_space_bytes(volume_path):
    """
    Estimate free space in the Databricks volume.
    Note: dbutils.fs does not provide direct free space info.
    This function returns None (cannot be reliably implemented in Databricks).
    """
    return None  # Not supported; handled as best effort

def list_files_older_than(volume_path, days):
    """
    List files in the volume path older than the specified number of days.
    """
    cutoff_ts = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).timestamp()
    old_files = []
    try:
        for dir_info in dbutils.fs.ls(volume_path):
            if dir_info.isDir():
                # Recursively check partition directories
                for file_info in dbutils.fs.ls(dir_info.path):
                    if not file_info.isDir() and file_info.modificationTime/1000 < cutoff_ts:
                        old_files.append(file_info.path)
            else:
                if dir_info.modificationTime/1000 < cutoff_ts:
                    old_files.append(dir_info.path)
    except Exception:
        pass
    return old_files

# =========================
# 1. Full Backup Operation
# =========================

try:
    # -- Step 1: Validate backup volume path exists
    if not check_volume_exists(BACKUP_PATH):
        log_operation("FAILED", "BACKUP", 0, "Backup path does not exist")
        raise Exception("Backup path does not exist")

    # -- Step 2: (Optional) Check for sufficient storage (not directly possible in Databricks)
    # Skipped: Databricks does not expose free space via dbutils.fs

    # -- Step 3: Read source table and validate schema
    expected_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("company", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("account_manager", StringType(), True),
        StructField("creation_date", DateType(), True),
        StructField("last_interaction_date", DateType(), True),
        StructField("purchase_history", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("zip", StringType(), True)
    ])
    df = spark.table(SOURCE_TABLE)
    # -- Ensure column count and order match
    if df.columns != [f.name for f in expected_schema.fields]:
        log_operation("FAILED", "BACKUP", 0, "Schema mismatch between source table and expected schema")
        raise Exception("Schema mismatch between source table and expected schema")
    # -- Ensure data types match (cast if needed)
    for field in expected_schema.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # -- Step 4: Count records for logging
    record_count = df.count()

    # -- Step 5: Write to compressed parquet, partitioned by state
    df.write.mode("overwrite") \
        .partitionBy(PARTITION_COL) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(BACKUP_PATH)

    # -- Step 6: Log success
    log_operation("SUCCESS", "BACKUP", record_count, None)

except Exception as e:
    # -- Log failure if not already logged
    if "log_operation" in locals():
        log_operation("FAILED", "BACKUP", 0, str(e))

# =========================
# 2. Vacuum Operation (Hard Delete)
# =========================

try:
    # -- Step 1: Calculate cutoff date (30 days ago)
    cutoff_date = (datetime.date.today() - datetime.timedelta(days=VACUUM_RETENTION_DAYS)).isoformat()

    # -- Step 2: Count records to be deleted
    cte_count = (
        df.filter(F.col("creation_date") < F.lit(cutoff_date))
        .select(F.count("*").alias("to_delete_count"))
    )
    to_delete_count = cte_count.collect()[0]["to_delete_count"]

    # -- Step 3: Perform hard delete using SQL DELETE
    spark.sql(f"""
        DELETE FROM {SOURCE_TABLE}
        WHERE creation_date < DATE('{cutoff_date}')
    """)

    # -- Step 4: Log success
    log_operation("SUCCESS", "VACUUM", to_delete_count, None)

except Exception as e:
    log_operation("FAILED", "VACUUM", 0, str(e))

# =========================
# 3. Retention Cleanup (Delete Backup Files Older Than 90 Days)
# =========================

try:
    old_files = list_files_older_than(BACKUP_PATH, BACKUP_RETENTION_DAYS)
    deleted_count = 0
    for file_path in old_files:
        try:
            dbutils.fs.rm(file_path, True)
            deleted_count += 1
        except Exception:
            # Ignore errors for individual files, continue
            pass
    log_operation("SUCCESS", "RETENTION_CLEANUP", deleted_count, None)
except Exception as e:
    log_operation("FAILED", "RETENTION_CLEANUP", 0, str(e))

# =========================
# End of Script
# =========================

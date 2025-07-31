spark.catalog.setCurrentCatalog("purgo_databricks")

# -----------------------------------------------------------------------------
# Channel-wise Sales Performance Analysis for Biosimilar Brands (PySpark)
# -----------------------------------------------------------------------------
# This script reads sales data from purgo_playground.bai_sales_agg_obu_customer_datapack,
# filters for biosimilar brands (kanjinti, mvasi, riabni) and valid channels (online, retail, mobile),
# restricts to records within 36 months from a provided cdl_effective_date (global parameter, yyyy-MM-dd),
# excludes records with nulls in group-by or metric fields,
# and aggregates sales metrics by channel and other key attributes.
# Invalid or missing cdl_effective_date records are logged to the error log table.
# -----------------------------------------------------------------------------
# Assumptions:
# - 'spark' session is available in Databricks.
# - Input table: purgo_playground.bai_sales_agg_obu_customer_datapack (Unity Catalog Delta table)
# - Error log table: purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
# - Output: Display channel-wise aggregated result as a DataFrame.
# -----------------------------------------------------------------------------

# --- Imports ---
# Commented out SparkSession initialization (already available in Databricks)
from pyspark.sql import SparkSession  # built-in
# spark = SparkSession.builder.getOrCreate()

from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, TimestampType, BooleanType, LongType  
from datetime import datetime, timedelta  

# --- Configuration ---
# /* Set the global cdl_effective_date parameter (must be in 'yyyy-MM-dd' format) */
CDL_EFFECTIVE_DATE = "2021-01-01"  # <-- Set this value as needed

# --- Helper Functions ---
def is_valid_yyyy_mm_dd(date_str):
    # Returns True if date_str is in 'yyyy-MM-dd' format, else False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except Exception:
        return False

# Register the UDF for use in DataFrame API
is_valid_yyyy_mm_dd_udf = F.udf(is_valid_yyyy_mm_dd, BooleanType())

# --- Read Source Data ---
df = spark.table("purgo_playground.bai_sales_agg_obu_customer_datapack")

# --- Data Quality: Log records with invalid or missing cdl_effective_date ---
# /* Identify and log records with null or invalid cdl_effective_date format */
invalid_cdl_df = (
    df
    .filter(
        (F.col("cdl_effective_date").isNull()) |
        (~F.col("cdl_effective_date").rlike(r"^\d{4}-\d{2}-\d{2}$")) |
        (~is_valid_yyyy_mm_dd_udf(F.col("cdl_effective_date")))
    )
    .withColumn(
        "error_message",
        F.when(F.col("cdl_effective_date").isNull(), F.lit("Missing cdl_effective_date"))
         .otherwise(F.lit("Invalid cdl_effective_date format"))
    )
)

# /* Write invalid records to error log table (append mode, schema must match) */
if invalid_cdl_df.head(1):
    invalid_cdl_df.select(
        "brand_normalized_name", "normalized_name", "market_normalized_name",
        "competitor_flag", "channel_name", "transaction_timestamp",
        "integrated_units", "integrated_normalized_units", "integrated_dollars",
        "cdl_effective_date", "error_message"
    ).write.mode("append").saveAsTable("purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log")

# --- Filter for Valid Records ---
# /* Only keep records with valid cdl_effective_date format and matching the global parameter */
valid_df = (
    df
    .filter(F.col("cdl_effective_date").isNotNull())
    .filter(F.col("cdl_effective_date") == CDL_EFFECTIVE_DATE)
    .filter(F.col("cdl_effective_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    .filter(is_valid_yyyy_mm_dd_udf(F.col("cdl_effective_date")))
)

# --- Calculate 36-Month Window ---
# /* Compute window start and end timestamps */
window_start = datetime.strptime(CDL_EFFECTIVE_DATE, "%Y-%m-%d")
try:
    window_end = window_start.replace(year=window_start.year + 3)
except ValueError:
    # Handle leap year edge case
    window_end = window_start + timedelta(days=365*3)

# --- CTE: Filtered and Aggregated Data ---
# /* CTE: Filter for biosimilar brands, valid channels, 36-month window, and non-null group-by/metric fields */
cte_df = (
    valid_df
    .filter(F.col("brand_normalized_name").isin("kanjinti", "mvasi", "riabni"))
    .filter(F.col("channel_name").isin("online", "retail", "mobile"))
    .filter(
        (F.col("transaction_timestamp") >= F.lit(window_start)) &
        (F.col("transaction_timestamp") < F.lit(window_end))
    )
    .filter(
        F.col("brand_normalized_name").isNotNull() &
        F.col("normalized_name").isNotNull() &
        F.col("market_normalized_name").isNotNull() &
        F.col("competitor_flag").isNotNull() &
        F.col("channel_name").isNotNull() &
        F.col("integrated_units").isNotNull() &
        F.col("integrated_normalized_units").isNotNull() &
        F.col("integrated_dollars").isNotNull()
    )
    .groupBy(
        "brand_normalized_name",
        "normalized_name",
        "market_normalized_name",
        "competitor_flag",
        "channel_name"
    )
    .agg(
        F.sum("integrated_units").alias("sum_integrated_units"),
        F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
        F.sum("integrated_dollars").alias("sum_integrated_dollars")
    )
)

# --- Output: Show Channel-wise Aggregated Result ---
# /* Display the result: one row per unique group-by combination with aggregated sales metrics */
cte_df.show(truncate=False)

# Commented out spark.stop() as it can cause issues in Databricks
# spark.stop()

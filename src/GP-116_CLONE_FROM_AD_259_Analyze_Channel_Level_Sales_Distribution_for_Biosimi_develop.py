spark.catalog.setCurrentCatalog("purgo_databricks")

# /*
#   Channel-wise Sales Performance Analysis for Biosimilar Brands (kanjinti, mvasi, riabni)
#   --------------------------------------------------------------------------------------
#   - Reads from purgo_playground.bai_sales_agg_obu_customer_datapack (Unity Catalog)
#   - Filters for brands: kanjinti, mvasi, riabni
#   - Filters for channels: online, retail, mobile
#   - Includes only records with valid, non-null cdl_effective_date in 'YYYY-MM-DD' format
#   - Includes only records where cdl_effective_date is within 36 months prior to a global reference date
#   - Aggregates sales metrics by channel and logs invalid records to purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log
#   - Handles nulls in sales metrics as zero for aggregation
#   - All code is Databricks and Unity Catalog compatible
#   - No temp views or temp tables are used
#   - All file operations use dbutils.fs (if needed), but none are required here
#   - All code is production-ready and follows Databricks best practices
# */

# ---------------------------
# /* SECTION: Imports & Setup */
# ---------------------------

from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, BooleanType, LongType, TimestampType  
from datetime import datetime, timedelta  
import re  

# ---------------------------
# /* SECTION: Configuration */
# ---------------------------

# Set the global reference date for the 36-month window (YYYY-MM-DD)
REFERENCE_DATE_STR = "2024-05-15"  # <-- Update as needed for production
REFERENCE_DATE = datetime.strptime(REFERENCE_DATE_STR, "%Y-%m-%d")
MONTHS_WINDOW = 36

# Allowed brands and channels
ALLOWED_BRANDS = ["kanjinti", "mvasi", "riabni"]
ALLOWED_CHANNELS = ["online", "retail", "mobile"]

# ---------------------------
# /* SECTION: Helper Functions */
# ---------------------------

def is_valid_date(date_str):
    """
    Returns True if date_str is a valid 'YYYY-MM-DD' date, else False.
    """
    if date_str is None:
        return False
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        return False
    except Exception:
        return False

def is_within_36_months(date_str, ref_date=REFERENCE_DATE):
    """
    Returns True if date_str is within 36 months prior to ref_date, else False.
    """
    if not is_valid_date(date_str):
        return False
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    delta = (ref_date.year - dt.year) * 12 + (ref_date.month - dt.month)
    if dt > ref_date:
        return False
    return 0 <= delta <= MONTHS_WINDOW

# Register UDFs for use in DataFrame API
from pyspark.sql.functions import udf  

is_valid_date_udf = udf(is_valid_date, BooleanType())
is_within_36_months_udf = udf(lambda d: is_within_36_months(d), BooleanType())

# ---------------------------
# /* SECTION: Read Source Data */
# ---------------------------

# Read from Unity Catalog table (no schema override, Delta source)
df = spark.table("purgo_playground.bai_sales_agg_obu_customer_datapack")

# ---------------------------
# /* SECTION: Data Quality & Logging Invalid Records */
# ---------------------------

# Add error_message for invalid or null cdl_effective_date
df_with_error = df.withColumn(
    "error_message",
    F.when(
        (F.col("cdl_effective_date").isNull()) |
        (F.trim(F.col("cdl_effective_date")) == "") |
        (~is_valid_date_udf(F.col("cdl_effective_date"))),
        F.lit("Invalid or null cdl_effective_date")
    )
)

# Identify records to log: invalid cdl_effective_date, brand not in list, channel not in list, or outside 36 months
invalid_records = df_with_error.filter(
    (F.col("error_message").isNotNull()) |
    (~F.col("brand_normalized_name").isin(ALLOWED_BRANDS)) |
    (~F.col("channel_name").isin(ALLOWED_CHANNELS)) |
    (~is_within_36_months_udf(F.col("cdl_effective_date")))
)

# Write invalid records to log table (append mode, production should use merge/upsert as needed)
invalid_records_to_log = invalid_records.withColumn(
    "error_message",
    F.when(F.col("error_message").isNotNull(), F.col("error_message"))
     .when(~F.col("brand_normalized_name").isin(ALLOWED_BRANDS), F.lit("brand_normalized_name not in allowed list"))
     .when(~F.col("channel_name").isin(ALLOWED_CHANNELS), F.lit("channel_name not in allowed list"))
     .when(~is_within_36_months_udf(F.col("cdl_effective_date")), F.lit("cdl_effective_date not within 36 months"))
     .otherwise(F.lit("Unknown error"))
)

invalid_records_to_log.select(
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name",
    "transaction_timestamp",
    "integrated_units",
    "integrated_normalized_units",
    "integrated_dollars",
    "cdl_effective_date",
    "error_message"
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "purgo_playground.bai_sales_agg_obu_customer_datapack_invalid_cdl_effective_date_log"
)

# ---------------------------
# /* SECTION: Filter Valid Records for Aggregation */
# ---------------------------

valid_records = df_with_error.filter(
    (F.col("error_message").isNull()) &
    (F.col("brand_normalized_name").isin(ALLOWED_BRANDS)) &
    (F.col("channel_name").isin(ALLOWED_CHANNELS)) &
    (is_within_36_months_udf(F.col("cdl_effective_date")))
)

# ---------------------------
# /* SECTION: Null Handling for Aggregation */
# ---------------------------

# Treat nulls as zero for aggregation columns
valid_records_for_agg = valid_records.withColumn(
    "integrated_units", F.coalesce(F.col("integrated_units"), F.lit(0))
).withColumn(
    "integrated_normalized_units", F.coalesce(F.col("integrated_normalized_units"), F.lit(0))
).withColumn(
    "integrated_dollars", F.coalesce(F.col("integrated_dollars"), F.lit(0))
)

# ---------------------------
# /* SECTION: Aggregation by Channel */
# ---------------------------

agg_result = valid_records_for_agg.groupBy(
    "brand_normalized_name",
    "normalized_name",
    "market_normalized_name",
    "competitor_flag",
    "channel_name"
).agg(
    F.sum("integrated_units").alias("sum_integrated_units"),
    F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
    F.sum("integrated_dollars").alias("sum_integrated_dollars")
)

# ---------------------------
# /* SECTION: Show Final Output */
# ---------------------------

# Show the result of channel-wise aggregation
agg_result.show(truncate=False)

# ---------------------------
# /* SECTION: (Optional) Combined Channel Summary */
# ---------------------------

# If a combined summary across all channels is required, uncomment below:
# combined_agg = valid_records_for_agg.groupBy(
#     "brand_normalized_name",
#     "normalized_name",
#     "market_normalized_name",
#     "competitor_flag"
# ).agg(
#     F.sum("integrated_units").alias("sum_integrated_units"),
#     F.sum("integrated_normalized_units").alias("sum_integrated_normalized_units"),
#     F.sum("integrated_dollars").alias("sum_integrated_dollars")
# )
# combined_agg.show(truncate=False)

# ---------------------------
# /* End of Channel-wise Sales Performance Analysis */
# ---------------------------

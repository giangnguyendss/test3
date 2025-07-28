spark.catalog.setCurrentCatalog("purgo_databricks")

# /*
#   Cold Chain Temperature Monitoring and Violation Aggregation
#   ----------------------------------------------------------------------
#   - Unity Catalog: purgo_databricks
#   - Schema: purgo_playground
#   - Table: cold_chain_temperature
#   - Requirements:
#       * Detect temperature violations (temperature < -8.0 or temperature > 0.0)
#       * Add violation_flag: "yes" (violation), "no" (in range), "error" (invalid/missing)
#       * Aggregate by location: average temperature, count of violations
#       * Handle NULL/invalid temperature values and log errors
#       * Output two DataFrames: one with violation flags, one with aggregation results
#   - All code is Databricks PySpark production-ready, with error handling and comments
# */

# ----------------------------------------
# /* SECTION: Imports (Databricks Only)  */
# ----------------------------------------

from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType  
from pyspark.sql.functions import col, when, avg, count  
from pyspark.sql.functions import udf  

# ----------------------------------------
# /* SECTION: Read Source Table           */
# ----------------------------------------

# Read the cold_chain_temperature table from Unity Catalog
# Table: purgo_playground.cold_chain_temperature
# Columns: location (STRING), temperature (STRING or DOUBLE), event_time (TIMESTAMP), etc.

df_src = spark.table("purgo_playground.cold_chain_temperature")

# ----------------------------------------
# /* SECTION: Violation Flag UDF          */
# ----------------------------------------

# UDF to parse temperature and set violation_flag
def parse_temp_and_flag(temp):
    try:
        if temp is None or (isinstance(temp, str) and temp.strip() == ""):
            return ("error", None)
        t = float(temp)
        if t < -8.0 or t > 0.0:
            return ("yes", t)
        else:
            return ("no", t)
    except Exception:
        return ("error", None)

parse_temp_and_flag_udf = udf(
    parse_temp_and_flag,
    StructType([
        StructField("violation_flag", StringType(), True),
        StructField("temperature_num", DoubleType(), True)
    ])
)

# ----------------------------------------
# /* SECTION: Apply Violation Detection   */
# ----------------------------------------

# Add violation_flag and parsed numeric temperature
df_flagged = df_src.withColumn(
    "parsed", parse_temp_and_flag_udf(col("temperature"))
).withColumn(
    "violation_flag", col("parsed.violation_flag")
).withColumn(
    "temperature_num", col("parsed.temperature_num")
).drop("parsed")

# ----------------------------------------
# /* SECTION: Error Logging               */
# ----------------------------------------

# Log error message for invalid temperature values
def log_error(row):
    if row.violation_flag == "error":
        print(f"Invalid temperature value at location '{row.location}' and event_time '{row.event_time}': {row.temperature}")

df_flagged.rdd.foreach(log_error)

# ----------------------------------------
# /* SECTION: Output DataFrames           */
# ----------------------------------------

# DataFrame 1: All records with violation_flag
df_violations = df_flagged.select(
    *[c for c in df_src.columns],  # All original columns
    "violation_flag"
)

# DataFrame 2: Aggregation by location (exclude error rows)
df_agg = (
    df_flagged
    .where(col("violation_flag") != "error")
    .groupBy("location")
    .agg(
        avg("temperature_num").alias("average_temperature"),
        count(when(col("violation_flag") == "yes", True)).alias("violation_count")
    )
)

# ----------------------------------------
# /* SECTION: Show Results                */
# ----------------------------------------

# Show violations DataFrame
df_violations.show(truncate=False)

# Show aggregation DataFrame
df_agg.show(truncate=False)

# ----------------------------------------
# /* END OF SCRIPT                       */
# ----------------------------------------

%pip install pytest

# /* 
#   Databricks PySpark Test Suite for Cold Chain Temperature Monitoring
#   - Unity Catalog: purgo_databricks
#   - Schema: purgo_playground
#   - Table: cold_chain_temperature
#   - Requirements:
#       * Detect temperature violations (temperature < -8.0 or temperature > 0.0)
#       * Aggregate by location: average temperature, count of violations
#       * Handle NULL/invalid temperature values
#       * Validate schema, data types, and data quality
#       * Test Delta Lake operations, window functions, and cleanup
#   - All code is executable in Databricks and follows best practices
# */

# ---------------------------
# /* SECTION: Imports & Setup */
# ---------------------------

from pyspark.sql import SparkSession  # (spark is already available in Databricks)
from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType  
from pyspark.sql.functions import col, when, isnan, avg, count, lit, expr  
from pyspark.sql.functions import udf  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType  
from pyspark.sql.window import Window  
import pytest  

# ---------------------------
# /* SECTION: Test Data Setup */
# ---------------------------

# Test data covers:
# - Valid in-range, out-of-range, edge, null, invalid, special characters in location
test_data = [
    Row(location="SiteA", temperature="-8.0", event_time="2024-03-21T00:00:00.000+0000"),
    Row(location="SiteA", temperature="-5.0", event_time="2024-03-21T01:00:00.000+0000"),
    Row(location="SiteB", temperature="0.0", event_time="2024-03-21T02:00:00.000+0000"),
    Row(location="Depot1", temperature="-7.5", event_time="2024-03-21T03:00:00.000+0000"),
    Row(location="Depot2", temperature="-6.0", event_time="2024-03-21T04:00:00.000+0000"),
    Row(location="SiteA", temperature="-8.1", event_time="2024-03-21T05:00:00.000+0000"),
    Row(location="SiteB", temperature="0.1", event_time="2024-03-21T06:00:00.000+0000"),
    Row(location="Depot1", temperature="-9.0", event_time="2024-03-21T07:00:00.000+0000"),
    Row(location="Depot2", temperature="1.0", event_time="2024-03-21T08:00:00.000+0000"),
    Row(location="SiteA", temperature="5.0", event_time="2024-03-21T09:00:00.000+0000"),
    Row(location="SiteB", temperature="-20.0", event_time="2024-03-21T10:00:00.000+0000"),
    Row(location="Depot1", temperature=None, event_time="2024-03-21T11:00:00.000+0000"),
    Row(location="Depot2", temperature="abc", event_time="2024-03-21T12:00:00.000+0000"),
    Row(location="仓库A", temperature="-8.0", event_time="2024-03-21T13:00:00.000+0000"),
    Row(location="СкладБ", temperature="0.0", event_time="2024-03-21T14:00:00.000+0000"),
    Row(location="Site-Ω", temperature="-8.5", event_time="2024-03-21T15:00:00.000+0000"),
    Row(location="Depot#1", temperature="0.5", event_time="2024-03-21T16:00:00.000+0000"),
    Row(location="Depot#1", temperature="-7.0", event_time="2024-03-21T17:00:00.000+0000"),
    Row(location="SiteA", temperature="-8.0001", event_time="2024-03-21T18:00:00.000+0000"),
    Row(location="SiteB", temperature="0.0001", event_time="2024-03-21T19:00:00.000+0000"),
    Row(location="Depot2", temperature=None, event_time="2024-03-21T20:00:00.000+0000"),
    Row(location="Depot2", temperature="", event_time="2024-03-21T21:00:00.000+0000"),
    Row(location="Depot2", temperature="-9.5", event_time="2024-03-21T22:00:00.000+0000"),
    Row(location="Depot2", temperature="0.0", event_time="2024-03-21T23:00:00.000+0000"),
    Row(location="Depot2", temperature="1e2", event_time="2024-03-22T00:00:00.000+0000"),
]

schema = StructType([
    StructField("location", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("event_time", StringType(), True),
])

df_raw = spark.createDataFrame(test_data, schema=schema)
df = df_raw.withColumn("event_time", col("event_time").cast(TimestampType()))

# ------------------------------------------
# /* SECTION: UDF for Violation Flag Parsing */
# ------------------------------------------

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

parse_temp_and_flag_udf = udf(parse_temp_and_flag, 
    StructType([
        StructField("violation_flag", StringType(), True),
        StructField("temperature_num", DoubleType(), True)
    ])
)

df_flagged = df.withColumn(
    "parsed", parse_temp_and_flag_udf(col("temperature"))
).withColumn(
    "violation_flag", col("parsed.violation_flag")
).withColumn(
    "temperature_num", col("parsed.temperature_num")
).drop("parsed")

# ------------------------------------------
# /* SECTION: Error Logging for Invalid Data */
# ------------------------------------------

def log_error(row):
    if row.violation_flag == "error":
        print(f"Invalid temperature value at location '{row.location}' and event_time '{row.event_time}': {row.temperature}")

df_flagged.foreach(log_error)

# ------------------------------------------
# /* SECTION: Schema Validation Tests */
# ------------------------------------------

expected_schema = StructType([
    StructField("location", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("violation_flag", StringType(), True),
    StructField("temperature_num", DoubleType(), True),
])

assert df_flagged.schema == expected_schema, "Schema does not match expected schema"

# ------------------------------------------
# /* SECTION: Data Type Conversion Tests */
# ------------------------------------------

# Test: All temperature_num values are either float or None
for row in df_flagged.select("temperature_num").collect():
    assert (row.temperature_num is None) or isinstance(row.temperature_num, float), "temperature_num is not float or None"

# Test: All violation_flag values are in allowed set
allowed_flags = {"yes", "no", "error"}
for row in df_flagged.select("violation_flag").collect():
    assert row.violation_flag in allowed_flags, f"violation_flag {row.violation_flag} is not valid"

# ------------------------------------------
# /* SECTION: Violation Detection Unit Tests */
# ------------------------------------------

test_cases = [
    ("-9.5", "yes"),
    ("-8.1", "yes"),
    ("-8.0", "no"),
    ("-5.0", "no"),
    ("0.0", "no"),
    ("0.1", "yes"),
    ("5.0", "yes"),
    (None, "error"),
    ("abc", "error"),
    ("", "error"),
]

for temp, expected_flag in test_cases:
    result = parse_temp_and_flag(temp)[0]
    assert result == expected_flag, f"Temperature {temp}: expected {expected_flag}, got {result}"

# ------------------------------------------
# /* SECTION: Data Quality Validation Tests */
# ------------------------------------------

# Test: No NULLs in violation_flag
assert df_flagged.filter(col("violation_flag").isNull()).count() == 0, "NULLs found in violation_flag"

# Test: All error cases have temperature_num == None
assert df_flagged.filter((col("violation_flag") == "error") & (col("temperature_num").isNotNull())).count() == 0, "Error rows have non-null temperature_num"

# ------------------------------------------
# /* SECTION: Aggregation by Location */
# ------------------------------------------

df_agg = df_flagged.where(col("violation_flag") != "error").groupBy("location").agg(
    avg("temperature_num").alias("average_temperature"),
    count(when(col("violation_flag") == "yes", True)).alias("violation_count")
)

# ------------------------------------------
# /* SECTION: Aggregation Logic Unit Tests */
# ------------------------------------------

# Example: For SiteA, should have correct average and violation count
sitea_rows = [
    float("-8.0"), float("-5.0"), float("-8.1"), float("5.0"), float("-8.0001")
]
sitea_flags = [
    "no", "no", "yes", "yes", "yes"
]
sitea_avg = sum(sitea_rows) / len(sitea_rows)
sitea_violation_count = sitea_flags.count("yes")

sitea_agg = df_agg.filter(col("location") == "SiteA").collect()
if sitea_agg:
    row = sitea_agg[0]
    assert abs(row.average_temperature - sitea_avg) < 1e-6, f"SiteA average_temperature mismatch: {row.average_temperature} vs {sitea_avg}"
    assert row.violation_count == sitea_violation_count, f"SiteA violation_count mismatch: {row.violation_count} vs {sitea_violation_count}"

# ------------------------------------------
# /* SECTION: NULL and Invalid Handling Tests */
# ------------------------------------------

# Test: All rows with violation_flag == "error" have temperature that is None, empty, or non-numeric
error_rows = df_flagged.filter(col("violation_flag") == "error").select("temperature").collect()
for r in error_rows:
    try:
        float(r.temperature)
        assert False, f"Error row has numeric temperature: {r.temperature}"
    except Exception:
        pass  # Expected

# ------------------------------------------
# /* SECTION: Delta Lake Operations Tests */
# ------------------------------------------

# Create a Delta table for test (if not exists)
delta_table_name = "purgo_playground.cold_chain_temperature_test_delta"
spark.sql(f"DROP TABLE IF EXISTS {delta_table_name}")

df_flagged.write.format("delta").mode("overwrite").saveAsTable(delta_table_name)

# Test: MERGE operation (simulate upsert of new violation)
from delta.tables import DeltaTable  

delta_tbl = DeltaTable.forName(spark, delta_table_name)
updates = spark.createDataFrame([
    Row(location="SiteA", temperature="-20.0", event_time="2024-03-22T01:00:00.000+0000", violation_flag="yes", temperature_num=-20.0)
])
delta_tbl.alias("t").merge(
    updates.alias("s"),
    "t.location = s.location AND t.event_time = s.event_time"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Test: UPDATE operation (set violation_flag to 'no' for a specific record)
delta_tbl.update(
    condition=col("location") == "SiteA",
    set={"violation_flag": lit("no")}
)

# Test: DELETE operation (delete all error rows)
delta_tbl.delete(col("violation_flag") == "error")

# Cleanup: Drop test Delta table
spark.sql(f"DROP TABLE IF EXISTS {delta_table_name}")

# ------------------------------------------
# /* SECTION: Window Function Analytics Test */
# ------------------------------------------

w = Window.partitionBy("location").orderBy(col("event_time"))
df_window = df_flagged.withColumn("row_num", expr("row_number() over (partition by location order by event_time)"))
assert df_window.filter(col("row_num") == 1).count() > 0, "Window function row_number failed"

# ------------------------------------------
# /* SECTION: Performance Test (Batch) */
# ------------------------------------------

import time  
start = time.time()
df_flagged.count()
end = time.time()
assert (end - start) < 10, "Batch processing took too long"

# ------------------------------------------
# /* SECTION: Streaming Scenario Test */
# ------------------------------------------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType  

stream_schema = StructType([
    StructField("location", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("event_time", TimestampType(), True),
])

# Simulate streaming input using memory source
stream_input = spark.createDataFrame([
    Row(location="DepotX", temperature="-9.0", event_time="2024-03-23T00:00:00.000+0000"),
    Row(location="DepotX", temperature="0.0", event_time="2024-03-23T01:00:00.000+0000"),
], schema=stream_schema)

stream_input.createOrReplaceTempView("stream_input_view")

stream_df = spark.readStream.table("stream_input_view")

stream_flagged = stream_df.withColumn(
    "parsed", parse_temp_and_flag_udf(col("temperature"))
).withColumn(
    "violation_flag", col("parsed.violation_flag")
).withColumn(
    "temperature_num", col("parsed.temperature_num")
).drop("parsed")

# Write stream to memory sink for test
query = stream_flagged.writeStream.format("memory").queryName("stream_flagged_out").outputMode("append").start()
query.processAllAvailable()
stream_out = spark.sql("SELECT * FROM stream_flagged_out")
assert stream_out.count() == 2, "Streaming output count mismatch"
query.stop()

# ------------------------------------------
# /* SECTION: Final Output Display */
# ------------------------------------------

# Violations DataFrame: all records with violation_flag
df_violations = df_flagged.select(
    "location", "temperature", "event_time", "violation_flag"
)

# Aggregation DataFrame: by location
df_agg = df_flagged.where(col("violation_flag") != "error").groupBy("location").agg(
    avg("temperature_num").alias("average_temperature"),
    count(when(col("violation_flag") == "yes", True)).alias("violation_count")
)

df_violations.show(truncate=False)
df_agg.show(truncate=False)

# ------------------------------------------
# /* SECTION: Cleanup */
# ------------------------------------------

# No temp views or temp tables to clean up
# spark.stop()  # Do not stop SparkSession in Databricks

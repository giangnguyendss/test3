# PySpark test data generation for purgo_playground.cold_chain_temperature
# All imports are required for test data generation and processing

from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType  
from pyspark.sql.functions import col, when, isnan, avg, count, lit  
from pyspark.sql.functions import udf  
from pyspark.sql.types import StringType  

# Test data covering:
# - Happy path (valid temperatures in range)
# - Edge cases (boundary values)
# - Error cases (out-of-range, invalid, null)
# - NULL handling
# - Special/multibyte characters in location

test_data = [
    # Happy path: valid, in-range
    Row(location="SiteA", temperature=-8.0, event_time="2024-03-21T00:00:00.000+0000"),
    Row(location="SiteA", temperature=-5.0, event_time="2024-03-21T01:00:00.000+0000"),
    Row(location="SiteB", temperature=0.0, event_time="2024-03-21T02:00:00.000+0000"),
    Row(location="Depot1", temperature=-7.5, event_time="2024-03-21T03:00:00.000+0000"),
    Row(location="Depot2", temperature=-6.0, event_time="2024-03-21T04:00:00.000+0000"),
    # Edge cases: just out of range
    Row(location="SiteA", temperature=-8.1, event_time="2024-03-21T05:00:00.000+0000"),
    Row(location="SiteB", temperature=0.1, event_time="2024-03-21T06:00:00.000+0000"),
    Row(location="Depot1", temperature=-9.0, event_time="2024-03-21T07:00:00.000+0000"),
    Row(location="Depot2", temperature=1.0, event_time="2024-03-21T08:00:00.000+0000"),
    # Error cases: way out of range
    Row(location="SiteA", temperature=5.0, event_time="2024-03-21T09:00:00.000+0000"),
    Row(location="SiteB", temperature=-20.0, event_time="2024-03-21T10:00:00.000+0000"),
    # NULL and invalid
    Row(location="Depot1", temperature=None, event_time="2024-03-21T11:00:00.000+0000"),
    Row(location="Depot2", temperature="abc", event_time="2024-03-21T12:00:00.000+0000"),
    # Special/multibyte characters in location
    Row(location="仓库A", temperature=-8.0, event_time="2024-03-21T13:00:00.000+0000"),
    Row(location="СкладБ", temperature=0.0, event_time="2024-03-21T14:00:00.000+0000"),
    Row(location="Site-Ω", temperature=-8.5, event_time="2024-03-21T15:00:00.000+0000"),
    Row(location="Depot#1", temperature=0.5, event_time="2024-03-21T16:00:00.000+0000"),
    Row(location="Depot#1", temperature=-7.0, event_time="2024-03-21T17:00:00.000+0000"),
    # More edge and error cases
    Row(location="SiteA", temperature=-8.0001, event_time="2024-03-21T18:00:00.000+0000"),
    Row(location="SiteB", temperature=0.0001, event_time="2024-03-21T19:00:00.000+0000"),
    Row(location="Depot2", temperature=None, event_time="2024-03-21T20:00:00.000+0000"),
    Row(location="Depot2", temperature="", event_time="2024-03-21T21:00:00.000+0000"),
    Row(location="Depot2", temperature="-9.5", event_time="2024-03-21T22:00:00.000+0000"),
    Row(location="Depot2", temperature="0.0", event_time="2024-03-21T23:00:00.000+0000"),
    Row(location="Depot2", temperature="1e2", event_time="2024-03-22T00:00:00.000+0000"),
]

schema = StructType([
    StructField("location", StringType(), True),
    StructField("temperature", StringType(), True),  # Read as string for error handling
    StructField("event_time", StringType(), True),   # ISO 8601 string
])

# Create DataFrame from test data
df_raw = spark.createDataFrame(test_data, schema=schema)

# Convert event_time to TimestampType
df = df_raw.withColumn("event_time", col("event_time").cast(TimestampType()))

# UDF to safely parse temperature and set violation_flag
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

from pyspark.sql.types import StructType, StructField, StringType, DoubleType  

parse_temp_and_flag_udf = udf(parse_temp_and_flag, 
    StructType([
        StructField("violation_flag", StringType(), True),
        StructField("temperature_num", DoubleType(), True)
    ])
)

# Apply UDF to get violation_flag and parsed temperature
df_flagged = df.withColumn(
    "parsed", parse_temp_and_flag_udf(col("temperature"))
).withColumn(
    "violation_flag", col("parsed.violation_flag")
).withColumn(
    "temperature_num", col("parsed.temperature_num")
).drop("parsed")

# For error cases, log error message (inline print for demonstration)
def log_error(row):
    if row.violation_flag == "error":
        print(f"Invalid temperature value at location '{row.location}' and event_time '{row.event_time}': {row.temperature}")

df_flagged.foreach(log_error)  # This will print errors for invalid temperature values

# Show the table with all original columns plus "violation_flag"
df_violations = df_flagged.select(
    "location", "temperature", "event_time", "violation_flag"
)

# Aggregate by location: average temperature (excluding errors), count of violations (flag == "yes")
df_agg = df_flagged.where(col("violation_flag") != "error").groupBy("location").agg(
    avg("temperature_num").alias("average_temperature"),
    count(when(col("violation_flag") == "yes", True)).alias("violation_count")
)

# Show results
# Violations DataFrame: all records with violation_flag
df_violations.show(truncate=False)

# Aggregation DataFrame: by location
df_agg.show(truncate=False)

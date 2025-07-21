# PySpark script: Databricks month-wise client shipment aggregation with cancellation analytics
# Purpose: Aggregate shipments by client, year, and month; compute total/cancelled shipments and cancellation percentage; join with client names.
# Author: Khanh Trinh
# Date: 2025-07-21
# Description: This script calculates per-month, per-client shipment metrics from 'purgo_playground.shipments',
# joins to 'purgo_playground.clients' for client_name, and outputs columns:
# client_name, year, month, total_shipments, cancelled_shipments, cancellation_percentage.
# It strictly enforces date/data types, handles invalid shipment_date records, uses efficient Spark aggregation,
# and follows Databricks & PySpark best practices as per production-use requirements.

# -------------------------------------
# Imports
# -------------------------------------
import pyspark.sql.functions as F      
from pyspark.sql.types import (        
    IntegerType, LongType, DoubleType
)

# --------------------------------------------
# Section: Main Analytics Function Definition
# --------------------------------------------

def compute_monthly_shipment_analytics(shipments_df, clients_df):
    """
    Aggregates shipment statistics by client, year, and month
    and computes cancellation metrics. Handles invalid data.
    Args:
        shipments_df (pyspark.sql.DataFrame): Input DataFrame with schema:
           shipment_id (bigint), shipment_date (date), product_id (bigint),
           client_id (bigint), status (string), cancellation_flag (string), revenue (bigint)
        clients_df  (pyspark.sql.DataFrame): Input DataFrame with schema:
           client_id (bigint), client_name (string)
    Returns:
        pyspark.sql.DataFrame: DataFrame with columns:
           client_name (STRING, nullable), year (INT), month (INT),
           total_shipments (BIGINT), cancelled_shipments (BIGINT),
           cancellation_percentage (DOUBLE), strictly following the target schema.
    """
    # Filter out records with invalid shipment_date (must be parseable and non-null)
    cleaned_shipments = shipments_df.filter(F.col("shipment_date").cast("date").isNotNull())

    # Extract year and month (ensures type; skips null/invalid)
    ship_with_date = cleaned_shipments.withColumn(
        "year", F.year("shipment_date").cast(IntegerType())
    ).withColumn(
        "month", F.month("shipment_date").cast(IntegerType())
    )

    # Aggregate total/cancelled shipments per client/year/month
    agg = ship_with_date.groupBy("client_id", "year", "month").agg(
        F.count(F.lit(1)).cast(LongType()).alias("total_shipments"),
        F.sum(F.when(F.col("cancellation_flag") == "Yes", 1).otherwise(0)).cast(LongType()).alias("cancelled_shipments")
    )

    # Compute cancellation percentage, strictly rounded, handle zero-div with 0.0, types enforced
    agg = agg.withColumn(
        "cancellation_percentage",
        F.when(
            F.col("total_shipments") > 0,
            F.round(F.col("cancelled_shipments") * 100.0 / F.col("total_shipments"), 1)
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "cancellation_percentage", F.col("cancellation_percentage").cast(DoubleType())
    )

    # Join with client names (LEFT JOIN to allow for unmatched client_id with NULL client_name)
    joined = agg.join(
        clients_df.select(
            F.col("client_id").cast("bigint"),
            F.col("client_name").cast("string")
        ),
        on="client_id",
        how="left"
    )

    # Select and enforce final schema and ordering
    output = joined.select(
        F.col("client_name"),
        F.col("year").cast(IntegerType()),
        F.col("month").cast(IntegerType()),
        F.col("total_shipments").cast(LongType()),
        F.col("cancelled_shipments").cast(LongType()),
        F.col("cancellation_percentage").cast(DoubleType())
    )

    return output

# -----------------------------------------
# Section: DataFrame Loading and Execution
# -----------------------------------------

# Load shipments and clients table as DataFrames.
# (These will leverage Databricks Unity Catalog, so schema will auto-infer as per Delta rules.)
shipments_df = spark.table("purgo_playground.shipments")
clients_df = spark.table("purgo_playground.clients")

# Compute shipment analytics metrics as per requirements.
analytics_df = compute_monthly_shipment_analytics(shipments_df, clients_df)

# -----------------------------------------
# Section: Data Quality Checks
# -----------------------------------------

# Data quality: Ensure expected columns exist and have proper types.
expected_schema = [
    ("client_name", "string"),
    ("year", "int"),
    ("month", "int"),
    ("total_shipments", "bigint"),
    ("cancelled_shipments", "bigint"),
    ("cancellation_percentage", "double"),
]
actual_types = {f.name: f.dataType.typeName() for f in analytics_df.schema.fields}
for cname, ctype in expected_schema:
    assert cname in actual_types, f"Missing column: {cname}"
    assert actual_types[cname] == ctype, f"Type mismatch for {cname}: {actual_types[cname]} != {ctype}"

# Data quality: No negative shipment counts or percentage out of bounds (<0 or >100.0)
assert analytics_df.filter(F.col("total_shipments") < 0).count() == 0, "Negative total_shipments detected"
assert analytics_df.filter(F.col("cancelled_shipments") < 0).count() == 0, "Negative cancelled_shipments detected"
assert analytics_df.filter((F.col("cancellation_percentage") < 0.0) | (F.col("cancellation_percentage") > 100.0)).count() == 0, "Cancellation percentage out of bounds"

# -----------------------------------------
# Section: Show or Save Output
# -----------------------------------------

# Show the analytic output (limit for preview); comment out for prod environments.
analytics_df.orderBy(F.col("client_name").asc_nulls_last(), "year", "month").show(truncate=False)

# To persist output, uncomment and specify desired Delta table or Parquet path. E.g.:
# analytics_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("purgo_playground.shipment_analysis")

# End of shipment aggregation analytic script.

spark.catalog.setCurrentCatalog("purgo_databricks")

# /*
# Advanced Sales KPI Calculation and Analysis for Databricks PySpark
#
# This script calculates:
#   - Total_Sales: Total and average sales by Region and Product
#   - Sales_Trends: Quarter-over-quarter sales change by Region and Product
#   - Final_KPI: Aggregated KPIs, including top selling product flag, by joining above
#
# Source Table: purgo_playground.sales_window (Unity Catalog: purgo_databricks)
# All calculations exclude rows with nulls in Product, Region, Year, Quarter, or Sales_Amount
# "Top_Selling_Products" = top 3 products by Total_Sales_Amount per Region (all if <3)
# "Average_Sales_Amount" = mean per Region and Product group
# "Sales_Trends" uses (Year, Quarter) for unique time ordering
# All outputs are shown separately at the end
# */

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql import functions as F  
from pyspark.sql.types import LongType, DoubleType, BooleanType, StringType  
from pyspark.sql.window import Window  

# /* SECTION: Load Source Data */
# Read from Unity Catalog table (replace with actual table if needed)
sales_window_df = spark.table("purgo_playground.sales_window")

# /* SECTION: Data Cleaning and Validation */
# Exclude rows with nulls in key columns
clean_sales_window_df = sales_window_df.filter(
    F.col("Product").isNotNull() &
    F.col("Region").isNotNull() &
    F.col("Year").isNotNull() &
    F.col("Quarter").isNotNull() &
    F.col("Sales_Amount").isNotNull()
)

# /* SECTION: Data Type Validation and Error Logging */
# Validate data types and log errors for invalid types
def log_invalid_type(df, column, expected_type):
    # Only log, do not raise, to comply with requirements
    invalid_rows = df.filter(~F.col(column).cast(expected_type).isNotNull() & F.col(column).isNotNull())
    if invalid_rows.head(1):
        for row in invalid_rows.select(column).distinct().collect():
            print(f"Invalid data type in column {column}: expected {expected_type}")

log_invalid_type(clean_sales_window_df, "Sales_ID", "bigint")
log_invalid_type(clean_sales_window_df, "Product", "string")
log_invalid_type(clean_sales_window_df, "Region", "string")
log_invalid_type(clean_sales_window_df, "Year", "bigint")
log_invalid_type(clean_sales_window_df, "Quarter", "bigint")
log_invalid_type(clean_sales_window_df, "Sales_Amount", "bigint")

# /* SECTION: Total_Sales Calculation */
total_sales_df = (
    clean_sales_window_df
    .groupBy("Region", "Product")
    .agg(
        F.sum("Sales_Amount").cast(LongType()).alias("Total_Sales_Amount"),
        F.avg("Sales_Amount").cast(DoubleType()).alias("Average_Sales_Amount")
    )
    .filter(F.col("Total_Sales_Amount").isNotNull() & (F.col("Total_Sales_Amount") != 0))
)

# /* SECTION: Sales_Trends Calculation */
window_trend = Window.partitionBy("Region", "Product").orderBy(F.col("Year").asc(), F.col("Quarter").asc())
sales_trends_df = (
    clean_sales_window_df
    .withColumn("Previous_Sales_Amount", F.lag("Sales_Amount").over(window_trend))
    .withColumn(
        "Sales_Change",
        F.when(F.col("Previous_Sales_Amount").isNotNull(),
               F.col("Sales_Amount") - F.col("Previous_Sales_Amount"))
         .otherwise(F.lit(None).cast(LongType()))
    )
    .select(
        "Region", "Product", "Year", "Quarter", "Sales_Amount",
        "Previous_Sales_Amount", "Sales_Change"
    )
)

# /* SECTION: Top_Selling_Products Calculation */
window_top3 = Window.partitionBy("Region").orderBy(F.col("Total_Sales_Amount").desc())
top_selling_products_df = (
    total_sales_df
    .withColumn("rank", F.row_number().over(window_top3))
    .filter(F.col("rank") <= 3)
    .select("Region", "Product")
)

# /* SECTION: Final_KPI Aggregation */
# Join Total_Sales with Top_Selling_Products to flag Is_Top_Selling_Product
total_sales_with_flag_df = (
    total_sales_df
    .join(
        top_selling_products_df.withColumn("Is_Top_Selling_Product", F.lit(True)),
        on=["Region", "Product"],
        how="left"
    )
    .withColumn(
        "Is_Top_Selling_Product",
        F.when(F.col("Is_Top_Selling_Product").isNull(), F.lit(False)).otherwise(F.col("Is_Top_Selling_Product"))
    )
)

# Join with Sales_Trends on Region and Product
final_kpi_df = (
    total_sales_with_flag_df
    .join(
        sales_trends_df,
        on=["Region", "Product"],
        how="inner"
    )
    .select(
        F.col("Region").cast(StringType()),
        F.col("Product").cast(StringType()),
        F.col("Total_Sales_Amount").cast(LongType()),
        F.col("Average_Sales_Amount").cast(DoubleType()),
        F.col("Is_Top_Selling_Product").cast(BooleanType()),
        F.col("Year").cast(LongType()),
        F.col("Quarter").cast(LongType()),
        F.col("Sales_Amount").cast(LongType()),
        F.col("Previous_Sales_Amount").cast(LongType()),
        F.col("Sales_Change").cast(LongType())
    )
)

# /* SECTION: Output Results */
# Show Total_Sales
print("=== Total_Sales ===")
total_sales_df.orderBy("Region", "Product").show(truncate=False)

# Show Sales_Trends
print("=== Sales_Trends ===")
sales_trends_df.orderBy("Region", "Product", "Year", "Quarter").show(truncate=False)

# Show Final_KPI
print("=== Final_KPI ===")
final_kpi_df.orderBy("Region", "Product", "Year", "Quarter").show(truncate=False)

# /* End of script */

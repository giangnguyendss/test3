spark.catalog.setCurrentCatalog("purgo_databricks")

# ------------------------------------------------------------------------------------------
# Therapeutic Area Study Aggregation and Analysis
# ------------------------------------------------------------------------------------------
# This script reads from purgo_playground.study_therapeutic_analysis, aggregates by
# therapeutic_area, and writes results to purgo_playground.study_therapeutic_analysis_results.
# It enforces all requirements for data quality, error handling, and schema validation.
# ------------------------------------------------------------------------------------------

# ----------------------------------------
# Imports
# ----------------------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, LongType, DoubleType  
from pyspark.sql.utils import AnalysisException  

# ----------------------------------------
# Helper: Error handling for missing columns
# ----------------------------------------
def validate_required_columns(df, required_cols):
    """
    Raise AnalysisException if any required column is missing from DataFrame.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise AnalysisException(f"Required column(s) {missing} is missing from source table", None)

# ----------------------------------------
# Helper: Error handling for output table writability
# ----------------------------------------
def check_output_table_writable(table_name):
    """
    Try to write a single-row DataFrame to the output table in append mode.
    If not writable, raise an Exception.
    """
    from pyspark.sql import Row  
    test_df = spark.createDataFrame([Row(therapeutic_area="__test__", total_studies=0, completed_studies=0, ongoing_studies=0, avg_enrolled_subjects=0.0)])
    try:
        test_df.limit(0).write.mode("append").saveAsTable(table_name)
    except Exception as e:
        raise Exception(f"Cannot write results to output table '{table_name}': {str(e)}")

# ----------------------------------------
# Main Aggregation Logic
# ----------------------------------------
try:
    # Read source table
    src_table = "purgo_playground.study_therapeutic_analysis"
    df = spark.read.table(src_table)

    # Validate required columns
    required_cols = [
        "therapeutic_area",
        "study_title",
        "study_conduct_status",
        "enrolled_subjects_qty"
    ]
    validate_required_columns(df, required_cols)

    # Filter out records with null therapeutic_area or study_title
    df_valid = df.filter(
        F.col("therapeutic_area").isNotNull() & F.col("study_title").isNotNull()
    )

    # If no valid records, write empty result table and exit
    if df_valid.limit(1).count() == 0:
        # Prepare empty DataFrame with correct schema
        from pyspark.sql.types import StructType, StructField
        result_schema = StructType([
            StructField("therapeutic_area", StringType(), True),
            StructField("total_studies", LongType(), True),
            StructField("completed_studies", LongType(), True),
            StructField("ongoing_studies", LongType(), True),
            StructField("avg_enrolled_subjects", DoubleType(), True)
        ])
        empty_df = spark.createDataFrame([], schema=result_schema)
        empty_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("purgo_playground.study_therapeutic_analysis_results")
        # Display empty result
        empty_df.show()
    else:
        # Cast enrolled_subjects_qty to LongType for safety
        df_valid = df_valid.withColumn("enrolled_subjects_qty", F.col("enrolled_subjects_qty").cast(LongType()))

        # CTE: Aggregation per therapeutic_area
        agg_df = (
            df_valid
            .groupBy("therapeutic_area")
            .agg(
                F.countDistinct("study_title").cast(LongType()).alias("total_studies"),
                F.countDistinct(
                    F.when(
                        (F.col("study_conduct_status") == "Completed") & F.col("study_conduct_status").isNotNull(),
                        F.col("study_title")
                    )
                ).cast(LongType()).alias("completed_studies"),
                F.countDistinct(
                    F.when(
                        (F.col("study_conduct_status") == "Ongoing") & F.col("study_conduct_status").isNotNull(),
                        F.col("study_title")
                    )
                ).cast(LongType()).alias("ongoing_studies"),
                F.avg(F.col("enrolled_subjects_qty").cast(DoubleType())).alias("avg_enrolled_subjects")
            )
        )

        # Ensure output schema matches target table
        result_schema = [
            ("therapeutic_area", StringType()),
            ("total_studies", LongType()),
            ("completed_studies", LongType()),
            ("ongoing_studies", LongType()),
            ("avg_enrolled_subjects", DoubleType())
        ]
        for col_name, col_type in result_schema:
            if col_name in agg_df.columns:
                agg_df = agg_df.withColumn(col_name, F.col(col_name).cast(col_type))

        # Check output table is writable
        check_output_table_writable("purgo_playground.study_therapeutic_analysis_results")

        # Write results to output table (overwrite)
        agg_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("purgo_playground.study_therapeutic_analysis_results")

        # Display results
        agg_df.show()

except AnalysisException as ae:
    # Log and raise error for missing columns
    print(f"ERROR: {str(ae)}")
    raise

except Exception as e:
    # Log and raise error for output table or other issues
    print(f"ERROR: {str(e)}")
    raise

# ------------------------------------------------------------------------------------------
# End of Therapeutic Area Study Aggregation and Analysis
# ------------------------------------------------------------------------------------------

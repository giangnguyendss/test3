spark.catalog.setCurrentCatalog("purgo_databricks")

# /*
#  * Databricks PySpark Production Pipeline for Compound Drug Screening Analysis
#  * 
#  * This script performs:
#  *   - Data type and schema validation
#  *   - Filtering for approved and valid compound studies
#  *   - Aggregation by therapeutic_area (avg_ic50, avg_auc, avg_efficacy, total_sample_size, study_count)
#  *   - Join of filtered data with aggregation
#  *   - Calculation of overall_score (average of non-null score1-5)
#  *   - Categorization into High/Moderate/Low Potential
#  *   - Output of all source columns, aggregation columns, overall_score, and potential_category
#  * 
#  * Unity Catalog: purgo_databricks
#  * Schema: purgo_playground
#  * Source Table: purgo_playground.compound_drug_analysis
#  * Output Table: purgo_playground.compound_screening_result_analysis
#  * 
#  * No validation part is executed (per requirements)
#  */

# ---------------------------
# # Imports
# ---------------------------
from pyspark.sql import functions as F  
from pyspark.sql.types import DoubleType, LongType  

# ---------------------------
# # Utility Functions
# ---------------------------

# Function to check required columns exist in DataFrame
def assert_required_columns(df, required_cols):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise RuntimeError(f"Required column(s) missing: {', '.join(missing)}")

# Function to check data types of numeric columns
def assert_numeric_columns(df, numeric_cols):
    for col in numeric_cols:
        if col in df.columns:
            # Try casting to double and check for nulls where original is not null
            bad_type_count = df.withColumn(
                "__casted", F.col(col).cast("double")
            ).filter(
                (F.col(col).isNotNull()) & (F.col("__casted").isNull())
            ).limit(1).count()
            if bad_type_count > 0:
                raise RuntimeError(f"Invalid data type in column: {col}")

# Function to calculate overall_score (average of non-null scores)
def calc_overall_score_expr():
    # Use array, filter, and aggregate to ignore nulls
    return F.when(
        (F.col("score1").isNull()) & (F.col("score2").isNull()) & (F.col("score3").isNull()) & (F.col("score4").isNull()) & (F.col("score5").isNull()),
        F.lit(None)
    ).otherwise(
        F.expr("""
            aggregate(
                filter(array(score1, score2, score3, score4, score5), x -> x is not null),
                cast(0.0 as double),
                (acc, x) -> acc + x
            ) / size(filter(array(score1, score2, score3, score4, score5), x -> x is not null))
        """)
    )

# Function to assign potential_category based on overall_score
def calc_potential_category_expr():
    return F.when(
        (F.col("overall_score") >= 70) & (F.col("overall_score") <= 100), F.lit("High Potential")
    ).when(
        (F.col("overall_score") >= 60) & (F.col("overall_score") < 70), F.lit("Moderate Potential")
    ).otherwise(F.lit("Low Potential"))

# ---------------------------
# # Read Source Table
# ---------------------------

try:
    df_src = spark.table("purgo_playground.compound_drug_analysis")
except Exception as e:
    raise RuntimeError(f"Error reading source table: {e}")

# ---------------------------
# # Schema and Data Type Validation
# ---------------------------

required_columns = [
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name",
    "ic50", "auc", "efficacy", "toxicity", "potency", "sample_size",
    "mutation_frequency", "mutation_severity", "compound_concentration",
    "cell_viability", "growth_inhibition", "result", "approved_flag",
    "validation_status", "status", "created_by",
    "score1", "score2", "score3", "score4", "score5"
]

numeric_columns = [
    "ic50", "auc", "efficacy", "sample_size",
    "score1", "score2", "score3", "score4", "score5"
]

assert_required_columns(df_src, required_columns)
assert_numeric_columns(df_src, numeric_columns)

# ---------------------------
# # Filter for Approved and Valid Studies
# ---------------------------

df_filtered = df_src.filter(
    (F.col("approved_flag") == 1) & (F.col("validation_status") == "valid")
)

# ---------------------------
# # Aggregation by therapeutic_area
# ---------------------------

df_agg = df_filtered.groupBy("therapeutic_area").agg(
    F.avg("ic50").alias("avg_ic50"),
    F.avg("auc").alias("avg_auc"),
    F.avg("efficacy").alias("avg_efficacy"),
    F.sum("sample_size").alias("total_sample_size"),
    F.countDistinct("study_id").alias("study_count")
)

# ---------------------------
# # Join Filtered Data with Aggregation
# ---------------------------

df_joined = df_filtered.join(
    df_agg,
    on="therapeutic_area",
    how="inner"
)

# ---------------------------
# # Result Analysis: overall_score and potential_category
# ---------------------------

df_result = df_joined.withColumn(
    "overall_score", calc_overall_score_expr()
).withColumn(
    "potential_category", calc_potential_category_expr()
)

# ---------------------------
# # Output: All Source Columns + Aggregation + Result Analysis
# ---------------------------

output_columns = [
    "study_id", "compound_id", "mutation_id", "therapeutic_area", "drug_name",
    "ic50", "auc", "efficacy", "toxicity", "potency", "sample_size",
    "mutation_frequency", "mutation_severity", "compound_concentration",
    "cell_viability", "growth_inhibition", "result", "approved_flag",
    "validation_status", "status", "created_by",
    "score1", "score2", "score3", "score4", "score5",
    "avg_ic50", "avg_auc", "avg_efficacy", "total_sample_size", "study_count",
    "overall_score", "potential_category"
]

# Ensure output DataFrame has all required columns in correct order
df_final = df_result.select(*output_columns)

# ---------------------------
# # Write Output Table (Delta, Overwrite)
# ---------------------------

df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("purgo_playground.compound_screening_result_analysis")

# ---------------------------
# # Display Final Results
# ---------------------------

df_final.show(truncate=False)

# /*
#  * End of Databricks PySpark Production Pipeline for Compound Drug Screening Analysis
#  */

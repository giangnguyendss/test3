spark.catalog.setCurrentCatalog("purgo_databricks")

# =============================================================================
# Compound Drug Screening Analysis - PySpark Implementation for Databricks
# =============================================================================
# This script performs filtering, aggregation, joining, scoring, and categorization
# on compound screening analysis data from purgo_playground.compound_drug_analysis.
# It generates insights by therapeutic_area, computes overall_score, and categorizes
# results as High/Moderate/Low Potential.
# =============================================================================

# =============================================================================
# Imports (all required are included, with pip package comments)
# =============================================================================
from pyspark.sql import functions as F  
from pyspark.sql.types import DoubleType, StringType  
from pyspark.sql.window import Window  

# =============================================================================
# Section: Read Source Data
# =============================================================================
# Read from Unity Catalog table purgo_playground.compound_drug_analysis
compound_df = spark.table("purgo_playground.compound_drug_analysis")

# =============================================================================
# Section: Filter Analysis
# =============================================================================
# Only include rows where approved_flag = 1 and validation_status = 'valid'
filtered_df = compound_df.filter(
    (F.col("approved_flag") == 1) & (F.col("validation_status") == "valid")
)

# Exclude rows where therapeutic_area is null or empty string for join/aggregation
filtered_df = filtered_df.filter(
    (F.col("therapeutic_area").isNotNull()) & (F.col("therapeutic_area") != "")
)

# =============================================================================
# Section: Aggregation by therapeutic_area
# =============================================================================
# Compute average ic50, auc, efficacy (ignoring nulls), total sample_size (null as 0), and count of distinct study_id (ignoring nulls)
agg_df = (
    filtered_df
    .groupBy("therapeutic_area")
    .agg(
        F.avg("ic50").alias("avg_ic50"),
        F.avg("auc").alias("avg_auc"),
        F.avg("efficacy").alias("avg_efficacy"),
        F.sum(F.coalesce(F.col("sample_size"), F.lit(0))).alias("total_sample_size"),
        F.countDistinct(F.col("study_id")).alias("study_count")
    )
)

# =============================================================================
# Section: Join Analysis
# =============================================================================
# Inner join filtered data with aggregated data on therapeutic_area
joined_df = (
    filtered_df
    .join(agg_df, on="therapeutic_area", how="inner")
)

# =============================================================================
# Section: Result Analysis - Compute overall_score and potential_category
# =============================================================================

# Compute overall_score as average of non-null scores (score1-5); if all null, overall_score is null
joined_df = joined_df.withColumn(
    "overall_score",
    F.when(
        (F.col("score1").isNull()) &
        (F.col("score2").isNull()) &
        (F.col("score3").isNull()) &
        (F.col("score4").isNull()) &
        (F.col("score5").isNull()),
        F.lit(None).cast(DoubleType())
    ).otherwise(
        (
            F.coalesce(F.col("score1"), F.lit(0.0)) +
            F.coalesce(F.col("score2"), F.lit(0.0)) +
            F.coalesce(F.col("score3"), F.lit(0.0)) +
            F.coalesce(F.col("score4"), F.lit(0.0)) +
            F.coalesce(F.col("score5"), F.lit(0.0))
        ) /
        (
            F.when(F.col("score1").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("score2").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("score3").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("score4").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("score5").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
    )
)

# Categorize potential_category based on overall_score
joined_df = joined_df.withColumn(
    "potential_category",
    F.when(F.col("overall_score").isNull(), F.lit("Low Potential"))
     .when((F.col("overall_score") >= 70) & (F.col("overall_score") <= 100), F.lit("High Potential"))
     .when((F.col("overall_score") >= 60) & (F.col("overall_score") < 70), F.lit("Moderate Potential"))
     .otherwise(F.lit("Low Potential"))
)

# =============================================================================
# Section: Final Output
# =============================================================================
# Select all original columns plus computed columns for output
final_cols = (
    compound_df.columns +
    ["avg_ic50", "avg_auc", "avg_efficacy", "total_sample_size", "study_count", "overall_score", "potential_category"]
)

# Ensure all selected columns exist in joined_df (for schema evolution safety)
final_cols = [col for col in final_cols if col in joined_df.columns]

final_output_df = joined_df.select(*final_cols)

# Display the final result
final_output_df.show(truncate=False)

# =============================================================================
# Section: Window Analytics Example (optional, for further analytics)
# =============================================================================
# Example: Rank compounds by overall_score within each therapeutic_area
window_spec = Window.partitionBy("therapeutic_area").orderBy(F.desc("overall_score"))
ranked_df = final_output_df.withColumn("score_rank", F.rank().over(window_spec))
# ranked_df.show(truncate=False)  # Uncomment to display ranked results

# =============================================================================
# End of Script
# =============================================================================

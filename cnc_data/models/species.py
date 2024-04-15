from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
)
from pyspark.sql.window import Window


def calculate_species_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:

    # Calculate metrics for new species
    sum_species_window = Window.orderBy("first_taxon_week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    cumulative_species_df = (
        df.groupBy("first_taxon_week")
        .agg(countDistinct("taxon_id").alias("new_species"))
        .orderBy("first_taxon_week")
        .withColumn("cumulative_species", sum("new_species").over(sum_species_window))
    )

    # Calculate metrics for unique species
    unique_species_df = (
        df.groupBy("observed_week")
        .agg(countDistinct("taxon_id").alias("unique_species"))
        .withColumn("observed_week_unique_species", col("observed_week"))
        .drop("observed_week")
        .orderBy("observed_week_unique_species")
    )

    output_df = unique_species_df.join(
        cumulative_species_df,
        on=(col("observed_week") == col("first_taxon_week")),
        how="left",
    ).select(
        col("observed_week").alias("week"),
        col("new_species"),
        col("unique_species"),
        col("cumulative_species"),
    )

    return output_df

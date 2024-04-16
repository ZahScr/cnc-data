from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum, countDistinct, coalesce, lit
from cnc_data.utilities.utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_species_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark).select(col("start_of_week")).distinct()
    )

    # Calculate metrics for new species
    sum_species_window = Window.orderBy("week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    complete_dates_species_df = (
        df.groupBy("first_taxon_week")
        .agg(countDistinct("taxon_id").alias("new_species"))
        .join(
            date_dimension_df,
            on=(col("start_of_week") == col("first_taxon_week")),
            how="right",
        )
        .select(
            col("start_of_week").alias("week"),
            coalesce(col("new_species"), lit(0)).alias("new_species"),
        )
    )

    cumulative_species_df = complete_dates_species_df.orderBy("week").withColumn(
        "cumulative_species", sum("new_species").over(sum_species_window)
    )

    # Calculate metrics for unique species
    unique_species_df = df.groupBy("observed_week").agg(
        countDistinct("taxon_id").alias("unique_species")
    )

    output_df = (
        cumulative_species_df.join(
            unique_species_df,
            on=(col("week") == col("observed_week")),
            how="left",
        )
        .select(
            col("week"),
            col("new_species"),
            coalesce(col("unique_species"), lit(0)).alias("unique_species"),
            col("cumulative_species"),
        )
        .filter(col("week").between("2024-01-01", "2024-04-01"))
        .orderBy("week")
    )

    return output_df


# -------------------------------------------------------------

# from pyspark.sql import SparkSession
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.functions import (
#     col,
#     sum,
#     countDistinct,
# )
# from pyspark.sql.window import Window


# def calculate_species_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:

#     # Calculate metrics for new species
#     sum_species_window = Window.orderBy("first_taxon_week").rowsBetween(
#         Window.unboundedPreceding, Window.currentRow
#     )
#     cumulative_species_df = (
#         df.groupBy("first_taxon_week")
#         .agg(countDistinct("taxon_id").alias("new_species"))
#         .orderBy("first_taxon_week")
#         .withColumn("cumulative_species", sum("new_species").over(sum_species_window))
#     )

#     # Calculate metrics for unique species
#     unique_species_df = (
#         df.groupBy("observed_week")
#         .agg(countDistinct("taxon_id").alias("unique_species"))
#         .withColumn("observed_week_unique_species", col("observed_week"))
#         .drop("observed_week")
#         .orderBy("observed_week_unique_species")
#     )

#     output_df = unique_species_df.join(
#         cumulative_species_df,
#         on=(col("observed_week") == col("first_taxon_week")),
#         how="left",
#     ).select(
#         col("observed_week").alias("week"),
#         col("new_species"),
#         col("unique_species"),
#         col("cumulative_species"),
#     )

#     return output_df

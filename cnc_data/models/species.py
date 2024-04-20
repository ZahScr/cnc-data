from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    coalesce,
    lit,
    current_date,
    count,
    lower,
)
from cnc_data.utilities.data_utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_weekly_species_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark)
        .select(
            col("start_of_week"),
            col("start_of_week_year").alias("year"),
            col("start_of_week_week_number_reconciled").alias("week_number"),
            col("start_of_year"),
        )
        .distinct()
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
            col("year"),
            col("start_of_year"),
            col("week_number"),
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
            col("year"),
            col("start_of_year"),
            col("week_number"),
            col("new_species"),
            coalesce(col("unique_species"), lit(0)).alias("unique_species"),
            col("cumulative_species"),
        )
        .distinct()
        .filter(col("week") <= current_date())
        .orderBy("week")
    )

    return output_df


def calculate_daily_species_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark)
        .select(
            col("date"),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
        )
        .distinct()
    )

    # Calculate metrics for new species
    sum_species_window = Window.orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    complete_dates_species_df = (
        df.groupBy("first_taxon_date")
        .agg(countDistinct("taxon_id").alias("new_species"))
        .join(
            date_dimension_df,
            on=(col("date") == col("first_taxon_date")),
            how="right",
        )
        .select(
            col("date"),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            coalesce(col("new_species"), lit(0)).alias("new_species"),
        )
    )

    cumulative_species_df = complete_dates_species_df.orderBy("date").withColumn(
        "cumulative_species", sum("new_species").over(sum_species_window)
    )

    # Calculate metrics for unique species
    unique_species_df = df.groupBy("observed_date").agg(
        countDistinct("taxon_id").alias("unique_species")
    )

    output_df = (
        cumulative_species_df.join(
            unique_species_df,
            on=(col("date") == col("observed_date")),
            how="left",
        )
        .select(
            col("date"),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            col("new_species"),
            coalesce(col("unique_species"), lit(0)).alias("unique_species"),
            col("cumulative_species"),
        )
        .distinct()
        .filter(col("date") <= current_date())
        .orderBy("date")
    )

    return output_df


def calculate_weekly_metrics_for_species(
    spark: SparkSession, df: DataFrame, scientific_names: List[str]
) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark)
        .select(
            col("start_of_week"),
            col("start_of_week_year").alias("year"),
            col("start_of_week_week_number_reconciled").alias("week_number"),
            col("start_of_year"),
        )
        .distinct()
    )

    lower_case_case_scientific_names = [name.lower() for name in scientific_names]

    taxon_filtered_df = df.filter(
        lower(col("scientific_name")).isin(lower_case_case_scientific_names)
    )

    # Calculate metrics for new users
    sum_observations_window = (
        Window.partitionBy("taxon_id", "scientific_name")
        .orderBy("week")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    complete_dates_taxon_df = (
        taxon_filtered_df.groupBy(
            "observed_week", "taxon_id", "scientific_name", "common_name"
        )
        .agg(countDistinct("id").alias("observations"))
        .join(
            date_dimension_df,
            on=(col("start_of_week") == col("observed_week")),
            how="right",
        )
        .select(
            col("start_of_week").alias("week"),
            col("taxon_id"),
            col("scientific_name"),
            col("common_name"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            coalesce(col("observations"), lit(0)).alias("observations"),
        )
    )

    cumulative_taxon_df = complete_dates_taxon_df.orderBy("week").withColumn(
        "cumulative_observations", sum("observations").over(sum_observations_window)
    )

    output_df = (
        cumulative_taxon_df.select(
            col("taxon_id"),
            col("scientific_name"),
            col("common_name"),
            col("week"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            col("observations"),
            col("cumulative_observations"),
        )
        .distinct()
        .filter(col("week") <= current_date())
        .orderBy("week")
    )

    return output_df

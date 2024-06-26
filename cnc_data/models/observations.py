from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    coalesce,
    lit,
    current_date,
)
from cnc_data.utilities.data_utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_weekly_observation_metrics(
    spark: SparkSession, df: DataFrame
) -> DataFrame:
    week_dimension_df = (
        create_date_dimension(spark)
        .select(
            col("start_of_week"),
            col("start_of_week_year").alias("year"),
            col("start_of_week_week_number_reconciled").alias("week_number"),
            col("start_of_year"),
        )
        .distinct()
    )

    # Calculate metrics for observations
    sum_observation_window = Window.orderBy("week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    weekly_observations_df = (
        df.groupBy("observed_week")
        .agg(countDistinct("id").alias("unique_observations"))
        .join(
            week_dimension_df,
            on=(col("start_of_week") == col("observed_week")),
            how="right",
        )
        .select(
            col("start_of_week").alias("week"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            coalesce(col("unique_observations"), lit(0)).alias("unique_observations"),
        )
    )

    observations_df = (
        weekly_observations_df.withColumn(
            "cumulative_observations",
            sum("unique_observations").over(sum_observation_window),
        )
        .select(
            col("week"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            col("unique_observations"),
            col("cumulative_observations"),
        )
        .distinct()
        .filter(col("week") <= current_date())
        .orderBy("week")
    )

    return observations_df


def calculate_observation_metrics(
    spark: SparkSession,
    df: DataFrame,
    agg_column: str = "date",
    yoy_column: str = "day_of_year",
) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark)
        .select(
            col(agg_column),
            col("year"),
            col(yoy_column),
            col("start_of_year"),
        )
        .distinct()
    )

    # Calculate metrics for observations
    sum_observation_window = Window.orderBy(agg_column).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    weekly_observations_df = (
        df.groupBy("observed_date")
        .agg(countDistinct("id").alias("unique_observations"))
        .join(
            date_dimension_df,
            on=(col(agg_column) == col("observed_date")),
            how="right",
        )
        .select(
            col(agg_column),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            coalesce(col("unique_observations"), lit(0)).alias("unique_observations"),
        )
    )

    observations_df = (
        weekly_observations_df.withColumn(
            "cumulative_observations",
            sum("unique_observations").over(sum_observation_window),
        )
        .select(
            col(agg_column),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            col("unique_observations"),
            col("cumulative_observations"),
        )
        .distinct()
        .filter(col(agg_column) <= current_date())
        .orderBy(agg_column)
    )

    return observations_df

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    coalesce,
    lit,
)
from cnc_data.utilities.utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_observation_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    week_dimension_df = (
        create_date_dimension(spark)
        .select(col("start_of_week"), col("year"))
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
            col("unique_observations"),
            col("cumulative_observations"),
        )
        .orderBy("week")
    )

    return observations_df
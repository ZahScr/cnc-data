from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
)
from pyspark.sql.window import Window


def calculate_observation_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Calculate metrics for observations
    sum_obs_window = Window.orderBy("observed_week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    observations_df = (
        df.groupBy("observed_week")
        .agg(countDistinct("id").alias("unique_observations"))
        .orderBy("observed_week")
        .withColumn(
            "cumulative_observations", sum("unique_observations").over(sum_obs_window)
        )
        .select(
            col("observed_week").alias("week"),
            col("unique_observations"),
            col("cumulative_observations"),
        )
    )

    return observations_df

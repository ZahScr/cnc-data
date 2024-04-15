from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
)
from pyspark.sql.window import Window


def calculate_user_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:

    # Calculate metrics for new users
    sum_user_window = Window.orderBy("first_user_week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    cumulative_user_df = (
        df.groupBy("first_user_week")
        .agg(countDistinct("user_id").alias("new_users"))
        .orderBy("first_user_week")
        .withColumn("cumulative_users", sum("new_users").over(sum_user_window))
    )

    # Calculate metrics for unique users
    unique_users_df = df.groupBy("observed_week").agg(
        countDistinct("user_id").alias("unique_users")
    )

    output_df = (
        unique_users_df.join(
            cumulative_user_df,
            on=(col("observed_week") == col("first_user_week")),
            how="left",
        )
        .select(
            col("observed_week").alias("week"),
            col("new_users"),
            col("unique_users"),
            col("cumulative_users"),
        )
        .orderBy("week")
    )

    return output_df

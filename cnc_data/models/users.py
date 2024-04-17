from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum, countDistinct, coalesce, lit, current_date
from cnc_data.utilities.data_utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_user_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    date_dimension_df = (
        create_date_dimension(spark)
        .select(
            col("start_of_week"), col("year"), col("week_number"), col("start_of_year")
        )
        .distinct()
    )

    # Calculate metrics for new users
    sum_user_window = Window.orderBy("week").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    complete_dates_user_df = (
        df.groupBy("first_user_week")
        .agg(countDistinct("user_id").alias("new_users"))
        .join(
            date_dimension_df,
            on=(col("start_of_week") == col("first_user_week")),
            how="right",
        )
        .select(
            col("start_of_week").alias("week"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            coalesce(col("new_users"), lit(0)).alias("new_users"),
        )
    )

    cumulative_user_df = complete_dates_user_df.orderBy("week").withColumn(
        "cumulative_users", sum("new_users").over(sum_user_window)
    )

    # Calculate metrics for unique users
    unique_users_df = df.groupBy("observed_week").agg(
        countDistinct("user_id").alias("unique_users")
    )

    output_df = (
        cumulative_user_df.join(
            unique_users_df,
            on=(col("week") == col("observed_week")),
            how="left",
        )
        .select(
            col("week"),
            col("year"),
            col("start_of_year"),
            col("week_number"),
            col("new_users"),
            coalesce(col("unique_users"), lit(0)).alias("unique_users"),
            col("cumulative_users"),
        )
        .distinct()
        .filter(col("week") <= current_date())
        .orderBy("week")
    )

    return output_df

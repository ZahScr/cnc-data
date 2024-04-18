from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, sum, countDistinct, coalesce, lit, current_date
from cnc_data.utilities.data_utils import create_date_dimension
from pyspark.sql.window import Window


def calculate_weekly_user_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
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


def calculate_daily_user_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
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

    # Calculate metrics for new users
    sum_user_window = Window.orderBy("date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    complete_dates_user_df = (
        df.groupBy("first_user_date")
        .agg(countDistinct("user_id").alias("new_users"))
        .join(
            date_dimension_df,
            on=(col("date") == col("first_user_date")),
            how="right",
        )
        .select(
            col("date"),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            coalesce(col("new_users"), lit(0)).alias("new_users"),
        )
    )

    cumulative_user_df = complete_dates_user_df.orderBy("date").withColumn(
        "cumulative_users", sum("new_users").over(sum_user_window)
    )

    # Calculate metrics for unique users
    unique_users_df = df.groupBy("observed_date").agg(
        countDistinct("user_id").alias("unique_users")
    )

    output_df = (
        cumulative_user_df.join(
            unique_users_df,
            on=(col("date") == col("observed_date")),
            how="left",
        )
        .select(
            col("date"),
            col("year"),
            col("day_of_year"),
            col("start_of_year"),
            col("new_users"),
            coalesce(col("unique_users"), lit(0)).alias("unique_users"),
            col("cumulative_users"),
        )
        .distinct()
        .filter(col("date") <= current_date())
        .orderBy("date")
    )

    return output_df

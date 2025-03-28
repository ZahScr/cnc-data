import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql.functions import col, first_value, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    date_trunc,
    min,
    date_format,
    year,
    month,
    dayofmonth,
    quarter,
    weekofyear,
    dayofyear,
    when,
    coalesce,
)
from datetime import datetime, timedelta


def get_cnc_events():
    return {
        "2019": ["2019-04-26", "2019-04-29"],
        "2020": ["2020-04-24", "2020-04-27"],
        "2021": ["2021-04-30", "2021-05-03"],
        "2022": ["2022-04-29", "2022-05-02"],
        "2023": ["2023-04-28", "2023-05-01"],
        "2024": ["2024-04-25", "2024-04-29"],
    }


def date_trunc_week(date):
    dt = datetime.strptime(date, "%Y-%m-%d").date()

    week_start = dt - timedelta(days=dt.weekday())
    week_end = week_start + timedelta(days=6)
    return [week_start, week_end]


# This function is used to transform the raw data into a format that can be used for analysis
def transform_for_metrics(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = (
        df.withColumnRenamed("observed_on", "observed_date")
        .withColumn("observed_week", date_trunc("week", "observed_date").cast("date"))
    )

    first_user_week_observations = df.groupBy("user_id").agg(
        min("observed_week").alias("first_user_week")
    )

    first_user_date_observations = df.groupBy("user_id").agg(
        min("observed_date").alias("first_user_date")
    )

    first_taxon_week_observation = df.groupBy("taxon_id").agg(
        min("observed_week").alias("first_taxon_week")
    )

    first_taxon_date_observation = df.groupBy("taxon_id").agg(
        min("observed_date").alias("first_taxon_date")
    )

    df = (
        df.join(first_user_week_observations, on="user_id", how="left")
        .join(first_taxon_week_observation, on="taxon_id", how="left")
        .join(first_user_date_observations, on="user_id", how="left")
        .join(first_taxon_date_observation, on="taxon_id", how="left")
        .select(
            "observed_date",
            "observed_week",
            coalesce(
                col("time_observed_at"), col("observed_date").cast("timestamp")
            ).alias("time_observed_at"),
            "id",
            "taxon_id",
            "user_id",
            "common_name",
            "scientific_name",
            "first_taxon_date",
            "first_taxon_week",
            "first_user_date",
            "first_user_week",
        )
    )

    return df


# This function is used to load the raw CNC data into a Spark dataframe
def load_raw_data(spark: SparkSession) -> DataFrame:
    # Paths to the CSV files
    directory_path = "cnc_data/raw/"

    # Get list of file names (can filter by extension just in case)
    file_list = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith(".csv")]

    # Initialize an empty DataFrame
    final_df = None

    # Loop through files and union them
    for idx, file_path in enumerate(file_list):
        print(f"Loading file {idx + 1} from path: {file_path}")
        
        df = spark.read.option("header", "true").csv(file_path)

        df = df.select(
            col("id"),
            col("uuid"),
            col("observed_on"),
            col("time_observed_at"),
            col("user_id"),
            col("created_at"),
            col("scientific_name"),
            col("common_name"),
            col("taxon_id"),
            col("taxon_kingdom_name"),
            col("taxon_species_name"),
        )

        # Validate that required columns are present
        required_columns = [
            "id",
            "uuid",
            "observed_on",
            "time_observed_at",
            "user_id",
            "created_at",
            # "latitude",
            # "longitude",
            "scientific_name",
            "common_name",
            "taxon_id",
            "taxon_kingdom_name",
            "taxon_species_name",
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        print(f"Loaded {df.count()} rows from {file_path}")

        if final_df is None:
            final_df = df
        else:
            final_df = final_df.unionByName(df)

    print(f"Loaded total {final_df.count()} rows from {len(file_list)} files.")

    # Select relevant columns, drop na
    final_df = (
        final_df.select(
            col("id"),
            col("observed_on"),
            col("time_observed_at"),
            col("uuid"),
            col("taxon_id"),
            col("taxon_kingdom_name"),
            col("taxon_species_name"),
            col("user_id"),
            col("common_name"),
            col("scientific_name"),
            col("created_at"),
        )
    )

    # Print the column names
    # print("Loaded observations dataset with columns:")
    # for name in df.columns:
    #     print(name)

    # Count the total number of rows
    print(f"Total rows raw loaded: {final_df.count()}")

    # df.offset(1000).show(500)

    transformed_df = transform_for_metrics(spark, final_df)

    print(f"Total rows post-transform: {transformed_df.count()}")

    return transformed_df


def create_date_dimension(spark, starting_year=2015, ending_year=2025):
    # Create a DataFrame with a single column named "date", containing dates from 2010-01-01 to 2030-12-31
    date_df = spark.range(0, (ending_year + 1 - starting_year) * 365, 1).selectExpr(
        f"date_add('{starting_year}-01-01', cast(id as int)) as date"
    )

    # Add the required columns to the DataFrame
    date_dimension_df = (
        date_df.select(
            col("date"),
            date_trunc("week", col("date")).cast("date").alias("start_of_week"),
            date_trunc("year", col("date")).cast("date").alias("start_of_year"),
            year("date").alias("year"),
            year("start_of_week").alias("start_of_week_year"),
            month("date").alias("month"),
            month("start_of_week").alias("start_of_week_month"),
            date_format(col("date"), "MMMM").alias("month_name"),
            date_format(col("start_of_week"), "MMMM").alias("start_of_week_month_name"),
            dayofmonth("date").alias("day"),
            date_format(col("date"), "EEEE").alias("day_name"),
            quarter("date").alias("quarter"),
            dayofyear("date").alias("day_of_year"),
            weekofyear("date").alias("week_number"),
            weekofyear("start_of_week").alias("start_of_week_week_number"),
        )
        .withColumn(
            "start_of_week_week_number_reconciled",
            when(
                (col("start_of_week_year") == 2019) & (col("start_of_week_month") == 12)
                # & (col("month") == 1)
                & (
                    (col("week_number") == 1)
                    | (col("day_of_year").cast("int").isin(364, 365))
                ),
                53,
            ).otherwise(col("week_number")),
        )
        .select(
            "date",
            "start_of_week",
            "start_of_year",
            "year",
            "start_of_week_year",
            "month",
            "start_of_week_month",
            "month_name",
            "start_of_week_month_name",
            "day",
            "day_name",
            "quarter",
            "day_of_year",
            "week_number",
            "start_of_week_week_number",
            "start_of_week_week_number_reconciled",
        )
    )

    return date_dimension_df


def load_metrics_data(spark: SparkSession, period: str = "weekly") -> List[DataFrame]:
    # Load CNC metrics data
    users_df = spark.read.csv(f"cnc_data/output/{period}_users", header=True)
    species_df = spark.read.csv(f"cnc_data/output/{period}_species", header=True)
    observations_df = spark.read.csv(
        f"cnc_data/output/{period}_observations", header=True
    )
    by_species = spark.read.csv(f"cnc_data/output/{period}_by_species", header=True)

    return [users_df, species_df, observations_df, by_species]


def adjust_yearly_cumulative_observations(
    df: DataFrame,
    period_column_name: str = "week",
    yoy_x_column_name: str = "week_number",
    partition_columns: List[str] = ["year", "taxon_id"],
) -> DataFrame:
    year_window = Window.partitionBy(partition_columns).orderBy(col(period_column_name))
    yoy_column_window = Window.partitionBy(
        *[[yoy_x_column_name] + partition_columns]
    ).orderBy(col(period_column_name))

    df = df.withColumn(
        f"starting_cumulative_observations",
        first_value(f"cumulative_observations").over(year_window),
    ).withColumn(
        f"year_adjusted_cumulative_observations",
        (
            col(f"cumulative_observations") - col(f"starting_cumulative_observations")
        ).cast("int"),
    )

    transformed_df = (
        df.distinct()
        .withColumn(
            "row_number",
            row_number().over(yoy_column_window),
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
        .orderBy(period_column_name)
    )

    return transformed_df


def transform_for_weekly_cumulative_year_chart(
    users_df: DataFrame,
    species_df: DataFrame,
    observations_df: DataFrame,
    period_column_name: str = "week",
    yoy_column_name: str = "week_number",
) -> DataFrame:
    # Calculate metrics for new species
    year_window = Window.partitionBy("year").orderBy(col(period_column_name))
    yoy_column_window = Window.partitionBy(yoy_column_name, "year").orderBy(
        col(period_column_name)
    )

    transformed_df = (
        users_df.alias("users")
        .join(species_df, on=period_column_name, how="left")
        .join(
            observations_df,
            on=period_column_name,
            how="left",
        )
        .selectExpr(
            f"users.{period_column_name}",
            "users.year",
            f"users.{yoy_column_name}",
            "users.start_of_year",
            "cast(new_users as int)",
            "cast(unique_users as int)",
            "cast(cumulative_users as int)",
            "cast(new_species as int)",
            "cast(unique_species as int)",
            "cast(cumulative_species as int)",
            "cast(unique_observations as int)",
            "cast(cumulative_observations as int)",
        )
        .withColumn(
            "starting_cumulative_users",
            first_value("cumulative_users").over(year_window),
        )
        .withColumn(
            "starting_cumulative_species",
            first_value("cumulative_species").over(year_window),
        )
        .withColumn(
            "starting_cumulative_observations",
            first_value("cumulative_observations").over(year_window),
        )
        .withColumn(
            "year_adjusted_cumulative_users",
            (col("cumulative_users") - col("starting_cumulative_users")).cast("int"),
        )
        .withColumn(
            "year_adjusted_cumulative_species",
            col("cumulative_species") - col("starting_cumulative_species"),
        )
        .withColumn(
            "year_adjusted_cumulative_observations",
            col("cumulative_observations") - col("starting_cumulative_observations"),
        )
        .distinct()
        .withColumn(
            "row_number",
            row_number().over(yoy_column_window),
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
        .orderBy(period_column_name)
    )

    return transformed_df

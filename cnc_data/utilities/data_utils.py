from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
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
)
from datetime import datetime, timedelta


def get_cnc_events():
    return {
        "2019": ["2019-04-26", "2019-04-29"],
        "2020": ["2020-04-24", "2020-04-27"],
        "2021": ["2021-04-30", "2021-05-03"],
        "2022": ["2022-04-29", "2022-05-02"],
        "2023": ["2023-04-29", "2023-05-02"],
        "2024": ["2024-04-29", "2024-05-02"],
    }


def date_trunc_week(date):
    # dt = datetime.strptime(date, "%d/%b/%Y")
    dt = datetime.strptime(date, "%Y-%m-%d").date()

    week_start = dt - timedelta(days=dt.weekday())
    week_end = week_start + timedelta(days=6)
    return [week_start, week_end]


# This function is used to transform the raw data into a format that can be used for analysis
def transform(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = (
        df.filter(col("observed_on") >= "2015-01-01")
        .withColumn("observed_week", date_trunc("week", "observed_on").cast("date"))
        .na.drop()
    )

    first_user_observations = df.groupBy("user_id").agg(
        min("observed_week").alias("first_user_week")
    )

    first_taxon_observation = df.groupBy("taxon_id").agg(
        min("observed_week").alias("first_taxon_week")
    )

    df = (
        df.join(first_user_observations, on="user_id", how="left")
        .join(first_taxon_observation, on="taxon_id", how="left")
        .select(
            "observed_on",
            "observed_week",
            "time_observed_at",
            "id",
            "taxon_id",
            "user_id",
            "common_name",
            "scientific_name",
            "first_taxon_week",
            "first_user_week",
        )
    )

    return df


# This function is used to load the raw CNC data into a Spark dataframe
def load_cnc_data(spark: SparkSession) -> DataFrame:
    # Define the paths to the CSV files
    csv_path1 = "cnc_data/raw/observations-420624.csv"
    csv_path2 = "cnc_data/raw/observations-420636.csv"
    csv_path3 = "cnc_data/raw/observations-420647.csv"

    # Read the CSV files into Spark dataframes
    df1 = (
        spark.read.format("csv")
        .option("header", "true")
        .load(csv_path1)
        .select(
            col("observed_on"),
            col("time_observed_at"),
            col("id"),
            col("taxon_id"),
            col("user_id"),
            col("common_name"),
            col("scientific_name"),
        )
    )
    df2 = (
        spark.read.format("csv")
        .option("header", "true")
        .load(csv_path2)
        .select(
            col("observed_on"),
            col("time_observed_at"),
            col("id"),
            col("taxon_id"),
            col("user_id"),
            col("common_name"),
            col("scientific_name"),
        )
    )
    df3 = (
        spark.read.format("csv")
        .option("header", "true")
        .load(csv_path3)
        .select(
            col("observed_on"),
            col("time_observed_at"),
            col("id"),
            col("taxon_id"),
            col("user_id"),
            col("common_name"),
            col("scientific_name"),
        )
    )

    # Concatenate the two dataframes vertically
    df = df1.unionAll(df2).unionAll(df3)

    # Print the column names
    # print("Loaded observations dataset with columns:")
    # for name in df.columns:
    #     print(name)

    # Count the total number of rows
    print(f"Total rows raw loaded: {df.count()}")

    # Select the specified columns and drop one nulls
    df = df.select(
        "observed_on",
        "time_observed_at",
        "id",
        "taxon_id",
        "user_id",
        # "taxon_kingdom_name",
        "common_name",
        "scientific_name",
    )

    df = transform(spark, df)

    # df.show()

    print(f"Total rows cleaned: {df.count()}")

    return df


def create_date_dimension(spark, starting_year=2015, ending_year=2025):
    # Create a DataFrame with a single column named "date", containing dates from 2010-01-01 to 2030-12-31
    date_df = spark.range(0, (ending_year + 1 - starting_year) * 365, 1).selectExpr(
        f"date_add('{starting_year}-01-01', cast(id as int)) as date"
    )

    # Add the required columns to the DataFrame
    date_dimension_df = date_df.select(
        col("date"),
        date_trunc("week", col("date")).cast("date").alias("start_of_week"),
        date_trunc("year", col("date")).cast("date").alias("start_of_year"),
        year("date").alias("year"),
        month("date").alias("month"),
        date_format(col("date"), "MMMM").alias("month_name"),
        dayofmonth("date").alias("day"),
        date_format(col("date"), "EEEE").alias("day_name"),
        quarter("date").alias("quarter"),
        weekofyear("date").alias("week_number"),
    )

    return date_dimension_df


def load_metrics_data(spark: SparkSession) -> list[DataFrame]:
    # Load CNC metrics data
    users_df = spark.read.csv("cnc_data/output/users", header=True)
    species_df = spark.read.csv("cnc_data/output/species", header=True)
    observations_df = spark.read.csv("cnc_data/output/observations", header=True)

    return [users_df, species_df, observations_df]


def transform_for_yearly_cumulative_chart(
    users_df: DataFrame, species_df: DataFrame, observations_df: DataFrame
) -> DataFrame:
    # Calculate metrics for new species
    year_window = Window.partitionBy("year").orderBy("week")
    week_number_window = Window.partitionBy("week_number", "year").orderBy("week")

    transformed_df = (
        users_df.alias("users")
        .join(species_df, on="week", how="left")
        .join(
            observations_df,
            on="week",
            how="left",
        )
        .selectExpr(
            "users.week",
            "users.year",
            "users.week_number",
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
            row_number().over(week_number_window),
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
        .orderBy("week")
    )

    return transformed_df

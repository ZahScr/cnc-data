import plotly.graph_objs as go
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
import os


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


def export_new_objects_yearly_chart(
    df: DataFrame, metric_object: str, metric_type=str, filetype="png", cnc_events=None
):
    years_colors = [
        [2015, "#9e0142"],
        [2016, "#d53e4f"],
        [2017, "#f46d43"],
        [2018, "#fdae61"],
        [2019, "#fee08b"],
        [2020, "#e6f598"],
        [2021, "#abdda4"],
        [2022, "#66c2a5"],
        [2023, "#3288bd"],
        [2024, "#5e4fa2"],
    ]

    df_pd = df.toPandas()
    df_pd["week"] = df_pd["week"].astype("datetime64[ns]")
    df_pd["year"] = df_pd["year"].astype("int")

    # series_type, category = map(lambda x: x.capitalize(), column.split("_"))
    title = f"Calgary {metric_type.capitalize()} iNaturalist {metric_object.capitalize()} by Week"
    y_series_name = f"{metric_type}_{metric_object}"
    y_title = f"{metric_type.capitalize()} {metric_object.capitalize()}"
    x_title = "Observation Week"

    fig = go.Figure()

    for item in years_colors:
        year = item[0]
        color = item[1]
        year_data = df_pd[df_pd["year"] == year]

        if not year_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=year_data["week_number"],
                    y=year_data[y_series_name],
                    mode="lines",
                    line=dict(color=color),
                    name=str(year),
                    # connectgaps=False,
                )
            )

        # Create the scatter plot
        # fig.add_trace(
        #     go.Scatter(
        #         x=x_series,
        #         y=y_series,
        #         mode="lines",
        #         fill="tozeroy",
        #         line=dict(color="#07874B"),
        #     )
        # )

    # Set the chart title and axes labels
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(color="black", family="Basic Sans"),
        ),
        xaxis_title=x_title,
        yaxis_title=y_title,
        font=dict(family="Noto Sans"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title_font=dict(family="Basic Sans", size=24),
        xaxis=dict(
            title_font=dict(family="Noto Sans", size=18),
            color="black",
            # tickvals=x_series[::52],  # display every year
            # ticktext=x_series.dt.year[::52].astype(str),  # display year as string
        ),
        yaxis=dict(
            title_font=dict(family="Noto Sans", size=18), color="black", autorange=True
        ),
    )

    if cnc_events:
        for event in cnc_events:
            start_date, end_date = cnc_events[event]
            event_start_week, _ = date_trunc_week(start_date)
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            event_title = f"CNC {event}"
            fig.add_vrect(
                x0=event_start_week,
                x1=end_date_dt,
                annotation_text=event_title,
                annotation_position="top left",
                annotation=dict(font_size=14, font_family="Noto Sans"),
                fillcolor="purple",
                opacity=0.25,
                line_width=0,
            )

    if not os.path.exists("images"):
        os.mkdir("images")

    filename = "-".join(title.lower().split(" "))

    fig.write_image(f"images/{filename}.{filetype}")

    # # Show the plot
    # fig.show()


def export_cumulative_yearly_chart(
    df: DataFrame,
    metric_object: str,
    filetype="png",
):
    years_colors = [
        [2015, "#9e0142"],
        [2016, "#d53e4f"],
        [2017, "#f46d43"],
        [2018, "#fdae61"],
        [2019, "#fee08b"],
        [2020, "#e6f598"],
        [2021, "#abdda4"],
        [2022, "#66c2a5"],
        [2023, "#3288bd"],
        [2024, "#5e4fa2"],
    ]

    df_pd = df.toPandas()
    df_pd["week"] = df_pd["week"].astype("datetime64[ns]")
    df_pd["year"] = df_pd["year"].astype("int")

    # series_type, category = map(lambda x: x.capitalize(), column.split("_"))
    title = f"Calgary Cumulative New iNaturalist {metric_object.capitalize()} by Week"
    y_series_name = f"year_adjusted_cumulative_{metric_object}"
    y_title = f"Cumulative New {metric_object.capitalize()}"
    x_title = "Observation Week"

    # Create the scatter plot
    fig = go.Figure()

    for item in years_colors:
        year = item[0]
        color = item[1]
        year_data = df_pd[df_pd["year"] == year]

        if not year_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=year_data["week_number"],
                    y=year_data[y_series_name],
                    mode="lines",
                    line=dict(color=color),
                    name=str(year),
                    connectgaps=False,
                )
            )

    # Set the chart title and axes labels
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(color="black", family="Basic Sans"),
        ),
        xaxis_title=x_title,
        yaxis_title=y_title,
        font=dict(family="Noto Sans"),
        paper_bgcolor="#ffffff",
        plot_bgcolor="rgba(0,0,0,0)",
        title_font=dict(family="Basic Sans", size=24),
        xaxis=dict(
            title_font=dict(family="Noto Sans", size=18),
            color="black",
        ),
        yaxis=dict(title_font=dict(family="Noto Sans", size=18), color="black"),
    )

    if not os.path.exists("images"):
        os.mkdir("images")

    filename = "-".join(title.lower().split(" "))

    fig.write_image(f"images/{filename}.{filetype}")

    # # Show the plot
    # fig.show()


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


# This function is used to transform the final dataframe to a Pandas dataframe for plotting
def transform_for_chart(
    spark: SparkSession, df: DataFrame, columns: list[str]
) -> DataFrame:
    # Convert the Spark dataframe to a Pandas dataframe
    df_pd = df.toPandas()

    df_pd["week"] = df_pd["observed_week"].astype("datetime64[ns]")

    # y_series_columns = list(
    #     filter(
    #         lambda x: (x != "observed_week" and x != "observed_week_ts"),
    #         df_pd.columns.values.tolist(),
    #     )
    # )

    return df_pd, y_series_columns


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

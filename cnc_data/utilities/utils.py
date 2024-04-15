import plotly.graph_objs as go
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import to_date, date_trunc, col, min
from datetime import datetime, timedelta
import os


def get_cnc_events():
    return {
        "2019": ["2019-04-26", "2019-04-29"],
        "2020": ["2020-04-24", "2020-04-27"],
        "2021": ["2021-04-30", "2021-05-03"],
        "2022": ["2022-04-29", "2022-05-02"],
    }


def date_trunc_week(date):
    # dt = datetime.strptime(date, "%d/%b/%Y")
    dt = datetime.strptime(date, "%Y-%m-%d").date()

    week_start = dt - timedelta(days=dt.weekday())
    week_end = week_start + timedelta(days=6)
    return [week_start, week_end]


def export_chart(
    x_series, y_series, title, x_title, y_title, filetype="png", cnc_events=None
):
    # Create the scatter plot
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=x_series,
            y=y_series,
            mode="lines",
            fill="tozeroy",
            line=dict(color="#07874B"),
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
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title_font=dict(family="Basic Sans", size=24),
        xaxis=dict(
            title_font=dict(family="Noto Sans", size=18),
            color="black",
            tickvals=x_series[::52],  # display every year
            ticktext=x_series.dt.year[::52].astype(str),  # display year as string
        ),
        yaxis=dict(title_font=dict(family="Noto Sans", size=18), color="black"),
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


# This function is used to transform the raw data into a format that can be used for analysis
def transform(spark: SparkSession, df: DataFrame) -> DataFrame:
    df = (
        df.filter(col("observed_on") >= "2015-01-01")
        .withColumn("observed_week", to_date(date_trunc("week", "observed_on")))
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
    csv_path1 = "cnc_data/raw/observations-306591.csv"
    csv_path2 = "cnc_data/raw/observations-306599.csv"

    # Read the CSV files into Spark dataframes
    df1 = spark.read.format("csv").option("header", "true").load(csv_path1)
    df2 = spark.read.format("csv").option("header", "true").load(csv_path2)

    # Concatenate the two dataframes vertically
    df = df1.union(df2)

    # Print the column names
    print("Loaded observations dataset with columns:")
    for name in df.columns:
        print(name)

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
def transform_for_chart(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Convert the Spark dataframe to a Pandas dataframe
    df_pd = df.toPandas()

    df_pd["observed_week_ts"] = df_pd["observed_week"].astype("datetime64[ns]")

    y_series_columns = list(
        filter(
            lambda x: (x != "observed_week" and x != "observed_week_ts"),
            df_pd.columns.values.tolist(),
        )
    )

    return df_pd, y_series_columns


from pyspark.sql.functions import (
    col,
    date_format,
    year,
    month,
    dayofmonth,
    quarter,
    weekofyear,
)


def create_date_dimension(spark, starting_year=2010, ending_year=2030):
    # Create a DataFrame with a single column named "date", containing dates from 2010-01-01 to 2030-12-31
    date_df = spark.range(0, (ending_year + 1 - starting_year) * 365, 1).selectExpr(
        f"date_add('{starting_year}-01-01', cast(id as int)) as date"
    )

    # Add the required columns to the DataFrame
    date_dimension_df = date_df.select(
        col("date"),
        year("date").alias("year"),
        month("date").alias("month"),
        date_format(col("date"), "MMMM").alias("month_name"),
        dayofmonth("date").alias("day"),
        date_format(col("date"), "EEEE").alias("day_name"),
        quarter("date").alias("quarter"),
        weekofyear("date").alias("week"),
    )

    return date_dimension_df

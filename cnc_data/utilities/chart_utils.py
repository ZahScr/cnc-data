import plotly.graph_objs as go
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from datetime import datetime
import os


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

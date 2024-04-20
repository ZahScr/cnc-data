import plotly.graph_objs as go
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
from cnc_data.utilities.data_utils import date_trunc_week
import os

year_color_map = [
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


def write_image(fig, object_path_name: str, filename: str, filetype: str = "png"):
    if not os.path.exists(f"images/{object_path_name}"):
        os.mkdir(f"images/{object_path_name}")

    fig.write_image(f"images/{object_path_name}/{filename}.{filetype}")


# This function is used to transform the final dataframe to a Pandas dataframe for plotting
def transform_for_chart(df: DataFrame, x_column: str) -> DataFrame:
    # Convert the Spark dataframe to a Pandas dataframe
    df_pd = df.toPandas()

    if x_column in ("week", "date"):
        df_pd[x_column] = df_pd[x_column].astype("datetime64[ns]")
    elif x_column in ("week_number", "day_of_year"):
        df_pd[x_column] = df_pd[x_column].astype("int")

    df_pd["year"] = df_pd["year"].astype("int")

    # TODO: Clean this up
    # y_series_columns = list(
    #     filter(
    #         lambda x: (x != "observed_week" and x != "observed_week_ts"),
    #         df_pd.columns.values.tolist(),
    #     )
    # )

    return df_pd


def export_new_objects_yearly_chart(
    df: DataFrame,
    metric_object: str,
    metric_type: str,
    period_name: str,
    x_column: str,
    y_column: str,
    filetype="png",
    cnc_events=None,
):
    df_pd = transform_for_chart(df, x_column)

    # series_type, category = map(lambda x: x.capitalize(), column.split("_"))
    title = f"Calgary {metric_type.capitalize()} iNaturalist {metric_object.capitalize()} by {period_name.capitalize()}"
    y_title = f"{metric_type.capitalize()} {metric_object.capitalize()}"
    x_title = f"Observation {period_name.capitalize()}"

    fig = go.Figure()

    for item in year_color_map:
        year = item[0]
        color = item[1]
        year_data = df_pd[df_pd["year"] == year]

        if not year_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=year_data[x_column],
                    y=year_data[y_column],
                    mode="lines",
                    line=dict(color=color),
                    name=str(year),
                    # connectgaps=False,
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
            showline=True,  # Show the x-axis line
            linewidth=2,  # Set the x-axis line width
            linecolor="black",  # Set the x-axis line color
            # tickvals=x_series[::52],  # display every year
            # ticktext=x_series.dt.year[::52].astype(str),  # display year as string
        ),
        yaxis=dict(
            title_font=dict(family="Noto Sans", size=18),
            color="black",
            autorange=True,
            showline=True,  # Show the x-axis line
            linewidth=2,  # Set the x-axis line width
            linecolor="black",  # Set the x-axis line color
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

    object_path_name = "-".join(metric_object.lower().split(" "))
    filename = "-".join(title.lower().split(" "))

    write_image(fig, object_path_name, filename, filetype)

    # # Show the plot
    # fig.show()


def export_cumulative_yearly_chart(
    df: DataFrame,
    metric_object: str,
    period_name: str,
    x_column: str,
    y_column: str,
    filetype="png",
):
    df_pd = transform_for_chart(df, x_column)

    # series_type, category = map(lambda x: x.capitalize(), column.split("_"))
    title = f"Calgary Cumulative New iNaturalist {metric_object.capitalize()} by {period_name.capitalize()}"
    y_title = f"Cumulative New {metric_object.capitalize()}"
    x_title = f"Observation {period_name.capitalize()}"

    # Create the scatter plot
    fig = go.Figure()

    for item in year_color_map:
        year = item[0]
        color = item[1]
        year_data = df_pd[df_pd["year"] == year]

        if not year_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=year_data[x_column],
                    y=year_data[y_column],
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
            showline=True,  # Show the x-axis line
            linewidth=2,  # Set the x-axis line width
            linecolor="black",  # Set the x-axis line color
        ),
        yaxis=dict(
            title_font=dict(family="Noto Sans", size=18),
            color="black",
            showline=True,  # Show the x-axis line
            linewidth=2,  # Set the x-axis line width
            linecolor="black",  # Set the x-axis line color
        ),
    )

    object_path_name = "-".join(metric_object.lower().split(" "))
    filename = "-".join(title.lower().split(" "))

    write_image(fig, object_path_name, filename, filetype)

    # # Show the plot
    # fig.show()

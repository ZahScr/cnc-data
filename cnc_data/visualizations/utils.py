import plotly.graph_objs as go
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
    fig.show()

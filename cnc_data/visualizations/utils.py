import plotly.graph_objs as go
import os


def export_chart(x_series, y_series, title, x_title, y_title, filetype="png"):
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
        xaxis=dict(title_font=dict(family="Noto Sans", size=18), color="black"),
        yaxis=dict(title_font=dict(family="Noto Sans", size=18), color="black"),
    )

    if not os.path.exists("images"):
        os.mkdir("images")

    filename = "-".join(title.lower().split(" "))

    fig.write_image(f"images/test-{filename}.{filetype}")

    # # Show the plot
    # fig.show()

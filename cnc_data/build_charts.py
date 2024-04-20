from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from cnc_data.utilities.data_utils import (
    load_metrics_data,
    transform_for_weekly_cumulative_year_chart,
    adjust_yearly_cumulative_observations,
)
from cnc_data.utilities.chart_utils import (
    export_cumulative_yearly_chart,
    export_new_objects_yearly_chart,
)
from cnc_data.constants import (
    SCIENTIFIC_PLANTS,
    SCIENTIFIC_SNAKES,
    SCIENTIFIC_BIRDS,
    SCIENTIFIC_FROGS,
)

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

# Load CNC pre-computed metrics data
users_df, species_df, observations_df, weekly_by_species_df = load_metrics_data(
    spark, period="weekly"
)


def export_main_metric_charts():
    # Transform for charts (combine and filter)
    weekly_transformed_df = transform_for_weekly_cumulative_year_chart(
        users_df, species_df, observations_df
    ).filter(col("year") >= 2018)

    METRICS = ["users", "species", "observations"]

    for metric in METRICS:
        y_column_yearly = f"year_adjusted_cumulative_{metric}"
        y_column_all_time = f"cumulative_{metric}"

        # Export weekly cumulative charts
        export_cumulative_yearly_chart(
            weekly_transformed_df,
            metric_object=metric,
            period_name="week number",
            x_column="week_number",
            y_column=y_column_yearly,
            filetype="svg",
        )

        # Export weekly cumulative charts with all time
        export_cumulative_yearly_chart(
            weekly_transformed_df,
            metric_object=metric,
            period_name="week",
            x_column="week",
            y_column=y_column_all_time,
            filetype="svg",
        )

        # Export weekly unique charts
        export_new_objects_yearly_chart(
            weekly_transformed_df,
            metric_object=metric,
            metric_type="unique",
            period_name="week number",
            x_column="week_number",
            y_column=f"unique_{metric}",
            filetype="svg",
        )

        export_new_objects_yearly_chart(
            weekly_transformed_df,
            metric_object=metric,
            metric_type="unique",
            period_name="week",
            x_column="week",
            y_column=f"unique_{metric}",
            filetype="svg",
        )

        # Export weekly new charts (exclude observations since `new` is not possible)
        if metric != "observations":
            export_new_objects_yearly_chart(
                weekly_transformed_df,
                metric_object=metric,
                metric_type="new",
                period_name="week number",
                x_column="week_number",
                y_column=f"new_{metric}",
                filetype="svg",
            )

            export_new_objects_yearly_chart(
                weekly_transformed_df,
                metric_object=metric,
                metric_type="new",
                period_name="week",
                x_column="week",
                y_column=f"new_{metric}",
                filetype="svg",
            )


def export_species_metrics_charts():
    ALL_SCIENTIFIC_NAMES = (
        SCIENTIFIC_PLANTS + SCIENTIFIC_SNAKES + SCIENTIFIC_BIRDS + SCIENTIFIC_FROGS
    )

    weekly_by_species_transformed_df = adjust_yearly_cumulative_observations(
        weekly_by_species_df
    )

    for species_name in ALL_SCIENTIFIC_NAMES:
        weekly_by_species_transformed_test = weekly_by_species_transformed_df.filter(
            col("scientific_name") == species_name
        )

        weekly_by_species_transformed_test.show()

        # Yearly by week number
        export_cumulative_yearly_chart(
            weekly_by_species_transformed_test,
            metric_object=species_name,
            period_name="week number",
            x_column="week_number",
            y_column="year_adjusted_cumulative_observations",
            filetype="svg",
        )

        # Yearly by week all time
        export_cumulative_yearly_chart(
            weekly_by_species_transformed_test,
            metric_object=species_name,
            period_name="week",
            x_column="week",
            y_column="year_adjusted_cumulative_observations",
            filetype="svg",
        )

        # Export weekly unique charts
        export_new_objects_yearly_chart(
            weekly_by_species_transformed_test,
            metric_object=species_name,
            metric_type="unique",
            period_name="week number",
            x_column="week_number",
            y_column="observations",
            filetype="svg",
        )


# export_main_metric_charts()
export_species_metrics_charts()

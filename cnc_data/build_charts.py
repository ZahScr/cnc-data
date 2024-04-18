from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# from pyspark.sql.window import Window
# from pyspark.sql.dataframe import DataFrame
from cnc_data.utilities.data_utils import (
    load_metrics_data,
    transform_for_weekly_cumulative_year_chart,
    create_date_dimension,
)
from cnc_data.utilities.chart_utils import (
    export_cumulative_yearly_chart,
    export_new_objects_yearly_chart,
)

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

# Load CNC pre-computed metrics data
users_df, species_df, observations_df = load_metrics_data(spark, period="weekly")


# Transform for charts (combine and filter)
weekly_transformed_df = transform_for_weekly_cumulative_year_chart(
    users_df, species_df, observations_df
).filter(col("year") >= 2018)

METRICS = ["users", "species", "observations"]

for metric in METRICS:
    # Export weekly cumulative charts
    export_cumulative_yearly_chart(
        weekly_transformed_df,
        metric_object=metric,
        period_name="week",
        x_column="week_number",
        filetype="svg",
    )

    # Export weekly unique charts
    export_new_objects_yearly_chart(
        weekly_transformed_df,
        metric_object=metric,
        metric_type="unique",
        period_name="week",
        x_column="week_number",
        filetype="svg",
    )

    # Export weekly new charts (exclude observations since `new` is not possible)
    if metric != "observations":
        export_new_objects_yearly_chart(
            weekly_transformed_df,
            metric_object=metric,
            metric_type="new",
            period_name="week",
            x_column="week_number",
            filetype="svg",
        )


# Check it out
# weekly_transformed_df.select(
#     "week",
#     "year",
#     "week_number",
#     "new_users",
#     "unique_users",
#     "new_species",
#     "unique_species",
#     "unique_observations",
# ).show(1000)

# create_date_dimension(spark).filter(
#     (col("year").isin(2018, 2019, 2020))
#     & (col("date").between("2019-12-01", "2020-02-01"))
# ).show(1500)

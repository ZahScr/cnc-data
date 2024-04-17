from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
from cnc_data.utilities.data_utils import (
    load_metrics_data,
    transform_for_yearly_cumulative_chart,
)
from cnc_data.utilities.chart_utils import (
    export_cumulative_yearly_chart,
    export_new_objects_yearly_chart,
)

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

# Load CNC pre-computed metrics data
users_df, species_df, observations_df = load_metrics_data(spark)

# Transform for charts (combine and filter)
transformed_df = transform_for_yearly_cumulative_chart(
    users_df, species_df, observations_df
).filter(col("year") >= 2018)

# Check it out
transformed_df.select(
    "week",
    "year",
    "week_number",
    "new_users",
    "unique_users",
    "new_species",
    "unique_species",
    "unique_observations",
).show(500)

# Export cumulative charts
export_cumulative_yearly_chart(transformed_df, metric_object="users", filetype="svg")
export_cumulative_yearly_chart(transformed_df, metric_object="species", filetype="svg")
export_cumulative_yearly_chart(
    transformed_df, metric_object="observations", filetype="svg"
)

# Export new charts
export_new_objects_yearly_chart(
    transformed_df, metric_object="users", metric_type="new", filetype="svg"
)
export_new_objects_yearly_chart(
    transformed_df, metric_object="species", metric_type="new", filetype="svg"
)

# Export unique charts
export_new_objects_yearly_chart(
    transformed_df, metric_object="users", metric_type="unique", filetype="svg"
)
export_new_objects_yearly_chart(
    transformed_df, metric_object="species", metric_type="unique", filetype="svg"
)
export_new_objects_yearly_chart(
    transformed_df, metric_object="observations", metric_type="unique", filetype="svg"
)

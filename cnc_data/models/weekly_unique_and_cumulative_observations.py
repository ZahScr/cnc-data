from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
)
from pyspark.sql.window import Window
from cnc_data.utilities.utils import (
    export_chart,
    get_cnc_events,
    load_cnc_data,
    transform_for_chart,
)


def export_charts(
    spark: SparkSession,
    x_series,
    y_series,
    x_title: str,
    y_title: str,
    chart_title: str,
) -> None:
    print(f"Exporting visualization for {y_title}")

    cnc_events = get_cnc_events()

    export_chart(
        x_series=x_series,
        y_series=y_series,
        title=chart_title,
        x_title=x_title,
        y_title=y_title,
        cnc_events=cnc_events,
    )
    # include svg file
    export_chart(
        x_series=x_series,
        y_series=y_series,
        title=chart_title,
        x_title=x_title,
        y_title=y_title,
        filetype="svg",
        cnc_events=cnc_events,
    )


# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

df = load_cnc_data(spark)

# Calculate the first observation week for each user_id and taxon_id for every observation
# user_cumsum_window = Window.partitionBy("user_id").orderBy("observed_week")
# taxon_cumsum_window = Window.partitionBy("taxon_id").orderBy("observed_week")
# df = df.withColumn(
#     "first_user_week", first("observed_week", True).over(user_cumsum_window)
# ).withColumn("first_taxon_week", first("observed_week", True).over(taxon_cumsum_window))


# Join metrics on observation week
# final_df = (
#     observation_df.join(
#         new_species_df, on=(col("observed_week") == col("first_taxon_week")), how="left"
#     )
#     .join(new_user_df, on=(col("observed_week") == col("first_user_week")), how="left")
#     .join(
#         unique_species_df,
#         on=(col("observed_week") == col("observed_week_unique_species")),
#         how="left",
#     )
#     .join(
#         unique_users_df,
#         on=(col("observed_week") == col("observed_week_unique_users")),
#         how="left",
#     )
#     .select(
#         "observed_week",
#         "unique_observations",
#         "cumulative_observations",
#         "new_users",
#         "cumulative_users",
#         "unique_users",
#         "new_species",
#         "cumulative_species",
#         "unique_species",
#     )
# )

# final_df.show()

# final_df_pd, y_series_columns = transform_for_chart(spark, final_df)


# for column in y_series_columns:
#     series_type, category = map(lambda x: x.capitalize(), column.split("_"))
#     y_series = final_df_pd[column]
#     x_series = final_df_pd["observed_week_ts"]
#     title = f"Calgary {series_type} iNaturalist {category} by Week"
#     y_title = f"{series_type} {category}"
#     x_title = "Observed Week"

#     export_charts(spark, x_series, y_series, x_title, y_title, title)

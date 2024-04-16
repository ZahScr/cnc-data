from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cnc_data.utilities.utils import load_metrics_data

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

users_df, species_df, observations_df = load_metrics_data(spark)

# users_df.show()
# species_df.show()
# observations_df.show()

df = (
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
        "new_users",
        "unique_users",
        "cumulative_users",
        "new_species",
        "unique_species",
        "cumulative_species",
        "unique_observations",
        "cumulative_observations",
    )
    .show()
)

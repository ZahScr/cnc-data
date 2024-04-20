from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from cnc_data.utilities.data_utils import load_cnc_data
from cnc_data.models.users import calculate_weekly_user_metrics
from cnc_data.models.species import (
    calculate_weekly_species_metrics,
    calculate_weekly_metrics_for_species,
)
from cnc_data.models.observations import calculate_weekly_observation_metrics
from cnc_data.constants import (
    SCIENTIFIC_PLANTS,
    SCIENTIFIC_SNAKES,
    SCIENTIFIC_BIRDS,
    SCIENTIFIC_FROGS,
)

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

df = load_cnc_data(spark)

# Build weekly metrics
weekly_users_df = calculate_weekly_user_metrics(spark, df)
# weekly_users_df.show()

weekly_species_df = calculate_weekly_species_metrics(spark, df)
# weekly_species_df.show()

weekly_observations_df = calculate_weekly_observation_metrics(spark, df)
# weekly_observations_df.show()

ALL_SCIENTIFIC_NAMES = (
    SCIENTIFIC_PLANTS + SCIENTIFIC_SNAKES + SCIENTIFIC_BIRDS + SCIENTIFIC_FROGS
)

weekly_by_species_metrics_df = calculate_weekly_metrics_for_species(
    spark, df, ALL_SCIENTIFIC_NAMES
)

# users_df.write.mode("overwrite").csv("cnc_data/output/users")
# species_df.write.mode("overwrite").csv("cnc_data/output/species")
# observations_df.write.mode("overwrite").csv("cnc_data/output/observations")

# Save weekly metrics
weekly_users_df.write.format("com.databricks.spark.csv").mode("overwrite").option(
    "header", "true"
).save("cnc_data/output/weekly_users")

weekly_species_df.write.format("com.databricks.spark.csv").mode("overwrite").option(
    "header", "true"
).save("cnc_data/output/weekly_species")

weekly_observations_df.write.format("com.databricks.spark.csv").mode(
    "overwrite"
).option("header", "true").save("cnc_data/output/weekly_observations")

# Save species-specific metrics
weekly_by_species_metrics_df.write.format("com.databricks.spark.csv").mode(
    "overwrite"
).option("header", "true").save("cnc_data/output/weekly_by_species")

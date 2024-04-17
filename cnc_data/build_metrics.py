from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from cnc_data.utilities.data_utils import load_cnc_data
from cnc_data.models.users import calculate_user_metrics
from cnc_data.models.species import calculate_species_metrics
from cnc_data.models.observations import calculate_observation_metrics

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

df = load_cnc_data(spark)

users_df = calculate_user_metrics(spark, df)
users_df.show()

species_df = calculate_species_metrics(spark, df)
species_df.show()

observations_df = calculate_observation_metrics(spark, df)
observations_df.show()

# users_df.write.mode("overwrite").csv("cnc_data/output/users")
# species_df.write.mode("overwrite").csv("cnc_data/output/species")
# observations_df.write.mode("overwrite").csv("cnc_data/output/observations")

users_df.write.format("com.databricks.spark.csv").mode("overwrite").option(
    "header", "true"
).save("cnc_data/output/users")

species_df.write.format("com.databricks.spark.csv").mode("overwrite").option(
    "header", "true"
).save("cnc_data/output/species")

observations_df.write.format("com.databricks.spark.csv").mode("overwrite").option(
    "header", "true"
).save("cnc_data/output/observations")

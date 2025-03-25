from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct
from pyspark.sql.functions import year

from cnc_data.utilities.data_utils import (
    load_raw_data,
    load_metrics_data
)

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

raw_observations_df = load_raw_data(spark)
raw_observations_df = raw_observations_df.withColumn("year", year(raw_observations_df["observed_date"]))

aggregated_raw_df = raw_observations_df.agg(
    count("*").alias("total_count"),
    countDistinct("id").alias("distinct_id_count")
)
aggregated_by_year_df = raw_observations_df.groupBy("year").agg(
    count("*").alias("yearly_total_count"),
    countDistinct("id").alias("yearly_distinct_id_count")
).orderBy("year", ascending=False)

aggregated_by_year_df.show()
aggregated_raw_df.show()

users_df, species_df, observations_df, _ = load_metrics_data(spark)

users_df.show(5)
species_df.show(5)
observations_df.show(5)
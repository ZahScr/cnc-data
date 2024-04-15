from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from cnc_data.utilities.utils import (
    load_cnc_data,
    create_date_dimension,
)
from cnc_data.models.users import calculate_user_metrics

# Create a SparkSession
spark = SparkSession.builder.appName("CNC Data").getOrCreate()

df = load_cnc_data(spark)

# users_df = calculate_user_metrics(spark, df)
# users_df.show()

# species_df = calculate_species_metrics(spark, df)
# species_df.show()

# observations_df = calculate_observation_metrics(spark, df)
# observations_df.show()


date_dimension_df = create_date_dimension(spark)
date_dimension_df.show()

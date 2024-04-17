from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date,
    date_trunc,
    col,
    sum,
    first,
    countDistinct,
    datediff,
)
from pyspark.sql.window import Window
from cnc_data.utilities.data_utils import export_chart


# Create a SparkSession
spark = SparkSession.builder.appName("cncData").getOrCreate()

# Define the paths to the CSV files
csv_path1 = "../raw/observations-306591.csv"
csv_path2 = "../raw/observations-306599.csv"

# Read the CSV files into Spark dataframes
df1 = spark.read.format("csv").option("header", "true").load(csv_path1)
df2 = spark.read.format("csv").option("header", "true").load(csv_path2)

# Concatenate the two dataframes vertically
df = df1.union(df2)

# Print the column names
print("Loaded observations dataset with columns:")
for name in df.columns:
    print(name)

# Count the total number of rows
print(f"Total rows raw loaded: {df.count()}")

# Select the specified columns and drop one nulls
df = (
    df.select(
        "observed_on",
        "created_at",
        "id",
        "taxon_id",
        "user_id",
    )
    .na.drop()
    .withColumn(
        "observation_creation_delta",
        datediff(to_date(col("created_at")), to_date(col("observed_on"))),
    )
)

deltas = (
    df.groupBy("observation_creation_delta")
    .agg(countDistinct("id").alias("num_observations"))
    .filter(
        col("observation_creation_delta") > 30
    )  # Seeing around 40k observations with a delta of 1
    .orderBy(col("observation_creation_delta").asc())
)

deltas.show()

# deltas_pd = deltas.toPandas()

# export_chart(
#     x_series=deltas_pd["observation_creation_delta"],
#     y_series=deltas_pd["num_observations"],
#     title="Test Delta Distribution",
#     x_title="Observation Creation Delta in Days",
#     y_title="Unique Observations",
# )

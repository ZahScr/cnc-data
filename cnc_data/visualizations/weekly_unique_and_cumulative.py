from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date,
    to_timestamp,
    date_trunc,
    col,
    sum,
    first,
    countDistinct,
)
from pyspark.sql.window import Window
from utils import export_chart


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
        "time_observed_at",
        "id",
        "taxon_id",
        "user_id",
        # "taxon_kingdom_name",
        "common_name",
        "iconic_taxon_name",
    )
    .withColumn("observed_week", to_date(date_trunc("week", "observed_on")))
    .na.drop()
)

print(f"Total rows cleaned: {df.count()}")

# Calculate the first observation week for each user_id and taxon_id for every observation
user_cumsum_window = Window.partitionBy("user_id").orderBy("observed_week")
taxon_cumsum_window = Window.partitionBy("taxon_id").orderBy("observed_week")
df = df.withColumn(
    "first_user_week", first("observed_week", True).over(user_cumsum_window)
).withColumn("first_taxon_week", first("observed_week", True).over(taxon_cumsum_window))

# Calculate metrics for species
sum_taxon_window = Window.orderBy("first_taxon_week").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
taxon_df = (
    df.groupBy("first_taxon_week")
    .agg(countDistinct("taxon_id").alias("unique_species"))
    .orderBy("first_taxon_week")
    .withColumn("cumulative_species", sum("unique_species").over(sum_taxon_window))
)

# Calculate metrics for useres
sum_user_window = Window.orderBy("first_user_week").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
user_df = (
    df.groupBy("first_user_week")
    .agg(countDistinct("user_id").alias("unique_users"))
    .orderBy("first_user_week")
    .withColumn("cumulative_users", sum("unique_users").over(sum_user_window))
)

# Calculate metrics for observations
sum_obs_window = Window.orderBy("observed_week").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
observation_df = (
    df.groupBy("observed_week")
    .agg(countDistinct("id").alias("unique_observations"))
    .orderBy("observed_week")
    .withColumn(
        "cumulative_observations", sum("unique_observations").over(sum_obs_window)
    )
)

# Join metrics on observation week
final_df = (
    observation_df.join(
        taxon_df, on=(col("observed_week") == col("first_taxon_week")), how="left"
    )
    .join(user_df, on=(col("observed_week") == col("first_user_week")), how="left")
    .select(
        "observed_week",
        "unique_observations",
        "cumulative_observations",
        "unique_users",
        "cumulative_users",
        "unique_species",
        "cumulative_species",
    )
)

final_df.show()

# Convert the Spark dataframe to a Pandas dataframe
final_df_pd = (
    final_df.filter(col("observed_week") >= "2015-01-01")
    .withColumn(
        "observed_week_ts", to_timestamp("observed_week")
    )  # convert observed_week to timestamp for plotting
    .toPandas()
)

y_series_columns = list(
    filter(lambda x: (x != "observed_week"), final_df_pd.columns.values.tolist())
)

for column in y_series_columns:
    series_type, category = map(lambda x: x.capitalize(), column.split("_"))
    y_series = final_df_pd[column]
    title = f"Calgary {series_type} iNaturalist {category} by Week"
    y_title = f"{series_type} {category}"

    print(f"Exporting visualization for {y_title}")

    export_chart(
        x_series=final_df_pd["observed_week_ts"],
        y_series=y_series,
        title=title,
        x_title="Observed Week",
        y_title=y_title,
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date,
    date_trunc,
    min,
    col,
    sum,
    count,
    first,
    countDistinct,
)
from pyspark.sql.window import Window
import plotly.graph_objs as go


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
print(df.columns)

# Count the total number of rows
print(f"Total rows raw: {df.count()}")

# Select the specified columns
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

user_cumsum_window = Window.partitionBy("user_id").orderBy("observed_week")
taxon_cumsum_window = Window.partitionBy("taxon_id").orderBy("observed_week")

# calculate the first observation week for each user_id and taxon_id for every observation
df = df.withColumn(
    "first_user_week", first("observed_week", True).over(user_cumsum_window)
).withColumn("first_taxon_week", first("observed_week", True).over(taxon_cumsum_window))

sum_taxon_window = Window.orderBy("first_taxon_week").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
taxon_df = (
    df.groupBy("first_taxon_week")
    .agg(countDistinct("taxon_id").alias("unique_taxons"))
    .orderBy("first_taxon_week")
    .withColumn("cumulative_taxons", sum("unique_taxons").over(sum_taxon_window))
)

sum_user_window = Window.orderBy("first_user_week").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
user_df = (
    df.groupBy("first_user_week")
    .agg(countDistinct("user_id").alias("unique_users"))
    .orderBy("first_user_week")
    .withColumn("cumulative_users", sum("unique_users").over(sum_user_window))
)

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
        "unique_taxons",
        "cumulative_taxons",
    )
)

final_df.show()

# Extract the data from the Spark dataframe and convert to pandas
pdf = final_df.select("observed_week", "cumulative_observations").toPandas()

# Create the plotly figure
fig = go.Figure()
fig.add_trace(
    go.Scatter(
        x=pdf["observed_week"],
        y=pdf["cumulative_observations"],
        mode="lines",
        name="Cumulative Observations",
    )
)

# Set the axis labels and title, etc
font_family = "Noto Sans Black"

fig.update_layout(
    title={
        "text": "Cumulative Observations by Week",
        "font": {"family": "Basic Sans Blacks"},
    },
    xaxis={"title": "Week", "tickfont": {"family": font_family}},
    yaxis={"title": "Cumulative Observations", "tickfont": {"family": font_family}},
    # Set the background color and font color
    plot_bgcolor="#07874B",
    paper_bgcolor="#07874B",
    font=dict(color="white"),
)

# Show the plot
fig.show()

# 132936 place_id = Municipality City of Calgistan
# https://api.inaturalist.org/v1/docs/#!/Observations/get_observations
# https://api.inaturalist.org/v1/observations?place_id=132936&year=2017&order=desc&order_by=created_at

"""
Get observations
1. Have `start_date` constant
2. Track the following variables:
    - max `max_id`, `max_created_at` for max `id`
3. Iterate while max `created_at` < current_date?:
    a. Get observations where `created_at` >=  `start_date` (use `created_d1`).
        - Limit to 200 results per page (use `per_page`)
        - Order by `order_by` use `created_at` (default)
        - Order `order` user `asc`
        - If `max_id` exists, set `id_above` = `max_id` in query
        - For returned values: pull min `id`, max `id`, count n observations, update tracking `max_id`, `max_created_at`
        - Check that max - min = n ?
    b. Transform data:
        -- Required fields:
        "id"
        "observed_on"
        "time_observed_at"
        "id"
        "taxon_id"
        "user_id"
        "common_name"
        "created_at"
        "location"
        "species_name" <- ???
        "geojson"
        -- Extra"
        "uuid"
        "scientific_name"
        "taxon_kingdom_name"
        "wikipedia_url"
        "status_name"
        "category"
        "spam"
        "hidden"
        "created_at_details"
        "place_guess"
        "quality_grade"
    c. Merge resulting 200 rows into DataFrame

Incremental get observations
1. Get most recent created from existing observations
2. Run
"""

import requests
import time
from datetime import datetime
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 1. Function to extract required and extra fields from an observation record
# ---------------------------------------------------------------------------
def extract_observation_fields(obs):
    """
    Extracts and transforms fields from a single observation JSON object.
    
    Required Fields:
      - id
      - observed_on
      - time_observed_at
      - taxon_id
      - user_id
      - common_name
      - created_at
      - location
      - species_name (using taxon.name)
      - geojson

    Extra Fields:
      - uuid
      - scientific_name (using taxon.name)
      - taxon_kingdom_name (using taxon.iconic_taxon_name)
      - wikipedia_url (using taxon.wikipedia_url)
      - status_name (using taxon.conservation_status.status_name)
      - category (using taxon.rank)
      - spam
      - hidden
      - created_at_details
      - place_guess
      - quality_grade
    """
    taxon = obs.get("taxon", {})
    return {
        "id": obs.get("id"),
        "observed_on": obs.get("observed_on"),
        "time_observed_at": obs.get("time_observed_at"),
        "taxon_id": taxon.get("id"),
        "user_id": obs.get("user", {}).get("id"),
        "common_name": taxon.get("preferred_common_name"),
        "created_at": obs.get("created_at"),
        "location": obs.get("location"),
        "species_name": taxon.get("name"),  # Using taxon.name for species_name
        "geojson": obs.get("geojson"),

        # Extra fields
        "uuid": obs.get("uuid"),
        "scientific_name": taxon.get("name"),
        "taxon_kingdom_name": taxon.get("iconic_taxon_name"),
        "wikipedia_url": taxon.get("wikipedia_url"),
        "status_name": taxon.get("conservation_status", {}).get("status_name"),
        "category": taxon.get("rank"),
        "spam": obs.get("spam"),
        "hidden": obs.get("hidden"),
        "created_at_details": obs.get("created_at_details"),
        "place_guess": obs.get("place_guess"),
        "quality_grade": obs.get("quality_grade")
    }
import requests
import time
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# 1. Function to extract required and extra fields from an observation record
# ---------------------------------------------------------------------------
def extract_observation_fields(obs):
    """
    Extracts and transforms fields from a single observation JSON object.
    
    Required Fields:
      - id
      - observed_on
      - time_observed_at
      - taxon_id
      - user_id
      - common_name
      - created_at
      - location
      - species_name (using taxon.name)
      - geojson

    Extra Fields:
      - uuid
      - scientific_name (using taxon.name)
      - taxon_kingdom_name (using taxon.iconic_taxon_name)
      - wikipedia_url (using taxon.wikipedia_url)
      - status_name (using taxon.conservation_status.status_name)
      - category (using taxon.rank)
      - spam
      - hidden
      - created_at_details
      - place_guess
      - quality_grade
    """
    taxon = obs.get("taxon", {})
    return {
        "id": obs.get("id"),
        "observed_on": obs.get("observed_on"),
        "time_observed_at": obs.get("time_observed_at"),
        "taxon_id": taxon.get("id"),
        "user_id": obs.get("user", {}).get("id"),
        "common_name": taxon.get("preferred_common_name"),
        "created_at": obs.get("created_at"),
        "location": obs.get("location"),
        "species_name": taxon.get("name"),  # Using taxon.name for species_name
        "geojson": obs.get("geojson"),

        # Extra fields
        "uuid": obs.get("uuid"),
        "scientific_name": taxon.get("name"),
        "taxon_kingdom_name": taxon.get("iconic_taxon_name"),
        "wikipedia_url": taxon.get("wikipedia_url"),
        "status_name": taxon.get("conservation_status", {}).get("status_name"),
        "category": taxon.get("rank"),
        "spam": obs.get("spam"),
        "hidden": obs.get("hidden"),
        "created_at_details": obs.get("created_at_details"),
        "place_guess": obs.get("place_guess"),
        "quality_grade": obs.get("quality_grade")
    }

# ---------------------------------------------------------------------------
# 2. Function to hit the API, transform the response, and return metadata
# ---------------------------------------------------------------------------
def fetch_and_transform_observations(start_date, max_id=None, per_page=200):
    """
    Hits the iNaturalist API /observations endpoint for observations with
    created_at >= start_date. If max_id is provided, only fetch records with id above max_id.
    
    Returns:
      - A list of transformed observations (using extract_observation_fields)
      - The maximum id from this page (for tracking iteration)
      - The maximum created_at value from this page
      - The number of observations fetched
    """
    url = "https://api.inaturalist.org/v1/observations"
    params = {
        "created_d1": start_date,  # records starting from start_date
        "per_page": per_page,
        "order_by": "created_at",
        "order": "asc"
    }
    if max_id:
        params["id_above"] = max_id

    response = requests.get(url, params=params)
    response.raise_for_status()
    results = response.json().get("results", [])

    if not results:
        return [], None, None, 0

    transformed = [extract_observation_fields(r) for r in results]
    
    # Get the range of IDs in this page for simple validation
    ids = [r["id"] for r in results]
    min_id = min(ids)
    max_id_returned = max(ids)
    max_created_at = max(r["created_at"] for r in results)

    # Simple check: if the difference between max and min id doesn't equal the count, print a warning.
    if (max_id_returned - min_id + 1) != len(results):
        print(f"Discrepancy in results: expected count {max_id_returned - min_id + 1} but got {len(results)}")
    
    return transformed, max_id_returned, max_created_at, len(results)

# ---------------------------------------------------------------------------
# 3. Function to iterate, load data, and merge into a Spark table
# ---------------------------------------------------------------------------
def ingest_observations(start_date: str, spark: SparkSession, table_name="inaturalist_observations"):
    """
    Iterates over the API starting from a given start_date.
    Accumulates the transformed observations and merges them into a Spark table.
    
    The loop stops when the latest fetched created_at exceeds the current UTC time.
    If the table exists, new records (identified by id) are merged.
    """
    max_id = None
    max_created_at = start_date
    data = []
    current_date = datetime.utcnow().isoformat()

    while max_created_at < current_date:
        print(f"Fetching observations with id above {max_id}...")
        observations, new_max_id, new_max_created_at, count = fetch_and_transform_observations(start_date, max_id)
        
        if not observations:
            print("No more data returned by the API.")
            break

        data.extend(observations)
        max_id = new_max_id
        max_created_at = new_max_created_at

        print(f"Fetched {count} observations. New max_id = {max_id}, new max_created_at = {max_created_at}")
        time.sleep(1)  # Pause to be respectful to the API

    if not data:
        print("No data was loaded from the API.")
        return

    # Create a DataFrame from the collected data
    df = spark.createDataFrame(data)
    
    # Merge the new data into the existing table on the 'id' column.
    if spark._jsparkSession.catalog().tableExists(table_name):
        # If the table exists, load it and filter out duplicate IDs before merging.
        existing_df = spark.table(table_name)
        new_df = df.alias("new").join(existing_df.alias("old"), on="id", how="left_anti")
        merged_df = new_df.unionByName(existing_df)
        merged_df.write.mode("overwrite").saveAsTable(table_name)
        print(f"Merged {new_df.count()} new records into existing table `{table_name}`.")
    else:
        df.write.mode("overwrite").saveAsTable(table_name)
        print(f"Created table `{table_name}` with {df.count()} records.")

# ---------------------------------------------------------------------------
# Main entry point with argument parsing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="iNaturalist observations ingestion")
    parser.add_argument("--backfill", action="store_true",
                        help="If provided, start from Jan 1, 2017; otherwise use last ingestion date - 1 day")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("iNaturalist Ingest") \
        .enableHiveSupport() \
        .getOrCreate()
    
    table_name = "inaturalist_observations"
    
    if args.backfill:
        start_date = "2017-01-01T00:00:00Z"
        print("Running in backfill mode. Start date:", start_date)
    else:
        # If table exists, get the maximum created_at and subtract 1 day.
        if spark._jsparkSession.catalog().tableExists(table_name):
            max_created_at_df = spark.sql(f"SELECT max(created_at) as max_created_at FROM {table_name}")
            max_created_at_value = max_created_at_df.collect()[0]["max_created_at"]
            if max_created_at_value:
                # Convert to datetime, taking care of the trailing 'Z'
                max_dt = datetime.fromisoformat(max_created_at_value.replace("Z", "+00:00"))
                start_date = (max_dt - timedelta(days=1)).isoformat()
            else:
                start_date = "2017-01-01T00:00:00Z"
        else:
            start_date = "2017-01-01T00:00:00Z"
        print("Running in incremental mode. Start date:", start_date)
    
    ingest_observations(start_date, spark, table_name=table_name)
# scripts/bikeshare.py

import os
import time
import pandas as pd
from datetime import datetime, timedelta

from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.cloud.bigquery import ExternalConfig
from google.api_core.exceptions import NotFound

SERVICE_ACCOUNT_PATH = "/opt/airflow/scripts/service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.full_control",
]

# Build credentials from the JSON file we mount in Docker
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_PATH,
    scopes=SCOPES
)

# Or read from the creds object, but let's just set it manually:
PROJECT_ID = "bikeshareproject-448815"


def create_or_get_bucket(bucket_name, project_id, credentials):
    """
    Creates or retrieves a GCS bucket using the provided project ID & credentials.
    """
    storage_client = storage.Client(project=project_id, credentials=credentials)
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except NotFound:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Created bucket: {bucket.name}")
    return bucket


def extract_and_partition_bikeshare_data(project_id, dataset_table, bucket_name, credentials):
    """
    - Queries the previous day's Austin Bikeshare data in BigQuery
    - Writes Parquet files to GCS with a single partition key: date_hour=YYYY-MM-DD-HH
    """
    # Yesterday as an example (for real daily runs, do -1 day, not -365)
    # Adjust if you'd like actual "yesterday" logic:
    # yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    yesterday = (datetime.utcnow() - timedelta(days=365)).date()
    date_str = yesterday.strftime("%Y-%m-%d")

    bq_client = bigquery.Client(project=project_id, credentials=credentials)
    query = f"""
        SELECT *
        FROM `{dataset_table}`
        WHERE DATE(start_time) = '{yesterday}'
    """
    df = bq_client.query(query).to_dataframe()

    if df.empty:
        print(f"No data found for {date_str}. Exiting.")
        return

    # Add partition columns
    df["date_partition"] = df["start_time"].dt.date.astype(str)
    df["hour_partition"] = df["start_time"].dt.hour.astype(int)

    storage_client = storage.Client(project=project_id, credentials=credentials)
    bucket = storage_client.bucket(bucket_name)

    for hour, hour_df in df.groupby("hour_partition"):
        date_hour_value = f"{date_str}-{hour:02d}"
        local_file_name = f"temp_{hour}.parquet"
        hour_df.to_parquet(local_file_name, index=False)

        blob_path = f"bikeshare/date_hour={date_hour_value}/data.parquet"
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_file_name)

        os.remove(local_file_name)
        print(f"Uploaded {len(hour_df)} rows → {blob_path}")


def create_external_table_single_partition(
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    credentials,
    delete_if_exists=True
):
    """
    Creates an external Hive-partitioned table in BigQuery using a single partition column.
    """
    bq_client = bigquery.Client(project=project_id, credentials=credentials)

    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    external_config = ExternalConfig("PARQUET")
    external_config.source_uris = [
        f"gs://{bucket_name}/bikeshare/date_hour=*/data.parquet"
    ]
    external_config.hive_partitioning_mode = "AUTO"
    # You can also set the partitioning field name if you'd like
    external_config.hive_partitioning_source_uri_prefix = f"gs://{bucket_name}/bikeshare/"
    table.external_data_configuration = external_config

    if delete_if_exists:
        try:
            bq_client.delete_table(table_ref)
            print(f"Deleted old table: {table_ref}")
        except NotFound:
            pass

    created_table = bq_client.create_table(table)
    print(f"Created external table: {created_table.full_table_id}")

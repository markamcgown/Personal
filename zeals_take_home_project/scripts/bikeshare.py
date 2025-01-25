# scripts/bikeshare.py
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import ExternalConfig
from google.api_core.exceptions import NotFound

def extract_and_partition_bikeshare_data(project_id, dataset_table, bucket_name, creds=None):
    """
    Pull from BigQuery, partition by date/hour, upload Parquet to GCS.
    """
    # Pull "yesterday" from 1 year ago's data
    yesterday = (datetime.utcnow() - timedelta(days=365)).date()
    date_str = yesterday.strftime("%Y-%m-%d")

    # Initialize clients with explicit credentials if provided
    bq_client = bigquery.Client(project=project_id, credentials=creds)
    storage_client = storage.Client(project=project_id, credentials=creds)

    query = f"""
        SELECT *
        FROM `{dataset_table}`
        WHERE DATE(start_time) = '{yesterday}'
    """
    df = bq_client.query(query).to_dataframe()
    if df.empty:
        print(f"No data found for {date_str}. Exiting.")
        return

    df["date_partition"] = df["start_time"].dt.date.astype(str)
    df["hour_partition"] = df["start_time"].dt.hour.astype(int)

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


def create_external_table_single_partition(project_id, dataset_id, table_id, bucket_name,
                                           delete_if_exists=True, creds=None):
    """
    Create (or recreate) an external table partitioned by hive partitioning.
    """
    client = bigquery.Client(project=project_id, credentials=creds)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    external_config = ExternalConfig("PARQUET")
    external_config.source_uris = [f"gs://{bucket_name}/bikeshare/date_hour=*/data.parquet"]
    external_config.hive_partitioning_mode = "AUTO"
    external_config.hive_partitioning_source_uri_prefix = f"gs://{bucket_name}/bikeshare/"
    table.external_data_configuration = external_config

    if delete_if_exists:
        try:
            client.delete_table(table_ref)
            print(f"Deleted old table: {table_ref}")
        except NotFound:
            pass

    created_table = client.create_table(table)
    print(f"Created external table: {created_table.full_table_id}")

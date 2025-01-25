from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def extract_task(**kwargs):
    """
    Extract bike share data by calling `extract_and_partition_bikeshare_data`.
    Explicitly load credentials from the service_account.json file.
    """
    from google.oauth2 import service_account
    from scripts.bikeshare import extract_and_partition_bikeshare_data

    # Path to the service account JSON inside the container
    credentials_path = "/opt/airflow/service_account.json"
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Service account file not found at {credentials_path}. "
            f"Did you mount or copy it into the container?"
        )

    # Load credentials from JSON instead of using GOOGLE_APPLICATION_CREDENTIALS
    creds = service_account.Credentials.from_service_account_file(credentials_path)

    project_id = "bikeshareproject-448815"
    dataset_table = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
    bucket_name = "your-bucket"

    # Pass the credentials to your extract function
    extract_and_partition_bikeshare_data(
        project_id=project_id,
        dataset_table=dataset_table,
        bucket_name=bucket_name,
        creds=creds
    )


def create_external_table_task(**kwargs):
    """
    Create or recreate the external table by calling
    `create_external_table_single_partition`.
    Explicitly load credentials from the service_account.json file.
    """
    from google.oauth2 import service_account
    from scripts.bikeshare import create_external_table_single_partition

    credentials_path = "/opt/airflow/service_account.json"
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Service account file not found at {credentials_path}. "
            f"Did you mount or copy it into the container?"
        )

    creds = service_account.Credentials.from_service_account_file(credentials_path)

    project_id = "bikeshareproject-448815"
    dataset_id = "my_dataset"
    table_id = "bikeshare_ext"
    bucket_name = "your-bucket"

    create_external_table_single_partition(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        delete_if_exists=True,
        creds=creds
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bikeshare_etl',
    default_args=default_args,
    description='Daily pipeline for Bikeshare data',
    schedule_interval='0 3 * * *',
    catchup=False
)

t1 = PythonOperator(
    task_id='extract_bikeshare_data',
    python_callable=extract_task,
    dag=dag
)

t2 = PythonOperator(
    task_id='create_external_table',
    python_callable=create_external_table_task,
    dag=dag
)

t1 >> t2

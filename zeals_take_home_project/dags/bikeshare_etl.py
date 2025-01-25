from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def extract_task(**kwargs):
    from scripts.bikeshare import extract_and_partition_bikeshare_data

    # Set the path to your service account JSON file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/service_account.json"

    project_id = "bikeshareproject-448815"
    dataset_table = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
    bucket_name = "your-bucket"
    extract_and_partition_bikeshare_data(project_id, dataset_table, bucket_name)

def create_external_table_task(**kwargs):
    from scripts.bikeshare import create_external_table_single_partition

    # Set the path to your service account JSON file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/service_account.json"

    project_id = "bikeshareproject-448815"
    dataset_id = "my_dataset"
    table_id = "bikeshare_ext"
    bucket_name = "your-bucket"
    create_external_table_single_partition(project_id, dataset_id, table_id, bucket_name, True)

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

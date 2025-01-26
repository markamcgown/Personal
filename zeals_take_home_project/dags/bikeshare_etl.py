# dags/bikeshare_etl.py

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def extract_task(**kwargs):
    """
    1) Creates (or gets) the unique GCS bucket
    2) Extracts & partitions the data
    3) XCom-push the bucket name so next task can use it
    """
    from scripts.bikeshare import (
        creds,
        PROJECT_ID,
        create_or_get_bucket,
        extract_and_partition_bikeshare_data
    )

    # Make a unique bucket name each time the task runs
    unique_bucket_name = f"bikeshare-bucket-{int(time.time())}"

    create_or_get_bucket(unique_bucket_name, PROJECT_ID, creds)
    dataset_table = "bigquery-public-data.austin_bikeshare.bikeshare_trips"
    extract_and_partition_bikeshare_data(PROJECT_ID, dataset_table, unique_bucket_name, creds)

    # Store the bucket name in XCom
    kwargs['ti'].xcom_push(key='bucket_name', value=unique_bucket_name)


def create_external_table_task(**kwargs):
    """
    1) Pull the bucket name from XCom
    2) Create the external BQ table with hive partitioning
    """
    from scripts.bikeshare import (
        creds,
        PROJECT_ID,
        create_external_table_single_partition
    )

    ti = kwargs['ti']
    unique_bucket_name = ti.xcom_pull(key='bucket_name', task_ids='extract_bikeshare_data')

    # Must already have a dataset named "my_dataset" in project "PROJECT_ID"
    dataset_id = "my_dataset"
    table_id = "bikeshare_ext"

    create_external_table_single_partition(
        project_id=PROJECT_ID,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=unique_bucket_name,
        credentials=creds,
        delete_if_exists=True
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Adjust as you like
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bikeshare_etl',
    default_args=default_args,
    description='Daily pipeline for Bikeshare data',
    schedule_interval='@daily',
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

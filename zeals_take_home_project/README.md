1. Project Overview
This project implements a daily ETL pipeline for Austin Bikeshare data from Google BigQuery to Google Cloud Storage (GCS), and then creates an external (BigLake) table in BigQuery for downstream analysis. It uses Apache Airflow to automate the end-to-end workflow.

Key Steps:

Data Extraction:

Query the BigQuery public dataset bigquery-public-data.austin_bikeshare.bikeshare_trips to retrieve the previous day’s data (for demonstration, it may query data treating yesterday as 365 days ago, due to the last date in this public dataset).

Partitioning + Storage:

Write the extracted data to GCS in Parquet format, partitioned by date and hour.
Directory structure is: bikeshare/date_hour=YYYY-MM-DD-HH/data.parquet.

Create External (BigLake) Table:

A hive-partitioned external table is created in BigQuery referencing the partitioned Parquet files.

Airflow DAG:

Orchestrates the pipeline daily (@daily schedule).
Runs two tasks: Extract (upload to GCS) → Create External Table (in BigQuery).
This setup is containerized with Docker + docker-compose to ensure consistent local development.

2. Repository Layout
├── dags
│   └── bikeshare_etl.py       # Airflow DAG definition
├── scripts
│   ├── __init__.py            # (empty) so scripts is recognized as a Python package
│   ├── bikeshare.py           # Python module with ETL logic
│   └── service_account.json   # Service account key (not tracked in public repos!)
├── Dockerfile                 # Docker instructions for building Airflow image
├── docker-compose.yml         # Compose file to run Airflow container
└── requirements.txt           # Python dependencies

3. Requirements
-Docker and docker-compose installed locally.
-A Google Cloud project with:
-A service account that has permissions for:
-BigQuery (e.g., roles/bigquery.dataEditor or roles/bigquery.admin)
-GCS (e.g., roles/storage.admin or roles/storage.objectAdmin)
-A BigQuery dataset created (for example, my_dataset) to store the external table.
-A local copy of your service_account.json key placed in ./scripts/service_account.json. (Do not commit this file to a public repository!)

4. Installation & Setup

Clone this repository locally:

git clone https://github.com/yourusername/your-repo.git
cd your-repo

Place your Google Cloud service account key in scripts/service_account.json. Make sure the file is ignored in .gitignore so it’s not pushed publicly.

Check or create a BigQuery dataset called my_dataset in your Google Cloud project, e.g.:

bq --location=US mk -d your-project-id:my_dataset

Build & Start Airflow locally:

docker-compose up --build

This will:
-Build a Docker image based on Dockerfile
-Install Python dependencies from requirements.txt
-Start Airflow with a scheduler + webserver in one container
-Access the Airflow UI at http://localhost:8080.

Default user/password is typically admin/admin (depending on your Dockerfile setup)
(Or see the container logs for a generated password.)

5. Usage
Enable the bikeshare_etl DAG in the Airflow UI if it’s not already active.
Trigger the DAG manually to test, or wait for the daily schedule (@daily).

The ETL pipeline will:
-Dynamically create a GCS bucket named bikeshare-bucket-<timestamp> (or reuse one if you modify the code)
-Query data from BigQuery’s bigquery-public-data.austin_bikeshare.bikeshare_trips
-Partition by date/hour, write as Parquet to GCS
-Create a hive-partitioned external table in my_dataset.bikeshare_ext.
-Check Airflow logs to confirm it’s working. If everything is successful, you can query the external table in BigQuery, e.g.:

SELECT *
FROM `your-project-id.my_dataset.bikeshare_ext`
LIMIT 10;

6. Testing & Troubleshooting
Logs: View Airflow task logs in the UI under “Task Instances.”
IAM Issues: If you see 403 errors writing to GCS, ensure your service account has storage.buckets.create or storage.objects.create.
PYTHONPATH / ModuleNotFound: We set ENV PYTHONPATH="/opt/airflow/scripts:$PYTHONPATH" in the Dockerfile. Make sure __init__.py is in scripts/.
Dataset Not Found: Ensure my_dataset exists in your GCP project.

7. Future Improvements
Use a real date offset (e.g., “yesterday” instead of 365 days ago).
Parameterize the GCS bucket name or the BQ dataset/table via environment variables.
Deploy this Airflow setup in a more production-like environment (e.g., multiple containers or in Cloud Composer).

8. Disclaimer
Do Not commit or publish your private service_account.json if this repository is public. Ensure .gitignore excludes it.

This project is a demonstration for the Data Engineering Test and is not meant for production usage without further security and configuration.

9. SQL Q&A's: 

1) **Find the total number of trips for each day.**
SELECT date_partition
,count(distinct trip_id) as trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1

2) **Calculate the average trip duration for each day.**
SELECT date_partition
,round(avg(duration_minutes),2) as avg_duration
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1

3) **Identify the top 5 stations with the highest number of trip starts.**
select start_station_name
from
(SELECT start_station_name
,count(distinct trip_id) as trip_starts
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1 
 order by trip_starts desc
 limit 5)

4) **Find the average number of trips per hour of the day.**
select hour_partition
,avg(trips) as avg_trips
from
(SELECT date_partition
,hour_partition
,count(distinct trip_id) as trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1,2)
 group by 1 
 order by hour_partition 

5) **Determine the most common trip route (start station to end station).**
select start_station_name
,end_station_name
from
(SELECT start_station_name
,end_station_name
,count(distinct trip_id) as trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
group by 1,2
order by trips desc 
limit 1)

6) **Calculate the number of trips each month.**
SELECT date_trunc(date(date_partition),MONTH) as month
,count(distinct trip_id) as trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
group by 1

7) **Find the station with the longest average trip duration.**

It's not clear if this is for the start/end station, so I average over them both (this is an assumption on my part).

WITH start_data AS (
  SELECT
    start_station_name AS station_name,
    duration_minutes
  FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
  WHERE start_station_name IS NOT NULL
),
end_data AS (
  SELECT
    end_station_name AS station_name,
    duration_minutes
  FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
  WHERE end_station_name IS NOT NULL
)

SELECT
  station_name,
  COUNT(*) AS trip_count,
  round(AVG(duration_minutes),2) AS avg_trip_duration
FROM (
  SELECT * FROM start_data
  UNION ALL
  SELECT * FROM end_data
)
GROUP BY station_name
ORDER BY avg_trip_duration DESC
LIMIT 1;

8) **Find the busiest hour of the day (most trips started).**

This finds on average the busiest hour of the day.

select hour_partition as hour 
from
(select hour_partition
,avg(trips) as avg_trips
from
(SELECT date_partition
,hour_partition
,count(distinct trip_id) as trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1,2)
 group by 1 
 order by avg_trips desc  
 limit 1)

This finds the hour with the most trips for all-time. Both queries find the same answer (hour=16).

select hour_partition as hour 
from
(SELECT hour_partition
,count(distinct trip_id) as total_trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1 
 order by total_trips desc  
 limit 1)

9) **Identify the day with the highest number of trips.**
select date_partition as date 
from
(SELECT date_partition
,count(distinct trip_id) as total_trips
 FROM `bikeshareproject-448815.my_dataset.bikeshare_ext` 
 group by 1 
 order by total_trips desc  
 limit 1)
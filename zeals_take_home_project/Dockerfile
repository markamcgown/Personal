# Dockerfile
FROM apache/airflow:2.3.0

# Configure environment so it uses SQLite + SequentialExecutor
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"

# Copy and install your requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# The "standalone" command initializes DB, creates user, starts webserver + scheduler
CMD ["airflow", "standalone"]

# Ensure admin user is created during build
RUN airflow db init && \
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

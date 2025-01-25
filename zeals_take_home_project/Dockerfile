# Dockerfile
FROM apache/airflow:2.3.0

# Configure environment so it uses SQLite + SequentialExecutor
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Copy and install your requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# The "standalone" command initializes DB, creates user, starts webserver + scheduler
CMD ["airflow", "standalone"]

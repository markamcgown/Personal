# Dockerfile

FROM apache/airflow:2.5.1

# Install any system packages you might need
USER root
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    # Install Google Cloud SDK
    apt-transport-https ca-certificates gnupg \
 && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
      | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
      | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
 && apt-get update && apt-get install -y google-cloud-sdk \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# Environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH="/opt/airflow/scripts:${PYTHONPATH}"

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Expose the webserver port
EXPOSE 8080

# By default, run the "standalone" command to init DB & start scheduler + webserver
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

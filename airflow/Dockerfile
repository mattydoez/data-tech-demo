FROM apache/airflow:2.1.2

USER root

# Create the necessary directories with correct permissions
RUN mkdir -p /usr/local/airflow && chmod -R 777 /usr/local/airflow

# Copy custom airflow.cfg if necessary
COPY airflow.cfg /usr/local/airflow/airflow.cfg


# Install additional Python packages if required
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

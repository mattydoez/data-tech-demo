# Use the official Airflow image
FROM apache/airflow:latest

# Switch to root user to install additional dependencies
USER root

# Install PostgreSQL client library, build-essential, and other necessary packages
RUN apt-get update && apt-get install -y \
    libpq-dev gcc build-essential python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy requirements.txt to the Docker image
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Set environment variable for Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow
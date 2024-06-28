# E-Commerce Data Analytics Suite

This project is an analytics suite for a fictional e-commerce company. The company sells various products on its website. The suite includes data generation, ETL orchestration, data modeling, and visualization.

## Stack

- Airflow
- Docker & Docker-Compose
- dbt core
- Superset

## Setup

### Environment Variables
Rename `.envexample` file to `.env` and set your desired environment variables. Ensure you do not commit files containing sensitive information like passwords.

### System Requirements
* [Docker](https://docs.docker.com/engine/install/)
* [Docker-Compose](https://docs.docker.com/compose/install/)

### Building and Running Containers
With Docker engine and Docker-Compose installed, change directory to the root folder of the project (where `docker-compose.yml` is located) and run:

docker-compose up –build

### Demo Credentials
Demo credentials are set in the .env file mentioned above. For Airflow, they are by default:
* AIRFLOW_USER=airflow
* AIRFLOW_PASSWORD=airflow

### Ports Exposed Locally
* Airflow: 8080
* Superset: 8088
* PostgreSQL Database: 5432

## dbt Core

If you'd like to run your dbt core models outside of Airflow, you can:
1. Create a Python virtual environment:
    ```
    python3 -m venv .venv
    ```
2. Activate it:
    ```
    source .venv/bin/activate
    ```
3. Install the requirements found under `dbt/requirements.txt`:
    ```
    pip install -r dbt/requirements.txt
    ```
4. Run dbt commands, such as:
    ```
    dbt compile
    ```

## General Workflow

1. Generate test data and set up the PostgreSQL database with schemas for raw data and data warehouse.
2. Use Airflow to orchestrate ETL tasks.
3. Use dbt to transform and model the data.
4. Explore and visualize the data using Superset.

### Airflow DAGs

Once the containers are running, open the Airflow GUI (locally at `localhost:8080`) to view and manage the ETL workflows (DAGs). The following workflows are defined:
- **initialize_etl_environment**: Initializes the ETL environment by creating necessary database objects. Run only once.
- **import_main_data**: Imports data from the raw schema to the staging area in the data warehouse.
- **run_dbt_models**: Transforms the data using dbt and loads it into the data warehouse.

### Superset Dashboards

After the DAGs have completed, analyze the data using Superset (locally at `localhost:8088`).

## Example Data

The data used in this project is fictional and automatically generated. Any resemblance to real persons, living or dead, or actual products is purely coincidental.
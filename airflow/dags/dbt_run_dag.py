from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dbt_run_command():
    return "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"

with DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Run dbt models with Airflow',
    schedule_interval=timedelta(minutes=180),
    catchup=False,
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=create_dbt_run_command(),
        env={
            'POSTGRES_USER': os.getenv('POSTGRES_USER'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD'),
            'DBT_TARGET': 'dev',
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin'
        }
    )

    run_dbt
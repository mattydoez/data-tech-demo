# airflow/dags/ecomm_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_and_insert_data():
    # Your data generation and insertion code here
    pass

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='Generate and insert e-commerce data every 10 minutes',
    schedule_interval=timedelta(minutes=10),
)

generate_and_insert_data_task = PythonOperator(
    task_id='generate_and_insert_data',
    python_callable=generate_and_insert_data,
    dag=dag,
)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, world!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 2),
    'retries': 1,
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)

t1 = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag,
)
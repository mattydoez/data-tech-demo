from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess
import json
import psycopg2
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_data():
    try:
        # Run the data generation script
        print("Running data generation script...")
        result = subprocess.run(["python3", "/opt/airflow/generator/events_generator.py"], check=True, capture_output=True, text=True, timeout=300)
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error in generate_data: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise e  # Raise exception to fail the task
    except subprocess.TimeoutExpired as e:
        print(f"Data generation script timed out: {e}")
        raise e  # Raise exception to fail the task
    except Exception as e:
        print(f"General error in generate_data: {e}")
        raise e  # Raise exception to fail the task

def insert_data(file_path, table_name, insert_query):
    try:
        print(f"Inserting data into {table_name} from {file_path}...")
        with open(file_path, "r") as f:
            data = json.load(f)

        conn = psycopg2.connect(
            dbname="company_db",
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="company_db",
            port="5432"
        )
        cursor = conn.cursor()
        
        for record in data:
            cursor.execute(insert_query, tuple(record.values()))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully inserted data into {table_name}.")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        raise e  # Raise exception to fail the task

def insert_clickstream():
    insert_data("/opt/airflow/generator/fake_clickstream_events.json", "clickstream_events", insert_clickstream_query)

def insert_transactions():
    insert_data("/opt/airflow/generator/fake_transaction_data.json", "transactions", insert_transactions_query)

insert_clickstream_query = """
    INSERT INTO clickstream_events (user_id, timestamp, page_visited, action, product_id) VALUES (%s, %s, %s, %s, %s)
"""

insert_transactions_query = """
    INSERT INTO transactions (timestamp, product_id, user_id, payment_method, transaction_type) VALUES (%s, %s, %s, %s, %s)
"""

dag = DAG(
    'events_generator',
    default_args=default_args,
    description='Generate and insert events data every 60 minutes',
    schedule_interval=timedelta(minutes=60),
)

generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag,
)

insert_events_task = PythonOperator(
    task_id='insert_events',
    python_callable=insert_clickstream,
    dag=dag,
)

insert_transactions_task = PythonOperator(
    task_id='insert_transactions',
    python_callable=insert_transactions,
    dag=dag,
)

generate_data_task >> [insert_events_task, insert_transactions_task]
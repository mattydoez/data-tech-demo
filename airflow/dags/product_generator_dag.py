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
        result = subprocess.run(["python3", "/opt/airflow/generator/product_generator.py"], check=True, capture_output=True, text=True, timeout=300)
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error in generate_data: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise e  # Raise the exception to fail the task
    except subprocess.TimeoutExpired as e:
        print(f"Data generation script timed out: {e}")
        raise e  # Raise the exception to fail the task
    except Exception as e:
        print(f"General error in generate_data: {e}")
        raise e  # Raise the exception to fail the task

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
        raise e  # Raise the exception to fail the task

def insert_products():
    insert_data("/opt/airflow/generator/fake_products.json", "products", insert_product_query)

insert_product_query = """
    INSERT INTO products (name, category, description, price) VALUES (%s, %s, %s, %s)
"""

dag = DAG(
    'product_generator',
    default_args=default_args,
    description='Generate and insert product data once',
    schedule_interval=None,
)

generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag,
)

insert_products_task = PythonOperator(
    task_id='insert_products',
    python_callable=insert_products,
    dag=dag,
)

generate_data_task >> insert_products_task
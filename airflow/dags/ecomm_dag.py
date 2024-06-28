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
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_and_insert_data():
    try:
        # Run the data generation script
        result = subprocess.run(["python3", "/opt/airflow/generator/ecomm_generator.py"], check=True, capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
        
        # Insert generated data into PostgreSQL database
        insert_data_into_db("/opt/airflow/generator/fake_users.json", "users", insert_user_query)
        insert_data_into_db("/opt/airflow/generator/fake_clickstream_events.json", "clickstream_events", insert_clickstream_event_query)
        insert_data_into_db("/opt/airflow/generator/fake_transaction_data.json", "transactions", insert_transaction_query)
        insert_data_into_db("/opt/airflow/generator/fake_google_search_data.json", "google_search", insert_google_search_query)
        insert_data_into_db("/opt/airflow/generator/fake_email_marketing_data.json", "email_marketing", insert_email_marketing_query)
        insert_data_into_db("/opt/airflow/generator/fake_facebook_data.json", "facebook", insert_facebook_query)
        
    except subprocess.CalledProcessError as e:
        print(f"Error in generate_and_insert_data: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
    except Exception as e:
        print(f"General error in generate_and_insert_data: {e}")

def insert_data_into_db(file_path, table_name, insert_query):
    try:
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
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")

insert_user_query = """
    INSERT INTO users (username, email, address, phone_number, created_at) VALUES (%s, %s, %s, %s, %s)
"""
insert_clickstream_event_query = """
    INSERT INTO clickstream_events (user_id, event_type, event_timestamp) VALUES (%s, %s, %s)
"""
insert_transaction_query = """
    INSERT INTO transactions (transaction_id, product_id, user_id, payment_method, transaction_type, transaction_timestamp) VALUES (%s, %s, %s, %s, %s, %s)
"""
insert_google_search_query = """
    INSERT INTO google_search (date, campaign, ad, impressions, clicks, cost, conversions, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
insert_email_marketing_query = """
    INSERT INTO email_marketing (date, user_email, campaign, received, opened, subscribed, clicks, bounces, unsubscribed, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
insert_facebook_query = """
    INSERT INTO facebook (platform, date, campaign, ad_unit, impressions, percent_watched, clicks, cost, conversions, likes, shares, comments, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

dag = DAG(
    'ecomm_data_pipeline',
    default_args=default_args,
    description='Generate and insert e-commerce data every 10 minutes',
    schedule_interval=timedelta(minutes=10),
)

generate_and_insert_data_task = PythonOperator(
    task_id='generate_and_insert_data',
    python_callable=generate_and_insert_data,
    dag=dag,
)
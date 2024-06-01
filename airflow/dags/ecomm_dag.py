# airflow/dags/ecomm_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess
import json
import psycopg2

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
    # Run the data generation script
    subprocess.run(["python3", "./generator/ecomm_generator.py"], check=True)
    
    # Insert generated data into PostgreSQL database
    # Assuming ecomm_generator.py writes JSON files to the same directory
    with open("./generator/fake_users.json", "r") as f:
        users = json.load(f)
    with open("./generator/fake_clickstream_events.json", "r") as f:
        clickstream_events = json.load(f)
    with open("./generator/fake_transaction_data.json", "r") as f:
        transaction_data = json.load(f)
    with open("./generator/fake_google_search_data.json", "r") as f:
        google_search_data = json.load(f)
    with open("./generator/fake_email_marketing_data.json", "r") as f:
        email_marketing_data = json.load(f)
    with open("./generator/fake_facebook_data.json", "r") as f:
        facebook_data = json.load(f)

    # Insert data into PostgreSQL
    conn = psycopg2.connect(
        dbname="company_db",
        user="ecomm_company",
        password="ecomm_company",
        host="postgres_db",  # Ensure this matches your Docker service name
        port="5432"
    )
    cursor = conn.cursor()

    # Insert users
    for user in users:
        cursor.execute(
            "INSERT INTO users (username, email, address, phone_number, created_at) VALUES (%s, %s, %s, %s, %s)",
            (user["username"], user["email"], user["address"], user["phone_number"], datetime.now())
        )

    # Insert clickstream events
    for event in clickstream_events:
        cursor.execute(
            "INSERT INTO clickstream_events (user_id, timestamp, page_visited, action, product_id, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (event["user_id"], event["timestamp"], event["page_visited"], event["action"], event["product_id"], datetime.now())
        )

    # Insert transactions
    for transaction in transaction_data:
        cursor.execute(
            "INSERT INTO transactions (timestamp, amount, product_id, user_id, payment_method, transaction_type, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (transaction["timestamp"], transaction["amount"], transaction["product_id"], transaction["user_id"], transaction["payment_method"], transaction["transaction_type"], datetime.now())
        )

    # Insert Google search data
    for data in google_search_data:
        cursor.execute(
            "INSERT INTO google_search (date, campaign, ad, impressions, clicks, cost, conversions, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (data["date"], data["campaign"], data["ad"], data["impressions"], data["clicks"], data["cost"], data["conversions"], datetime.now())
        )

    # Insert email marketing data
    for data in email_marketing_data:
        cursor.execute(
            "INSERT INTO email_marketing (date, user_email, campaign, received, opened, subscribed, clicks, bounces, unsubscribed, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (data["date"], data["user_email"], data["campaign"], data["received"], data["opened"], data["subscribed"], data["clicks"], data["bounces"], data["unsubscribed"], datetime.now())
        )

    # Insert Facebook data
    for data in facebook_data:
        cursor.execute(
            "INSERT INTO facebook (platform, date, campaign, ad_unit, impressions, percent_watched, clicks, cost, conversions, likes, shares, comments, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (data["platform"], data["date"], data["campaign"], data["ad_unit"], data["impressions"], data["percent_watched"], data["clicks"], data["cost"], data["conversions"], data["likes"], data["shares"], data["comments"], datetime.now())
        )

    conn.commit()
    cursor.close()
    conn.close()

dag = DAG(
    'ecomm_data_pipeline',
    default_args=default_args,
    description='Generate and insert e-commerce data every 5-10 minutes',
    schedule_interval=timedelta(minutes=10),
)

generate_and_insert_data_task = PythonOperator(
    task_id='generate_and_insert_data',
    python_callable=generate_and_insert_data,
    dag=dag,
)

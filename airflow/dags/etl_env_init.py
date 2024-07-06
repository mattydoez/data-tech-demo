
from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'initialize_etl_environment',
    default_args=default_args,
    description='Initialize ETL Environment',
    schedule_interval='@daily',
    start_date=datetime(2024, 7, 2),
    tags=['init'],
    is_paused_upon_creation=False,
    wait_for_downstream=True
)


create_schemas_task = PostgresOperator(
    task_id='create_schemas',
    sql = """
    Create schema if not exists raw;
    Create schema if not exists staging;
    Create schema if not exists analytics;  
    Create schema if not exists ops;     
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_last_transfer_table_task = PostgresOperator(
    task_id='create_last_transfer_table',
    sql = """
    CREATE TABLE IF NOT EXISTS ops.last_transfer (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        last_transfer TIMESTAMP
    );
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_users_table_task = PostgresOperator(
    task_id='create_users_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.users (
        user_id INT PRIMARY KEY,
        username VARCHAR(50),
        email VARCHAR(100),
        address VARCHAR(255),
        phone_number VARCHAR(255),
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_transactions_table_task = PostgresOperator(
    task_id='create_transactions_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.transactions (
        transaction_id INT PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        product_id INT,
        user_id INT REFERENCES raw.users(user_id),
        payment_method VARCHAR(50),
        transaction_type INT,
        created_at TIMESTAMP
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_events_table_task = PostgresOperator(
    task_id='create_events_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.clickstream_events (
        event_id INT PRIMARY KEY,
        user_id INT REFERENCES raw.users(user_id),
        timestamp TIMESTAMP NOT NULL,
        page_visited VARCHAR(255),
        action VARCHAR(50),
        product_id INT,
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_products_table_task = PostgresOperator(
    task_id='create_products_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.products (
        product_id INT PRIMARY KEY,
        name VARCHAR(255),
        category VARCHAR(255),
        description VARCHAR(255),
        price DECIMAL,
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_google_search_table_task = PostgresOperator(
    task_id='create_google_search_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.google_search (
        id INT PRIMARY KEY,
        date DATE NOT NULL,
        campaign VARCHAR(100),
        ad VARCHAR(100),
        impressions INT,
        clicks INT,
        cost DECIMAL(10, 2),
        conversions INT,
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_facebook_table_task = PostgresOperator(
    task_id='create_facebook_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.facebook (
        id INT PRIMARY KEY,
        platform VARCHAR(50), -- 'Instagram' or 'Facebook'
        date DATE NOT NULL,
        campaign VARCHAR(100),
        ad_unit VARCHAR(100),
        impressions INT,
        percent_watched DECIMAL(5, 2),
        clicks INT,
        cost DECIMAL(10, 2),
        conversions INT,
        likes INT,
        shares INT,
        comments INT,
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)

create_email_marketing_table_task = PostgresOperator(
    task_id='create_email_marketing_table',
    sql = """
    CREATE TABLE IF NOT EXISTS raw.email_marketing (
        id INT PRIMARY KEY,
        date DATE NOT NULL,
        user_email VARCHAR(100),
        campaign VARCHAR(100),
        ad VARCHAR(100),
        received INT,
        opened INT,
        subscribed INT,
        clicks INT,
        bounces INT,
        unsubscribed INT,
        created_at TIMESTAMP 
    );
        
    """,
    dag=dag,
    postgres_conn_id = 'company_dw',
    autocommit = True
)


create_schemas_task >> create_last_transfer_table_task >> [create_users_table_task, create_transactions_table_task, create_events_table_task, create_products_table_task, create_google_search_table_task, create_facebook_table_task, create_email_marketing_table_task]


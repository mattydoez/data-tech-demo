from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'crm_sales_data_ingestion',
    default_args=default_args,
    description='Ingest CRM sales data into company_dw',
    schedule_interval=None,  # Set to None for manual trigger
)

def create_schema_if_not_exists():
    hook = PostgresHook(postgres_conn_id='company_dw')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS crm_sales_data;")
    conn.commit()
    cursor.close()
    conn.close()

def delete_existing_table(table_name):
    hook = PostgresHook(postgres_conn_id='company_dw')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS crm_sales_data.{table_name} CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()

def ingest_csv_to_postgres(file_path, table_name):
    hook = PostgresHook(postgres_conn_id='company_dw')
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = pd.read_csv(file_path)
    df_columns = list(df.columns)
    columns = ",".join(df_columns)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS crm_sales_data.{table_name} (
        {", ".join([f'{col} TEXT' for col in df_columns])}
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Inserting DataFrame to SQL Database
    for i, row in df.iterrows():
        sql = f"INSERT INTO crm_sales_data.{table_name} ({columns}) VALUES ({'%s, ' * (len(row) - 1)}%s)"
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

with dag:
    create_schema = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema_if_not_exists
    )

    delete_accounts = PythonOperator(
        task_id='delete_accounts',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'accounts'},
    )

    delete_products = PythonOperator(
        task_id='delete_products',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'products'},
    )

    delete_sales_pipeline = PythonOperator(
        task_id='delete_sales_pipeline',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'sales_pipeline'},
    )

    delete_sales_teams = PythonOperator(
        task_id='delete_sales_teams',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'sales_teams'},
    )

    ingest_accounts = PythonOperator(
        task_id='ingest_accounts',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/crm_sales_data/accounts.csv', 'table_name': 'accounts'},
    )

    ingest_products = PythonOperator(
        task_id='ingest_products',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/crm_sales_data/products.csv', 'table_name': 'products'},
    )

    ingest_sales_pipeline = PythonOperator(
        task_id='ingest_sales_pipeline',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/crm_sales_data/sales_pipeline.csv', 'table_name': 'sales_pipeline'},
    )

    ingest_sales_teams = PythonOperator(
        task_id='ingest_sales_teams',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'file_path': '/opt/airflow/data/crm_sales_data/sales_teams.csv', 'table_name': 'sales_teams'},
    )

    create_schema >> [delete_accounts, delete_products, delete_sales_pipeline, delete_sales_teams]
    delete_accounts >> ingest_accounts
    delete_products >> ingest_products
    delete_sales_pipeline >> ingest_sales_pipeline
    delete_sales_teams >> ingest_sales_teams
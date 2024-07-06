from datetime import timedelta, datetime
import pytz
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from custom_sensors import ExternalTaskWithinDaysSensor
import psycopg2
from airflow.utils.task_group import TaskGroup

AIRFLOW_CONN_COMPANY_DB = os.getenv('AIRFLOW_CONN_COMPANY_DB')
AIRFLOW_CONN_COMPANY_DW = os.getenv('AIRFLOW_CONN_COMPANY_DW')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

tables_to_update = ['users', 'transactions', 'clickstream_events', 'products', 'google_search', 'facebook', 'email_marketing']

dag = DAG(
    'import_main_data',
    default_args=default_args,
    description='Import Core Company Data From Raw Data',
    schedule_interval='@hourly',
    start_date=datetime(2024, 7, 2),
    is_paused_upon_creation=True,
    wait_for_downstream=True
)

wait_for_init = ExternalTaskWithinDaysSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',
    external_task_id=None,
    days=7,
    mode='poke',
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

def get_last_transfer_time(table_name):
    conn = psycopg2.connect(AIRFLOW_CONN_COMPANY_DW)
    cursor = conn.cursor()
    cursor.execute(f"SELECT last_transfer FROM ops.last_transfer WHERE table_name = '{table_name}' ORDER BY last_transfer DESC LIMIT 1")
    last_transfer = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return last_transfer

def update_last_transfer_time(table_name):
    conn = psycopg2.connect(AIRFLOW_CONN_COMPANY_DW)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO ops.last_transfer (table_name, last_transfer) VALUES ('{table_name}', NOW())")
    conn.commit()
    cursor.close()
    conn.close()

def generate_import_sql_script_command(table_name, id_col, col_list):
    script_path = "/opt/airflow/scripts/import_core_data.sh"
    return f"bash {script_path} {table_name} {id_col} '{col_list}' {AIRFLOW_CONN_COMPANY_DB} {AIRFLOW_CONN_COMPANY_DW}"


with dag:
    # Define user-related tasks within a task group
    with TaskGroup(group_id='user_related_tasks') as user_related_tasks:
        # Users transfer 
        generate_user_sql_task = PythonOperator(
            task_id='generate_user_sql',
            python_callable=generate_import_sql_script_command,
            op_args=['users', 'user_id', 'user_id, username, email, address, phone_number, created_at'],
            dag=dag,
        )

        import_users_task = BashOperator(
            task_id='export_and_load_user_data',
            bash_command="{{ task_instance.xcom_pull(task_ids='generate_user_sql') }}",
            env={
                'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
                'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
            },
            dag=dag,
        )

        update_last_user_transfer_task = PythonOperator(
            task_id='update_last_user_transfer_time',
            python_callable=update_last_transfer_time,
            op_args=['users'],
            dag=dag,
        )

        generate_user_sql_task >> import_users_task >> update_last_user_transfer_task

    # Transactions transfer 
    generate_transactions_sql_task = PythonOperator(
        task_id='generate_transactions_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['transactions', 'transaction_id', 'transaction_id, timestamp, product_id, user_id, payment_method, transaction_type, created_at'],
        dag=dag,
    )

    import_transactions_task = BashOperator(
        task_id='export_and_load_transaction_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_transactions_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_transactions_transfer_task = PythonOperator(
        task_id='update_last_transactions_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['transactions'],
        dag=dag,
    )

    # Products transfer 
    generate_products_sql_task = PythonOperator(
        task_id='generate_products_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['products', 'product_id', 'product_id, name, category, description, price, created_at'],
        dag=dag,
    )

    import_products_task = BashOperator(
        task_id='export_and_load_products_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_products_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_products_transfer_task = PythonOperator(
        task_id='update_last_products_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['products'],
        dag=dag,
    )

    # Clickstream Events transfer 
    generate_clickstream_events_sql_task = PythonOperator(
        task_id='generate_clickstream_events_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['clickstream_events', 'event_id', 'event_id, user_id, timestamp, page_visited, action, product_id, created_at'],
        dag=dag,
    )

    import_clickstream_events_task = BashOperator(
        task_id='export_and_load_clickstream_events_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_clickstream_events_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_clickstream_events_transfer_task = PythonOperator(
        task_id='update_last_clickstream_events_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['clickstream_events'],
        dag=dag,
    )

    # Google Search transfer 
    generate_google_search_sql_task = PythonOperator(
        task_id='generate_google_search_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['google_search', 'id', 'id, date, campaign, ad, impressions, clicks, cost, conversions, created_at'],
        dag=dag,
    )

    import_google_search_task = BashOperator(
        task_id='export_and_load_google_search_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_google_search_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_google_search_transfer_task = PythonOperator(
        task_id='update_last_google_search_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['google_search'],
        dag=dag,
    )

    #Facebook transfer 
    generate_facebook_sql_task = PythonOperator(
        task_id='generate_facebook_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['facebook', 'id', 'id, platform, date, campaign, ad_unit, impressions, percent_watched, clicks, cost, conversions, likes, shares, comments, created_at'],
        dag=dag,
    )

    import_facebook_task = BashOperator(
        task_id='export_and_load_facebook_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_facebook_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_facebook_transfer_task = PythonOperator(
        task_id='update_last_facebook_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['facebook'],
        dag=dag,
    )

    # Email Marketing transfer 
    generate_email_marketing_sql_task = PythonOperator(
        task_id='generate_email_marketing_sql',
        python_callable=generate_import_sql_script_command,
        op_args=['email_marketing', 'id', 'id, date, user_email, campaign, ad, received, opened, subscribed, clicks, bounces, unsubscribed, created_at'],
        dag=dag,
    )

    import_email_marketing_task = BashOperator(
        task_id='export_and_load_email_marketing_data',
        bash_command="{{ task_instance.xcom_pull(task_ids='generate_email_marketing_sql') }}",
        env={
            'AIRFLOW_CONN_COMPANY_DB': AIRFLOW_CONN_COMPANY_DB,
            'AIRFLOW_CONN_COMPANY_DW': AIRFLOW_CONN_COMPANY_DW,
        },
        dag=dag,
    )

    update_last_email_marketing_transfer_task = PythonOperator(
        task_id='update_last_email_marketing_transfer_time',
        python_callable=update_last_transfer_time,
        op_args=['email_marketing'],
        dag=dag,
    )

    # Set dependencies
    wait_for_init >> user_related_tasks

    user_related_tasks >> generate_products_sql_task >> import_products_task >> update_last_products_transfer_task
    user_related_tasks >> generate_transactions_sql_task >> import_transactions_task >> update_last_transactions_transfer_task
    user_related_tasks >> generate_clickstream_events_sql_task >> import_clickstream_events_task >> update_last_clickstream_events_transfer_task
    user_related_tasks >> generate_google_search_sql_task >> import_google_search_task >> update_last_google_search_transfer_task
    user_related_tasks >> generate_facebook_sql_task >> import_facebook_task >> update_last_facebook_transfer_task
    user_related_tasks >> generate_email_marketing_sql_task >> import_email_marketing_task >> update_last_email_marketing_transfer_task
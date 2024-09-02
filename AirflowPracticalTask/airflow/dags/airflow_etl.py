from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    dag_id='nyc_airbnb_etl',
    default_args=default_args,
    description='An ETL pipeline for NYC Airbnb data',
    schedule='@daily',
    start_date=datetime(2024, 8, 1),
    catchup=False,
)

nyc_airbnb_data_path = '/home/vadym/Documents/Projects/Data-Engineering/AirflowPracticalTask/raw/AB_NYC_2019.csv'
database_conn_id = 'postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/airflow_etl'

# Define Python functions for ETL tasks
def extract_data(**kwargs):
    data_path = kwargs['data_path']
    # Extraction logic here
    print(f"Extracting data from {data_path}")

def transform_data(**kwargs):
    # Transformation logic here
    print("Transforming data")

def load_data(**kwargs):
    conn_id = kwargs['conn_id']
    # Loading logic here
    print(f"Loading data into database using connection id {conn_id}")

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={'data_path': nyc_airbnb_data_path},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'conn_id': database_conn_id},
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
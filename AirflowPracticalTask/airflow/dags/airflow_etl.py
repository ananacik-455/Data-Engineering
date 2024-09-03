from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable

import psycopg2
import pandas as pd
import sys
import os

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

# REST OF SOLUTION IS IN RAW APPEARANCE
# FOR DEADLINE ITS ALL I HAVE WITHOUT README FILE
# FOR REST SOLUTION CHECK THIS GITHUB:
# https://github.com/ananacik-455/Data-Engineering/tree/main/AirflowPracticalTask
# CAN'T ADD COMENTS TO SUBMISSION :(
# SO DO IT THERE

# Retrieve configuration from Airflow Variables
base_path = Variable.get("base_path", default_var="/default/path")
raw_path = os.path.join(base_path, Variable.get("raw_path"))
transformed_path = os.path.join(base_path, Variable.get("transformed_path"))

if not os.path.exists(raw_path):
    os.makedirs(raw_path)
if not os.path.exists(transformed_path):
    os.makedirs(transformed_path)

database_conn = {
    "dbname": Variable.get("db_name"),
    "user": Variable.get("db_user"),
    "password": Variable.get("db_password"),
}


# Define Python functions for ETL tasks
def extract_data(**kwargs):
    data_path = kwargs['raw_path']
    # Extraction logic
    print("Extracting data")
    try:
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        raise ValueError(f"File {data_path} not found.")
    except Exception as e:
        raise ValueError(f"An error occured: {str(e)}")
    
    # Push the DataFrame to XCom
    kwargs['ti'].xcom_push(key='airflow_df', value=df.to_dict())

def transform_data(**kwargs):
    output_path = kwargs['output_path']
    # Transformation logic here
    print("Transforming data")
    df_dict = kwargs['ti'].xcom_pull(task_ids='extract_data', key='airflow_df')
    df = pd.DataFrame(df_dict)
    print(df.head())
    df = df[df['price'] > 0]
    df['last_review'] = pd.to_datetime(df['last_review'])
    df['last_review'].fillna(df['last_review'].min(), inplace=True)
    df['reviews_per_month'].fillna(0, inplace=True)
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df.to_csv(output_path, index=False)


def connect_to_db(database_conn):
    print("Connection to database")
    conn = psycopg2.connect(
        dbname=database_conn['dbname'],
        user=database_conn['user'],
        password=database_conn["password"]
        )
    return conn, conn.cursor()


def load_data(**kwargs):

    # Read transformed_data
    df = pd.read_csv(kwargs["transformed_path"])

    # Create connection to database
    conn, cur = connect_to_db(kwargs["database_conn"])

    # Loading logic
    print(f"Loading data into database")
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO airbnb_listings\
            (id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365)\
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(row)
        )
    conn.commit()
    conn.close()
    

def data_quality_check(**kwargs):
    # Create connection to database
    conn, cur = connect_to_db(kwargs["database_conn"])
    cur.execute("SELECT COUNT(*)\
                 FROM airbnb_listings")
    record_count = cur.fetchone()[0]
    if record_count == 0:
        raise ValueError("No data found in airbnb_listings table")
    
    # Read transformed_data
    df = pd.read_csv(kwargs["transformed_path"])
    if record_count != df.shape[0]:
        raise ValueError(f"Number of records in the PostgreSQL table [{record_count}] dont matches\
                           the expected number from the transformed CSV [{df.shape[0]}].")
       
    cur.execute("SELECT COUNT(*)\
                 FROM airbnb_listings \
                WHERE price IS NULL \
                OR minimum_nights IS NULL \
                OR availability_365 IS NULL")
    null_count = cur.fetchone()[0]
    if null_count > 0:
        raise ValueError(f"{null_count} records have NULL values in critical columns")
    
    conn.commit()
    conn.close()

# Define Airflow tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={'raw_path': raw_path},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'output_path': transformed_path},
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'database_conn': database_conn,
               'transformed_path': transformed_path},
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    op_kwargs={'database_conn': database_conn,
               'transformed_path': transformed_path},
    dag=dag,
)

def on_failure_callback(context):
    with open(os.path.join(base_path, 'airflow/logs/airflow_failures.log'), 'a') as f:
        f.write(f"{context['task_instance_key_str']} failed on {context['execution_date']}.\n")

# Attach the failure callback to tasks
extract_task.on_failure_callback = on_failure_callback
transform_task.on_failure_callback = on_failure_callback
load_task.on_failure_callback = on_failure_callback
quality_check_task.on_failure_callback = on_failure_callback

# Set task dependencies
extract_task >> transform_task >> load_task >> quality_check_task
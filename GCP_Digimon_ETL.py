from datetime import timedelta, datetime
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

# Create a DAG instance
dag = DAG(
    'BigQuery_Digimon_ETL', 
    default_args=default_args,
    description='A simple Airflow DAG to Load Digimon Dataset to BigQuery',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

# Task 1: Start Task (Dummy Operator)
start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

# Config variables
BQ_CONN_ID = "google_cloud_default"
BQ_PROJECT = "ringed-land-398802"
BQ_DATASET = "Digimon"
BQ_TABLE = "Digimon_List"

## Task BigQueryOperator: Delete existing table in big query
delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
    dag=dag
)

## Function ETL using Pandas GBQ
def etl_function():
    # Extract data into a DataFrame (example: CSV file)
    df = pd.read_csv('/opt/airflow/dags/dataset/Digimon_Dataset/DigiDB_digimonlist.csv')
    
    # Perform transformations (example: converting column names to lowercase)
    df.columns = df.columns.str.lower()
    
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/key_gcp/keyfile.json',
    )
    # Return df
    pandas_gbq.to_gbq(df, f'{BQ_DATASET}.{BQ_TABLE}', project_id=BQ_PROJECT, credentials=credentials)

## Task PythonOperator: Insert a new table from Dataframe
execute_insert_query = PythonOperator(
    task_id='execute_insert_query',
    python_callable=etl_function,
    dag=dag
)

## Task BigQueryOperator: check that the data table is existed in the dataset
bq_load_task = BigQueryOperator(
    task_id='bq_load_task',
    sql='''
    SELECT
      *
    FROM `ringed-land-398802.Digimon.Digimon_List`;
    ''',
    destination_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.Digimon_List',
    write_disposition='WRITE_TRUNCATE', # Choose WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
    allow_large_results=True,
    use_legacy_sql=False,
    gcp_conn_id=BQ_CONN_ID,  # Use your Airflow BigQuery connection ID
    dag=dag
)

# Define task
start_task >> delete_table >> execute_insert_query >> bq_load_task
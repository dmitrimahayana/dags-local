import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import timedelta, datetime
from moduleFlights.CollectFlights import Collect_Flights

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

## Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 0,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

## Create a DAG instance
dag = DAG(
    'BigQuery_Flight_ETL', 
    default_args=default_args,
    description='An Airflow DAG for BigQuery Flights Dataset',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

## Config variables
BQ_CONN_ID = "google_cloud_default"
BQ_PROJECT = "ringed-land-398802"
BQ_DATASET = "Flights"
BQ_TABLE = "Flight_List"

## Task BigQueryOperator: Delete existing table in big query
delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
    dag=dag
)

## Function ETL using Pandas GBQ
def extract_flight_data():
    LIMIT_JSON_FILE = 100000
    collect_obj = Collect_Flights('/opt/airflow/dags/repo/dataset/Revalue_Nature/Case 2/', LIMIT_JSON_FILE)
    df = collect_obj.collect_data()
    
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/key_gcp/keyfile.json',
    )
    # Return df
    pandas_gbq.to_gbq(df, f'{BQ_DATASET}.{BQ_TABLE}', project_id=BQ_PROJECT, credentials=credentials)

## Task PythonOperator: Insert a new table from Dataframe
extract_flight_task = PythonOperator(
    task_id='extract_flight_data',
    python_callable=extract_flight_data,
    dag=dag
)

## Task BigQueryOperator: check that the data table is existed in the dataset
bq_table_count = BigQueryCheckOperator(
    task_id="bq_table_count",
    sql=f"SELECT COUNT(*) FROM `{BQ_DATASET}.{BQ_TABLE}`;",
    use_legacy_sql=False,
    gcp_conn_id=BQ_CONN_ID,  # Use your Airflow BigQuery connection ID
    dag=dag
)
## Task BigQueryOperator: check that the data table is existed in the dataset
# bq_load_task = BigQueryOperator(
#     task_id='bq_load_task',
#     sql='''
#     SELECT
#       *
#     FROM `ringed-land-398802.Flights.Flight_List`;
#     ''',
#     destination_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.Flight_List',
#     write_disposition='WRITE_TRUNCATE', # Choose WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
#     allow_large_results=True,
#     use_legacy_sql=False,
#     gcp_conn_id=BQ_CONN_ID,  # Use your Airflow BigQuery connection ID
#     dag=dag
# )

## Define task
delete_table
extract_flight_task >> bq_table_count
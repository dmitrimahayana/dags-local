from datetime import timedelta, datetime
import pandas as pd

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from moduleFlights.CollectFlights import Collect_Flights
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer

## Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

## Create a DAG instance
dag = DAG(
    'Postgres_Snowflake_ETL',
    default_args=default_args,
    description='An Airflow DAG to perform ETL data from Postgres to Snowflake',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

## Variable Config Snowflake
SNOWFLAKE_CONN = "snowflake_default"
SNOWFLAKE_DB = "FLIGHTS"
SNOWFLAKE_SCHEMA = "FLIGHT_SCHEMA"
SNOWFLAKE_TABLE = "Flight_List"
SNOWFLAKE_USER = "NAINAFI"
SNOWFLAKE_PASS = "Cakcuk623827"
# SNOWFLAKE_PASS = Variable.get("snowflake_pass")
SNOWFLAKE_ACC = "IUDFCYJ-UD33577"

## Tas SnowflakeOperator: Create Table
# CREATE_TABLE_SQL_STRING = (
#     f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_TABLE} "
#     f"(id INT, "
#     f"aircraft_type VARCHAR(250), "
#     f"flight_rules VARCHAR(250), "
#     f"plots_altitude INT, "
#     f"plots_baro_vert_rate FLOAT, "
#     f"plots_mach FLOAT, "
#     f"plots_measured_flight_level FLOAT, "
#     f"start_time DATETIME, "
#     f"time_of_track DATETIME);"
# )
# snowflake_create_table = SnowflakeOperator(
#     task_id = "snowflake_create_table", 
#     sql = CREATE_TABLE_SQL_STRING,
#     snowflake_conn_id = SNOWFLAKE_CONN,
#     dag = dag
# )

## Function to Extract Data from Flight Dataset
def extract_flight_data():
    LIMIT_JSON_FILE = 1
    collect_obj = Collect_Flights('/opt/airflow/dataset/Revalue_Nature/Case 2/', LIMIT_JSON_FILE)
    df = collect_obj.collect_data()

    # Create snowflake connection
    conn_string = f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASS}@{SNOWFLAKE_ACC}/{SNOWFLAKE_DB}/{SNOWFLAKE_SCHEMA}"
    engine = create_engine(conn_string)

    # Convert df to snowflake data
    with engine.connect() as con:
            df.to_sql(name=SNOWFLAKE_TABLE, con=con, if_exists='replace', index=False, method=pd_writer)

## Task PythonOperator: Insert a new table from Dataframe
etl_flight_task = PythonOperator(
    task_id='etl_flight_task',
    python_callable=extract_flight_data,
    dag=dag
)

## Task SnowflakeOperator: Query Table
snowflake_count_row = SnowflakeOperator(
    task_id = "snowflake_count_row",
    sql = f'SELECT * FROM "{SNOWFLAKE_DB}"."{SNOWFLAKE_SCHEMA}"."{SNOWFLAKE_TABLE}"',
    snowflake_conn_id = SNOWFLAKE_CONN,
    dag = dag
)

## Define task
etl_flight_task >> snowflake_count_row
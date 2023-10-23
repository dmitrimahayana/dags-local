import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

# Create a DAG instance
dag = DAG(
    'AWS_RDS_Simple_EL', 
    default_args=default_args,
    description='An Airflow DAG for Brazilian Olist Dataset',
    schedule_interval='@monthly',  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

# Get Current Date
today = date.today()
current_date = today.strftime('%Y-%m-%d')

# Config variables
bucket_name = "brazilian-olist"
bucket_subfolder = "extraction/" + current_date + "/"
postgres_conn = "postgres_default"
aws_region = "ap-southeast-1"

# Define the Redshift PostgresOperator task
count_products_redshift_sql_task = PostgresOperator(
    task_id='count_products_redshift_sql_task',
    postgres_conn_id=postgres_conn,
    sql="SELECT * FROM PUBLIC.PRODUCTS;",
    autocommit=True,
    dag=dag,
)
# Define the Redshift PostgresOperator task
count_sellers_redshift_sql_task = PostgresOperator(
    task_id='count_sellers_redshift_sql_task',
    postgres_conn_id=postgres_conn,
    sql="SELECT * FROM PUBLIC.sellers;",
    autocommit=True,
    dag=dag,
)
# Define the Redshift PostgresOperator task
count_orders_redshift_sql_task = PostgresOperator(
    task_id='count_orders_redshift_sql_task',
    postgres_conn_id=postgres_conn,
    sql="SELECT * FROM PUBLIC.orders;",
    autocommit=True,
    dag=dag,
)
# Define the Redshift PostgresOperator task
count_order_items_redshift_sql_task = PostgresOperator(
    task_id='count_order_items_redshift_sql_task',
    postgres_conn_id=postgres_conn,
    sql="SELECT * FROM PUBLIC.order_items;",
    autocommit=True,
    dag=dag,
)
# Define the Redshift PostgresOperator task
count_order_payments_redshift_sql_task = PostgresOperator(
    task_id='count_order_payments_redshift_sql_task',
    postgres_conn_id=postgres_conn,
    sql="SELECT * FROM PUBLIC.order_payments;",
    autocommit=True,
    dag=dag,
)

# Define task
[count_products_redshift_sql_task, count_sellers_redshift_sql_task] >> count_order_items_redshift_sql_task
count_order_items_redshift_sql_task >> [count_orders_redshift_sql_task, count_order_payments_redshift_sql_task]
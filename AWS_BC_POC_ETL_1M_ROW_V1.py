import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 2,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=1),  # Time between retries
}

# Create a DAG instance
dag = DAG(
    'AWS_BC_POC_ETL_1M_ROW_V1',
    tags=["AWS_BC_POC"],
    default_args=default_args,
    description='An Airflow DAG for Brazilian Olist Dataset',
    schedule_interval='@monthly',  # Set the schedule interval (e.g., None for manual runs)
    catchup=False,  # Do not backfill (run past dates) when starting the DAG
    concurrency=5,  # Set the number of tasks to run concurrently
    max_active_runs=1,  # Set the maximum number of active DAG runs
)

# Get Current Date
today = date.today()
current_date = today.strftime('%Y-%m-%d')

# Config variables
bucket_name = "brazilian-olist"
bucket_subfolder = "extraction/" + current_date + "/"
postgres_conn = "postgres_default"
rdsmysql_conn = "rdsmysql_default"
aws_conn = "aws_default"
redshift_conn = "redshift_default"
redshift_dbname = "dev"
redshift_dbuser = "admin"
redshift_poll_interval = 10
redshift_postgres_conn = "redshift_postgres_default"
aws_region = "ap-southeast-1"

# Set the batch size
batch_size = 100000
# Initialize variables for pagination
offset = 0
total_rows = 1102860

# Loop until all rows are processed
while offset < total_rows:
    # Transfer SQL to S3
    sql_to_s3_1m_rows_table = SqlToS3Operator(
        task_id=f'1_mysql_to_s3_batch_{offset}',
        query=f'SELECT * FROM bc_poc_table1 LIMIT {batch_size} OFFSET {offset}',
        s3_bucket=bucket_name,
        s3_key=bucket_subfolder + "1m_rows_table1-" + str(current_date) + ".csv",
        replace=True,
        sql_conn_id=rdsmysql_conn,
        aws_conn_id=aws_conn,
        file_format='csv',
        pd_kwargs={'index': False},
        dag=dag,
    )
    offset += batch_size
    # sql_to_s3_1m_rows_table.set_upstream(previous_task)  # Set dependencies as needed
    
# # Transfer SQL to S3
# sql_to_s3_order_items = SqlToS3Operator(
#     task_id="sql_to_s3_order_items1",
#     query="SELECT * FROM order_items;",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "order-items1-" + str(current_date) + ".csv",
#     replace=True,
#     sql_conn_id=rdsmysql_conn,
#     aws_conn_id=aws_conn,
#     file_format='csv',
#     pd_kwargs={'index': False},
#     dag=dag,
# )
# # Transfer SQL to S3
# sql_to_s3_order_payments = SqlToS3Operator(
#     task_id="sql_to_s3_order_payments1",
#     query="SELECT * FROM order_payments;",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "order-payments1-" + str(current_date) + ".csv",
#     replace=True,
#     sql_conn_id=rdsmysql_conn,
#     aws_conn_id=aws_conn,
#     file_format='csv',
#     pd_kwargs={'index': False},
#     dag=dag,
# )
# # Transfer SQL to S3
# sql_to_s3_orders = SqlToS3Operator(
#     task_id="sql_to_s3_orders1",
#     query="SELECT * FROM orders;",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "orders1-" + str(current_date) + ".csv",
#     replace=True,
#     sql_conn_id=rdsmysql_conn,
#     aws_conn_id=aws_conn,
#     file_format='csv',
#     pd_kwargs={'index': False},
#     dag=dag,
# )


# # Transfer S3 to Redshit
# transfer_s3_to_redshift_orders = S3ToRedshiftOperator(
#     task_id="transfer_s3_to_redshift_orders",
#     schema="PUBLIC",
#     table="orders",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "orders-" + str(current_date) + ".csv",
#     redshift_conn_id=redshift_conn,
#     aws_conn_id=aws_conn,
#     copy_options=["FORMAT AS CSV", "DELIMITER ','", "QUOTE '\"'", 'IGNOREHEADER 1', f"REGION AS '{aws_region}'"],
#     method="REPLACE", # Use APPEND, UPSERT and REPLACE
#     # upsert_keys=[], # List of fields to use as key on upsert action
#     dag=dag,
# )
# # Transfer S3 to Redshit
# transfer_s3_to_redshift_order_items = S3ToRedshiftOperator(
#     task_id="transfer_s3_to_redshift_order_items",
#     schema="PUBLIC",
#     table="order_items",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "order-items-" + str(current_date) + ".csv",
#     redshift_conn_id=redshift_conn,
#     aws_conn_id=aws_conn,
#     copy_options=["FORMAT AS CSV", "DELIMITER ','", "QUOTE '\"'", 'IGNOREHEADER 1', f"REGION AS '{aws_region}'"],
#     method="REPLACE", # Use APPEND, UPSERT and REPLACE
#     # upsert_keys=[], # List of fields to use as key on upsert action
#     dag=dag,
# )
# # Transfer S3 to Redshit
# transfer_s3_to_redshift_order_payments = S3ToRedshiftOperator(
#     task_id="transfer_s3_to_redshift_order_payments",
#     schema="PUBLIC",
#     table="order_payments",
#     s3_bucket=bucket_name,
#     s3_key=bucket_subfolder + "order-payments-" + str(current_date) + ".csv",
#     redshift_conn_id=redshift_conn,
#     aws_conn_id=aws_conn,
#     copy_options=["FORMAT AS CSV", "DELIMITER ','", "QUOTE '\"'", 'IGNOREHEADER 1', f"REGION AS '{aws_region}'"],
#     method="REPLACE", # Use APPEND, UPSERT and REPLACE
#     # upsert_keys=[], # List of fields to use as key on upsert action
#     dag=dag,
# )


# # Define the Redshift PostgresOperator task
# count_orders_redshift_sql_task = PostgresOperator(
#     task_id='count_orders_redshift_sql_task',
#     postgres_conn_id=redshift_postgres_conn,
#     sql="SELECT * FROM PUBLIC.orders;",
#     autocommit=True,
#     database='dev',
#     dag=dag,
# )
# # Define the Redshift PostgresOperator task
# count_order_items_redshift_sql_task = PostgresOperator(
#     task_id='count_order_items_redshift_sql_task',
#     postgres_conn_id=redshift_postgres_conn,
#     sql="SELECT * FROM PUBLIC.order_items;",
#     autocommit=True,
#     database='dev',
#     dag=dag,
# )
# # Define the Redshift PostgresOperator task
# count_order_payments_redshift_sql_task = PostgresOperator(
#     task_id='count_order_payments_redshift_sql_task',
#     postgres_conn_id=redshift_postgres_conn,
#     sql="SELECT * FROM PUBLIC.order_payments;",
#     autocommit=True,
#     database='dev',
#     dag=dag,
# )

# Define task
# sql_to_s3_order_items
# sql_to_s3_order_payments
# sql_to_s3_orders
# sql_to_s3_order_items >> transfer_s3_to_redshift_order_items >> count_order_items_redshift_sql_task
# sql_to_s3_order_payments >> transfer_s3_to_redshift_order_payments >> count_order_payments_redshift_sql_task
# sql_to_s3_orders >> transfer_s3_to_redshift_orders >> count_orders_redshift_sql_task
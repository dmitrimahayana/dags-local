import pandas as pd
from datetime import timedelta, datetime, date

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Get Current Date
today = date.today()
current_date = today.strftime('%Y-%m-%d')

# Config variables
bucket_name = "brazilian-olist"
bucket_subfolder = "extraction/" + current_date + "/"
postgres_conn = "postgres_default"
rdsmysql_conn = "rdsmysql_default"
aws_conn = "aws_default"
aws_region = "ap-southeast-1"

with DAG(
    dag_id="Dynamic_Task_V1", 
    start_date=datetime(2022, 3, 4), 
    description='An Airflow DAG for Brazilian Olist Dataset',
    schedule_interval='@monthly', 
    catchup=False,
    ) as dag:

    @task()
    def upload_to_s3(**kwargs):
        sql_to_s3_task = SqlToS3Operator(
            task_id="sql_to_s3_order_items1",
            query="SELECT * FROM order_items;",
            s3_bucket=bucket_name,
            s3_key=bucket_subfolder + "order-items1-" + str(current_date) + ".csv",
            replace=True,
            sql_conn_id=rdsmysql_conn,
            aws_conn_id=aws_conn,
            file_format='csv',
            pd_kwargs={'index': False},
            dag=dag,
        )
        if sql_to_s3_task.execute(context={}) == 'failed':
            print("ERROR S3")
        else:
            print("NO ERROR S3")

    @task
    def get_row_data(**kwargs):
        get_data_task = MySqlOperator(
            task_id='get_data_task1',
            sql="SELECT COUNT(*) FROM bc_poc_table1 WHERE id_relasi < 10;",
            mysql_conn_id=rdsmysql_conn,
            dag=dag
        )

        ti = kwargs['ti']
        result = get_data_task.execute(context=ti.get_template_context())
        count_data = result[0][0]
        print("count_data:", count_data)
        
        batch_size = 20
        offset = 0
        total_rows = count_data
        list_offset = []
        while offset < total_rows:
            list_offset.append(offset)
            offset += batch_size
        
        
        print("list_offset:", list_offset)

        # # Push the result (assuming it's a single value) to XCom
        ti.xcom_push(key='count_data', value=count_data)
        ti.xcom_push(key='batch_size', value=batch_size)

        return list_offset

    @task
    def get_batch_data(offset: int, **kwargs):
        # Pull XCom value using the correct key
        ti = kwargs['ti']
        count_data = ti.xcom_pull(task_ids='get_row_data', key='count_data')
        batch_size = ti.xcom_pull(task_ids='get_row_data', key='batch_size')
        print("count_data:", count_data)
        print("offset:", offset)
        print("batch_size:", batch_size)

        # Transfer SQL to S3
        filename = bucket_subfolder + "1m_rows_table1-" + str(offset) + "-" + str(current_date) + ".csv"
        sql_to_s3_task = SqlToS3Operator(
            task_id=f'1_mysql_to_s3_batch_{offset}',
            query=f'SELECT * FROM bc_poc_table1 LIMIT {batch_size} OFFSET {offset}',
            s3_bucket=bucket_name,
            s3_key=filename,
            replace=True,
            sql_conn_id=rdsmysql_conn,
            aws_conn_id=aws_conn,
            file_format='csv',
            pd_kwargs={'index': False},
            dag=dag,
        )
        if sql_to_s3_task.execute(context={}) == 'failed':
            print("ERROR S3")
        else:
            print("NO ERROR S3")

    upload_to_s3()
    list_offset = get_row_data()
    get_batch_data.expand(offset=list_offset)
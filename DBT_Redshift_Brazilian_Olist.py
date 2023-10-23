from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator
)
from airflow.utils.dates import days_ago
import socket
print("HOSTNAME: ", socket.gethostname())

default_args = {
    'owner': 'Dmitri',
    'dir': '/opt/airflow/dags/Py-DBT-Redshift-BrazilianOlist',
    'start_date': days_ago(0)
}

with DAG(dag_id='DBT-Redshift-Brazilian_Olist', default_args=default_args, schedule_interval='@monthly') as dag:

  dbt_seed = DbtSeedOperator(
    task_id='dbt_seed',
  )

  # dbt_snapshot = DbtSnapshotOperator(
  #   task_id='dbt_snapshot',
  # )

  dbt_run = DbtRunOperator(
    task_id='dbt_run',
  )

  dbt_test = DbtTestOperator(
    task_id='dbt_test',
    retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
  )

  dbt_seed >> dbt_run >> dbt_test
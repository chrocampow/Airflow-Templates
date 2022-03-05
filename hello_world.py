from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG crisonew3!'

SQL = [
    """CREATE TABLE IF NOT EXISTS FIRST_TABLE (
        dt date,
        dag_id character varying,
        primary key (dt, dag_id)
    )"""
    ]

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    start_date=datetime(2022,3,3),
    catchup=False,
    schedule_interval='0 0 * * *'
) as dag:
    task1 = CloudSQLExecuteQueryOperator(
        task_id='create_postgres_table',
        gcp_cloudsql_conn_id='google_cloud_default',
        sql=SQL
    )
    hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator >> task1
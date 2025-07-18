from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello, Airflow!'

with DAG(
    dag_id='hello_airflow',
    default_args={'owner': 'airflow'},
    description='A simple DAG that prints Hello, Airflow!',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    hello_task = PythonOperator(
        task_id='hello_python',
        python_callable=print_hello,
    )
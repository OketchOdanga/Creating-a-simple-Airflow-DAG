from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_data():
    # Simulate extracting data from a CSV file
    data = ['customer1,address1', 'customer2,address2', 'customer3,address3']
    return data

def transform_data(data):
    # Simulate transforming the data
    transformed_data = [item.upper() for item in data]
    return transformed_data

def load_data(data):
    # Simulate loading the data into a database
    for item in data:
        print(f"Loading data: {item}")

with DAG(
    dag_id='datamart_pipeline',
    default_args={'owner': 'airflow'},
    description='A simple data pipeline for DataMart Inc.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['datamart'],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'data': extract_task.output} # This will not work, you need to use XComs
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'data': transform_task.output} # This will not work, you need to use XComs
    )

    extract_task >> transform_task >> load_task
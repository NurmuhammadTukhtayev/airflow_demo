from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from common.defaults import default_args

# simple function to return a age
def get_age():
    return 25

# function to return several parameters
def get_full_name(ti):
    ti.xcom_push(key='first', value='John')
    ti.xcom_push(key='last', value='Doe')

# function to pull XComs
def pull_xcom(ti):
    first = ti.xcom_pull(key='first', task_ids='get_full_name_task')
    last = ti.xcom_pull(key='last', task_ids='get_full_name_task')
    age = ti.xcom_pull(task_ids='get_age_task')

    print(f"Hello {first} {last}. You are {age} years old.")

# simple function to demonstrate passing parameters
def closing_message(status, msg):
    print(f"Status: {status}")
    print(msg)

# defining the DAG
with DAG(
    dag_id='xcom_demo',
    default_args=default_args(),
    description='A simple DAG to demonstrate XComs',
    start_date=datetime(2025, 9, 22),
    schedule='@daily'
) as dag:
    # task 1: get age
    task_1 = PythonOperator(
        task_id='get_age_task',
        python_callable=get_age,
        do_xcom_push=True
    )

    # task 2: get full name
    task_2 = PythonOperator(
        task_id='get_full_name_task',
        python_callable=get_full_name,
        do_xcom_push=True
    )

    # task 3: pull XComs
    task_3 = PythonOperator(
        task_id='pull_task',
        python_callable=pull_xcom,
        do_xcom_push=False
    )

    # task 4: closing message with parameters
    task_4 = PythonOperator(
        task_id='closing_task',
        python_callable=closing_message,
        op_kwargs={'status': 'Success', 'msg': 'DAG has completed successfully!'},
        do_xcom_push=False
    )

    # Setting up dependencies
    [task_1, task_2] >> task_3
    task_3 >> task_4

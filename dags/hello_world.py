from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from common.defaults import default_args

# defining the DAG
with DAG(
    dag_id='hello_world',
    default_args=default_args(),
    description='My first DAG',
    start_date=datetime(2025, 9, 22),
    schedule='@daily'
) as dag:
    task_1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world, this is my first dag!'
    )

    task_2 = BashOperator(
        task_id='second_task',
        bash_command='echo This is the second task'
    )

    task_3 = BashOperator(
        task_id='third_task',  
        bash_command='echo This is the third task'    
    )

    # Setting up dependencies using set_downstream
    # task_1.set_downstream(task_2)
    # task_1.set_downstream(task_3)

    # Setting up dependencies using bitshift operators
    task_1 >> task_2
    task_1 >> task_3

    # or 
    # task_1 >> [task_2, task_3]


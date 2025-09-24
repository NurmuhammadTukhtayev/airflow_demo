from common.defaults import default_args
from airflow.decorators import dag, task
from datetime import datetime

# defining the DAG using TaskFlow API
@dag(
    dag_id='taskflow_api_demo',
    default_args=default_args(),
    description='A simple DAG to demonstrate TaskFlow API',
    start_date=datetime(2025, 9, 22),
    schedule='@daily'
)
def taskflow_api_demo():
    @task()
    def get_age():
        return 25
    
    @task(multiple_outputs=True)
    def get_full_name():
        return {'first_name': 'John', 'last_name': 'Doe'}
    
    @task()
    def greeting(first_name:str, last_name:str, age: int):
        print(f"Hello {first_name} {last_name}. You are {age} years old.")

    person_age = get_age()
    person_name_dict = get_full_name()
    print(person_age, person_name_dict)
    greeting(first_name=person_name_dict['first_name'], last_name=person_name_dict['last_name'], age=person_age)

demo_dag = taskflow_api_demo()    

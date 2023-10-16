#Creating a DAG from the Python Operator Example 2
#Add a new task and make it print some custom text based on user input
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator



default_args = {
	'owner':'ImUser',
	'retries': 3,
	'retry_delay': timedelta(minutes=2)
}

def sampleTask():
    print("Yo! This is the first task using Python Operator in Airflow :)")

def printText(name, location):
    print(f"Hello There! I'm {name}, the owner of this repo."
          f"I am from {location}")


with DAG(
	dag_id = 'pythonOperator_example_2',
	default_args = default_args,
	description = 'This is my first python DAG in learning Airflow',
	start_date = datetime(2023,10,11,1),
	schedule_interval = '@daily'
) as dag:
    task_1 =  PythonOperator(
        task_id = 'first_task',
        python_callable=sampleTask,
		)
    task_2 =  PythonOperator(
        task_id = 'second_task',
        python_callable=printText,
        op_kwargs={'name':'Admin', 'location':'India'}
		)

task_1 >> task_2

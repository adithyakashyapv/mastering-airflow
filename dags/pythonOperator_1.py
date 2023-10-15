#Creating a DAG from the Python Operator Example 1
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


with DAG(
	dag_id = 'pythonOperator_example_1',
	default_args = default_args,
	description = 'This is my first python DAG in learning Airflow',
	start_date = datetime(2023,10,11,1),
	schedule_interval = '@daily'
) as dag:
    task_1 =  PythonOperator(
        task_id = 'first_task',
        python_callable=sampleTask
		)

task_1

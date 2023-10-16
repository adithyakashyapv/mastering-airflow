#Creating a DAG from the Python Operator Example 3
#Add a new task to pass the values to Xcom and reading it in the downstram task
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator



default_args = {
	'owner':'ImUser',
	'retries': 3,
	'retry_delay': timedelta(minutes=2)
}

def sampleTask():
    print("Hello There! I am running a python operator example task with XCOMS :)")

def get_name():
    return 'Kashyapa'
    

def printText(ti, location):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello There! I'm {name}, the owner of this repo."
          f"I am from {location}")


with DAG(
	dag_id = 'pythonOperator_example_3',
	default_args = default_args,
	description = 'This is my first python DAG in learning Airflow',
	start_date = datetime(2023,10,11,1),
	schedule_interval = '@daily'
) as dag:
    task_1 =  PythonOperator(
        task_id = 'first_task',
        python_callable=sampleTask,
		)
    task_2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    task_3 =  PythonOperator(
        task_id = 'second_task',
        python_callable=printText,
        op_kwargs={'location':'India'}
		)

task_1 >> task_2 >> task_3
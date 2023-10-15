#This script contains more than one bash operaton task
#This has Bitshifting method to run the DAG
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash import BashOperator



default_args = {
	'owner':'ImUser',
	'retries': 3,
	'retry_delay': timedelta(minutes=2)
}

with DAG(
	dag_id = 'bashOperator_example_3',
	default_args = default_args,
	description = 'This is my first DAG in learning Airflow',
	start_date = datetime(2023,10,11,1),
	schedule_interval = '@daily'
) as dag:
    task_1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world, this is the first task"
		)
    task_2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo Yo! this is the second task"
        )
    task_3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo Yo! this is the third task"
        )
    

task_1 >> [task_2, task_3]
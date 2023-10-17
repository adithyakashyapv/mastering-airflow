from airflow.decorators import dag, task

from datetime import datetime, timedelta


default_args = {
	'owner':'ImUser',
	'retries': 3,
	'retry_delay': timedelta(minutes=2)
}

@dag(
	dag_id='dag_with_taskflow_api_v1',
	default_args = default_args,
	start_date=datetime(2023, 10, 11),
	schedule_interval ='@daily'
	)

def etl_sample_1():

    @task()
    def get_name():
         return "Kashyapa"
    
    @task()
    def get_location():
        return "India"
    
    @task()
    def hello(name, location):
     print(f"Hello There! I am {name} the owner of this repo"
           f'I am from {location}')

	
    
    name = get_name()
    location = get_location()
    hello(name = name, location=location)
	

TaskFlowAPIDag = etl_sample_1()
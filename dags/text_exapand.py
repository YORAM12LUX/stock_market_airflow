import random

from airflow import DAG
from airflow.decorators import task 
from datetime import datetime


with DAG(
    dag_id = "my_dag",
    start_date = datetime(2023, 1, 1), 
    scheule_interval = "@daily",
    catchup = False,
) as dag:
    
    @task
    def get_files():

        return [f"file_{nb}" for nb in range(random.randint(3, 5))] 
    
    @task
    def download_file(folder:str,file_name: str):

        return f"{folder}/{file_name}"
    
    files = download_file.partial(folder="/include/test_data").expand(file_name=get_files())


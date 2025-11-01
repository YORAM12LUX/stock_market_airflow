from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, Airflow is working!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_airflow',
    default_args=default_args,
    description='Un DAG simple pour tester Airflow',
    schedule=timedelta(minutes=1),  # <-- ici la correction
    catchup=False,
    tags=['test'],
) as dag:

    task_hello = PythonOperator(
        task_id='say_hello',
        python_callable=hello_world,
    )

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, _load_to_dw,slack_alert_robust

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name=None,
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        },
        on_success_callback=lambda context: slack_alert_robust(context, success=True),
        on_failure_callback=lambda context: slack_alert_robust(context, success=False)
    )
    
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        },
        on_success_callback=lambda context: slack_alert_robust(context, success=True),
        on_failure_callback=lambda context: slack_alert_robust(context, success=False)
    )
    
    # Chargement des données vers le data warehouse
    load_to_dw_task = PythonOperator(
        task_id='load_to_dw',
        python_callable=_load_to_dw
    )

    @task
    def notify_success(**context):
        import requests

        try:
            connection = BaseHook.get_connection('slack_webhook')
            webhook_url = connection.host
            dag_id = context['dag'].dag_id
            execution_date = context['ds']  # ou context['execution_date'] pour un objet datetime

            
            #message = "*PIPELINE TERMINÉE AVEC SUCCÈS*  ✅ : `stock_market`"
                # Message Slack
            message = (
                f"*PIPELINE TERMINÉE AVEC SUCCÈS* ✅ \n"
                f"DAG: `{dag_id}`\n"
                f"Date d'exécution: `{execution_date}`"
            )

            payload = {"text": message}
            response = requests.post(webhook_url, json=payload, timeout=10)

            if response.status_code == 200:
                print("Notification Slack envoyée")
            else:
                print(f"Erreur Slack ❌ : {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Erreur dans notify_success ❌ : {str(e)}")

    notify = notify_success()

    
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw_task >> notify
        

dag = stock_market()
    
# Script de test complet
import requests
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_slack_complete():
    try:
        # Test 1: Connexion Airflow
        connection = BaseHook.get_connection('slack_webhook')
        webhook_url = connection.host
        print(f"✅ Connexion OK: {webhook_url[:50]}...")
        
        # Test 2: Envoi message
        payload = {"text": "🧪 Test complet Airflow → Slack"}
        response = requests.post(webhook_url, json=payload)
        
        if response.status_code == 200:
            print("✅ Message envoyé avec succès!")
            return True
        else:
            print(f"❌ Erreur: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Test échoué: {e}")
        return False


with DAG(
    dag_id='test_slack_complete',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "slack"],
) as dag:

    test_minio = PythonOperator(
        task_id='send_slack',
        python_callable=test_slack_complete,
    )
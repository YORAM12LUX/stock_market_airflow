from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO
import requests
from airflow.exceptions import AirflowNotFoundException



BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _get_stock_prices(url, symbol):
    
    
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    # Sécurité : désérialiser uniquement si c'est une string
    if isinstance(stock, str):
        stock = json.loads(stock)

    client = _get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')

    # Envoi dans MinIO
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{symbol}'

    


def _get_formatted_csv(path):
    client = _get_minio_client()
    symbol = path.split('/')[-1]
    prefix = f"{symbol}/formatted_prices/"

    objects = list(client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True))

    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name

    raise AirflowNotFoundException('The CSV file does not exist in MinIO.')



def _load_to_dw(**context):
    """
    Fonction pour charger les données depuis S3/MinIO vers PostgreSQL
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from io import StringIO
    import logging
    
    logging.info("Starting data load to data warehouse")
    
    try:
        # Récupération des informations de connexion
        postgres_conn = BaseHook.get_connection('postgres')
        
        # Configuration MinIO avec votre fonction
        minio_client = _get_minio_client()
        
        # Test de connexion MinIO
        try:
            buckets = minio_client.list_buckets()
            logging.info(f"Successfully connected to MinIO. Available buckets: {[b.name for b in buckets]}")
        except Exception as e:
            logging.error(f"Failed to connect to MinIO: {str(e)}")
            raise
        
        # Récupération du chemin du fichier
        csv_file_path = context['ti'].xcom_pull(task_ids='get_formatted_csv')
        logging.info(f"Loading file: {csv_file_path}")
        
        # Vérification que le fichier existe
        try:
            stat = minio_client.stat_object(BUCKET_NAME, csv_file_path)
            logging.info(f"File exists in bucket {BUCKET_NAME}, size: {stat.size} bytes")
        except Exception as e:
            logging.error(f"File not found: {str(e)}")
            raise
        
        # Lecture du fichier CSV depuis MinIO
        try:
            response = minio_client.get_object(BUCKET_NAME, csv_file_path)
            csv_content = response.read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            logging.info(f"Read {len(df)} rows from CSV")
            logging.info(f"Columns: {list(df.columns)}")
        finally:
            response.close()
            response.release_conn()
        
        # Connexion PostgreSQL
        engine = create_engine(
            f"postgresql://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/postgres"
        )

        
        # Chargement des données
        df.to_sql('stock_market', engine, schema='public', if_exists='append', index=False)
        logging.info(f"Successfully loaded {len(df)} rows to stock_market table")
        
        return f"Loaded {len(df)} rows to stock_market table"
        
    except Exception as e:
        logging.error(f"Error loading data to warehouse: {str(e)}")
        raise






def slack_alert_robust(context, success=True):
    """
    Version robuste qui fonctionne dans tous les contextes
    """
    try:
        print(f"🔍 SLACK CALLBACK TRIGGERED - Success: {success}")
        print(f"🔍 Context keys: {list(context.keys())}")
        
        # Récupérer l'URL du webhook
        try:
            connection = BaseHook.get_connection('slack_webhook')
            webhook_url = connection.host
            print(f"Webhook URL récupérée: {webhook_url[:50]}...")
        except Exception as e:
            print(f"Erreur récupération webhook: {e}")
            return
        
        # Extraire les informations du contexte de manière sécurisée
        dag_id = context.get('dag_run', {}).dag_id if context.get('dag_run') else context.get('dag', {}).dag_id
        task_instance = context.get('task_instance')
        
        if not dag_id and context.get('dag'):
            dag_id = context['dag'].dag_id
            
        task_id = task_instance.task_id if task_instance else "unknown"
        execution_date = context.get('execution_date', 'unknown')
        
        print(f"🔍 DAG: {dag_id}, Task: {task_id}, Date: {execution_date}")
        
        # Construire le message
        status_emoji = "✅" if success else "❌"
        status_text = "SUCCÈS" if success else "ÉCHEC"
        
        message = f"{status_emoji} *{status_text}*\n"
        message += f"DAG: `{dag_id}`\n"
        message += f"Tâche: `{task_id}`\n"
        message += f"Date: {execution_date}"
        
        # Envoi du message
        payload = {"text": message}
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Message Slack envoyé avec succès!")
        else:
            print(f"❌ Erreur Slack: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ ERREUR CRITIQUE SLACK CALLBACK: {str(e)}")
        import traceback
        traceback.print_exc()
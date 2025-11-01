from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from minio import Minio
from io import BytesIO
import json

def test_minio_connection():
    # Récupération de la connexion Airflow (conn_id = minio)
    conn = BaseHook.get_connection('minio')
    endpoint = conn.extra_dejson['endpoint_url'].split('//')[1]

    # Client MinIO
    client = Minio(
        endpoint=endpoint,
        access_key=conn.login,
        secret_key=conn.password,
        secure=False
    )

    # Nom du bucket
    bucket_name = "test-bucket"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Contenu à uploader
    data = "Ceci est un test MinIO depuis Airflow"
    encoded = BytesIO(data.encode('utf-8'))

    # Upload dans MinIO
    client.put_object(
        bucket_name=bucket_name,
        object_name="test.txt",
        data=encoded,
        length=len(data)
    )
    print("✔️ Fichier test.txt envoyé dans test-bucket sur MinIO")

with DAG(
    dag_id='test_minio_connection',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "minio"],
) as dag:

    test_minio = PythonOperator(
        task_id='send_test_file_to_minio',
        python_callable=test_minio_connection,
    )

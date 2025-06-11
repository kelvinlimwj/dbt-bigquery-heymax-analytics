import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts')))

from gcs_to_bq_utils import load_latest_csv_to_bq

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1
}

with DAG(
    dag_id='load_latest_gcs_csv_to_bigquery',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['gcs', 'bigquery']
) as dag:

    load_task = PythonOperator(
        task_id='load_latest_csv',
        python_callable=load_latest_csv_to_bq,
        op_kwargs={
            "project_id": "your-project-id",
            "dataset_id": "your-dataset-id",
            "table_id": "your-table-name",
            "bucket_name": "your-bucket-name",
            "prefix": "event_stream/"
        }
    )
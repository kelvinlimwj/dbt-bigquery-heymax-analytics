from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from gcs_to_bq_utils import load_latest_csv_to_bq

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='load_latest_gcs_csv_to_bigquery',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # or '@once' for testing
    catchup=False,
    tags=["gcs", "bigquery", "elt"]
) as dag:

    load_task = PythonOperator(
        task_id='load_latest_csv',
        python_callable=load_latest_csv_to_bq,
        op_kwargs={
            "project_id": "heymax-kelvin-analytics",
            "dataset_id": "heymax_analytics",
            "table_id": "event_stream_raw",
            "bucket_name": "heymax_kelvin_raw_data_sg",
            "prefix": "event_stream/"
        }
    )
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="test_gcs_logging_dag",
    start_date=datetime(2025, 6, 11),
    schedule_interval=None,
    catchup=False,
    tags=["test", "logging"],
) as dag:

    test_log = DummyOperator(
        task_id="log_this_task"
    )
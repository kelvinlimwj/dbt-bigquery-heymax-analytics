from google.cloud import bigquery
import pandas as pd
import os

def gcs_to_bq(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    
    if not file_name.endswith('.csv') or not file_name.startswith('event_stream_data/'):
        return

    uri = f'gs://{bucket_name}/{file_name}'
    bq_client = bigquery.Client()
    
    table_id = "dbt-analytics-heymax.heymax_analytics.event_stream_raw"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )
    load_job.result() 

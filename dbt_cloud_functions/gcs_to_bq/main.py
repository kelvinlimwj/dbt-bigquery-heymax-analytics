from google.cloud import bigquery
import uuid

def gcs_to_bq(event, context):
    client = bigquery.Client()

    bucket = event['bucket']
    file_name = event['name']
    dataset_id = "heymax_analytics"
    table_id = "event_stream_raw"
    project_id = "dbt-analytics-heymax"

    if not file_name.endswith('.csv') or not file_name.startswith('event_stream_data/'):
        print("Not a valid CSV file or not in expected folder. Skipping.")
        return

    uri = f"gs://{bucket}/{file_name}"
    temp_table = f"{dataset_id}.temp_{uuid.uuid4().hex[:8]}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE"
    )

    load_job = client.load_table_from_uri(uri, f"{project_id}.{temp_table}", job_config=job_config)
    load_job.result()
    print(f"Loaded {uri} into temporary table {temp_table}")

    temp_schema = [field.name for field in client.get_table(f"{project_id}.{temp_table}").schema]
    target_schema = [field.name for field in client.get_table(f"{project_id}.{dataset_id}.{table_id}").schema]
    common_columns = list(set(temp_schema) & set(target_schema))

    if not common_columns:
        print("No matching columns between staging and target table. Aborting merge.")
        return

    select_cols = ", ".join([f"`{col}`" for col in common_columns])
    merge_condition = " AND ".join([f"T.{col} = S.{col}" for col in common_columns])

    merge_sql = f"""
    MERGE `{project_id}.{dataset_id}.{table_id}` T
    USING (
      SELECT {select_cols}
      FROM `{project_id}.{temp_table}`
    ) S
    ON {merge_condition}
    WHEN NOT MATCHED THEN
      INSERT ({select_cols})
      VALUES ({select_cols})
    """

    client.query(merge_sql).result()
    print("Merge completed based on full column match.")

    client.delete_table(f"{project_id}.{temp_table}")
    print(f"Temporary table {temp_table} deleted.")

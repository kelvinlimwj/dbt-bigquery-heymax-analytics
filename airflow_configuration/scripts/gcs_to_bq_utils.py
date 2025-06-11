import os
import uuid
from google.cloud import bigquery, storage

def load_latest_csv_to_bq(project_id, dataset_id, table_id, bucket_name, prefix):
    bq_client = bigquery.Client()
    storage_client = storage.Client()

    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))

    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]

    if not csv_blobs:
        print("No CSV files found.")
        return

    latest_blob = sorted(csv_blobs, key=lambda b: b.updated or b.time_created, reverse=True)[0]
    file_name = latest_blob.name
    uri = f"gs://{bucket_name}/{file_name}"
    print(f"Latest CSV file found: {file_name}")

    temp_table = f"{dataset_id}.temp_{uuid.uuid4().hex[:8]}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE"
    )

    load_job = bq_client.load_table_from_uri(uri, f"{project_id}.{temp_table}", job_config=job_config)
    load_job.result()
    print(f"Loaded {uri} into temporary table {temp_table}")

    temp_cols = [field.name for field in bq_client.get_table(f"{project_id}.{temp_table}").schema]
    target_cols = [field.name for field in bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}").schema]

    new_columns = list(set(temp_cols) - set(target_cols))
    if new_columns:
        print(f"New columns not in target table: {new_columns}")

    common_cols = list(set(temp_cols) & set(target_cols))
    if not common_cols:
        print("No matching columns between temp and target. Aborting merge.")
        return

    select_cols = ", ".join([f"`{col}`" for col in common_cols])
    merge_condition = " AND ".join([f"T.{col} = S.{col}" for col in common_cols])

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

    bq_client.query(merge_sql).result()
    print("Merge completed.")

    bq_client.delete_table(f"{project_id}.{temp_table}")
    print(f"Deleted temporary table {temp_table}")

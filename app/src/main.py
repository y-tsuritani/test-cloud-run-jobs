import os  # noqa: D100

from google.cloud import bigquery, logging, storage  # noqa: D100

logging_client = logging.Client()
logger = logging_client.logger("export-table-to-gcs")


def get_tables(bq_client: bigquery.Client, dataset_id: str) -> list[str]:
    """Get table IDs in the dataset.

    Args:
        bq_client (bigquery.client): BigQuery client
        dataset_id (str): Dataset ID

    Returns:
        list[str]: Table IDs
    """
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        tables = bq_client.list_tables(dataset_ref)
    except Exception as e:
        logger.log_text(f"Failed to list tables in {dataset_id}: {e}", severity="ERROR")
        raise e

    return sorted([table.table_id for table in tables])


def export_table_to_gcs(
    bq_client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    destination_uri: str,
) -> None:
    """Export BigQuery table to GCS.

    Args:
        bq_client (bigquery.client): BigQuery client
        dataset_id (str): Dataset ID
        table_id (str): Table ID
        destination_uri (str): GCS URI
    """
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
    except Exception as e:
        logger.log_text(f"Failed to get table reference: {e}", severity="ERROR")
        raise e

    try:
        extract_job = bq_client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )
        extract_job.result()  # 処理が完了するまで待機
    except Exception as e:
        logger.log_text(f"Failed to extract {table_id}: {e}", severity="ERROR")
        raise e
    logger.log_text(f"Exported {table_id} to {destination_uri}", severity="INFO")


def cleanup_gcs_files(gcs_client: storage.Client, bucket_name: str, table_name: str) -> None:
    """Delete all files in GCS.

    Args:
        gcs_client (storage.Client): GCS client
        bucket_name (str): Bucket name
        table_name (str): Table name
    """
    try:
        bucket = gcs_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=f"{table_name}/")
        if not blobs:
            logger.log_text(f"No files in {bucket_name}", severity="INFO")
            return
        for blob in blobs:
            blob.delete()
    except Exception as e:
        logger.log_text(f"Failed to delete files in {bucket_name}: {e}", severity="ERROR")
        raise e
    logger.log_text(f"Deleted all files in {bucket_name}", severity="INFO")


def main() -> str:
    """Export a table to GCS.

    Returns:
        str: A message
    """
    project_id = os.environ.get("PROJECT_ID")
    dataset_id = os.environ.get("SOURCE_DATASET")
    bucket_name = os.environ.get("DESTINATION_BUCKET")

    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    logger.log_text(f"Connected to BigQuery: {bq_client}", severity="INFO")
    print(f"project_id: {project_id}")
    print(f"source_dataset: {dataset_id}")
    print(f"bq_client: {bq_client}")
    tables = get_tables(bq_client, dataset_id)
    logger.log_text(f"Tables in {dataset_id}: {tables}", severity="INFO")

    # TASK_INDEX環境変数でタスクのインデックスを取得
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
    logger.log_text(f"Task index: {task_index}", severity="INFO")

    # タスクインデックスに対応するテーブルを取得
    table_id = tables[task_index]
    # 前回のエクスポートファイルを削除
    cleanup_gcs_files(gcs_client, bucket_name, table_id)
    # エクスポートファイルは1ファイル1GBまでなので、複数ファイルに分割してエクスポート
    destination_uri = f"gs://{bucket_name}/{table_id}/{table_id}_*.csv"
    export_table_to_gcs(bq_client, dataset_id, table_id, destination_uri)
    logger.log_text(f"Exported {table_id} to {destination_uri}", severity="INFO")

    return f"Exported {table_id} to {destination_uri}"


if __name__ == "__main__":
    main()

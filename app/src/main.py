"""This module contains functions to interact with Google BigQuery and export tables to GCS.

The module provides functions to get table IDs from a BigQuery dataset and export tables to Google Cloud Storage.
"""  # noqa: E501

import os

from google.cloud import bigquery


def get_tables(bq_client: bigquery.client, dataset_id: str) -> list[str]:
    """Get table IDs in the dataset.

    Args:
        bq_client (bigquery.client): BigQuery client
        dataset_id (str): Dataset ID

    Returns:
        list[str]: Table IDs
    """
    dataset_ref = bq_client.dataset(dataset_id)
    tables = bq_client.list_tables(dataset_ref)

    return sorted([table.table_id for table in tables])


def export_table_to_gcs(
    bq_client: bigquery.client,
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
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        location="US",
    )

    extract_job.result()  # 処理が完了するまで待機
    print(f"Exported {table_id} to {destination_uri}")


def main() -> str:
    """Export a table to GCS.

    Returns:
        str: A message
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("SOURCE_DATASET_ID")
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    bq_client = bigquery.Client(project=project_id)
    tables = get_tables(bq_client, dataset_id)

    # TASK_INDEX環境変数でタスクのインデックスを取得
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))

    # タスクインデックスに対応するテーブルを取得
    table_id = tables[task_index]
    destination_uri = f"gs://{bucket_name}/{table_id}_*.csv"

    export_table_to_gcs(project_id, dataset_id, table_id, destination_uri)

    return f"Exported {table_id} to {destination_uri}"

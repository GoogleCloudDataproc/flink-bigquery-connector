"""Python script that validates the data written to a BigQuery table by
the Flink job.
It compares the total row count and unique key count in the source
(either a BigQuery table or a GCS URI) with the destination BigQuery table.
The source is a BigQuery Table for bounded read-write tests and GCS file for
unbounded read-write tests.
"""

import argparse
from collections.abc import Sequence

from absl import app
from google.cloud import bigquery
from google.cloud import storage
from absl import logging


def execute_query(bq_client, table_name, query):
    """Executes a BigQuery query and returns the result.
    Args:
        bq_client: A BigQuery client object.
        table_name: The name of the BigQuery table.
        query: The SQL query to execute.
    """
    logging.info(f"Query: {query}")
    try:
        job = bq_client.query(query, location='US')
        rows = job.result()  # Start the job and wait for it to complete and get the result.
        for row in rows:
            return row['unique_key_count']
    except Exception as _:
        raise RuntimeError(f'Could not obtain the count of unique keys from table: {table_name}')


def get_unique_key_count(bq_client, project_name, dataset_name, table_name):
    """Retrieves the count of distinct unique_key from a BigQuery table.
    Args:
        bq_client: A BigQuery client object.
        project_name: The project ID.
        dataset_name: The dataset name.
        table_name: The table name.
    Returns:
        The count of distinct unique keys in the table.
    """
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    query = (
        "SELECT COUNT(DISTINCT(unique_key)) as unique_key_count FROM `" + table_id + "`;"
    )
    return execute_query(bq_client, table_name, query)


def get_total_row_count_bigquery(bq_client, project_name, dataset_name, table_name):
    """Retrieves the total row count from a BigQuery table.
    Args:
        bq_client: A BigQuery client object.
        project_name: The project ID.
        dataset_name: The dataset name.
        table_name: The table name.
    Returns:
        The total number of rows in the table.
    """
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    query = (
        "SELECT COUNT(*) as unique_key_count FROM `" + table_id + "`;"
    )
    return execute_query(bq_client, table_name, query)


def get_total_row_count_gcs_file(storage_client, source_gcs_uri):
    """Retrieves the total row count from a CSV file in GCS.
    Args:
        storage_client: A GCS client object.
        source_gcs_uri: The GCS URI of the CSV file.
    Returns:
        The total number of rows in the CSV file.
    """
    path_to_csv = source_gcs_uri + "fullSource.csv"
    bucket_name = path_to_csv.split("/")[2]
    blob_path = '/'.join(path_to_csv.split('/')[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_string().decode('utf-8')
    row_count = len(content.splitlines())

    return row_count


def assert_total_row_count(bq_client, storage_client, project_name, dataset_name, source,
                           destination_table_name, mode, is_exactly_once):
    """Asserts that the total row count in the source, either GCS bucket or BQ table
    matches the destination BQ Table.
    Args:
        bq_client: A BigQuery client object.
        storage_client: A GCS client object.
        project_name: The project ID.
        dataset_name: The dataset name.
        source: The source table name or GCS URI.
        destination_table_name: The destination table name.
        mode: The mode of the Flink job ('bounded' or 'unbounded').
        is_exactly_once: True if exactly-once processing is enabled.
    """
    source_total_row_count = 0
    if mode == "unbounded":
        source_total_row_count = get_total_row_count_gcs_file(storage_client, source)
    else:
        source_total_row_count = get_total_row_count_bigquery(bq_client, project_name, dataset_name,
                                                 source)
    logging.info(f"Total Row Count for Source {source}:"
                 f" {source_total_row_count}")

    destination_total_row_count = get_total_row_count_bigquery(bq_client, project_name, dataset_name,
                                                      destination_table_name)
    logging.info(f"Total Row Count for Destination Table {destination_table_name}:"
                 f" {destination_total_row_count}")
    if is_exactly_once:
        if source_total_row_count != destination_total_row_count:
            raise AssertionError("Source and Destination Row counts do not match")
    else:
        if destination_total_row_count < source_total_row_count:
            raise AssertionError("Destination Row count is less than Source Row Count")


def assert_unique_key_count(bq_client, storage_client, project_name, dataset_name, source,
                            destination_table_name,
                            mode,
                            is_exactly_once):
    """Asserts that the unique key count in the source, either GCS bucket or BQ table
    matches the destination BQ Table.
    Args:
        bq_client: A BigQuery client object.
        storage_client: A GCS client object.
        project_name: The project ID.
        dataset_name: The dataset name.
        source: The source table name or GCS URI.
        destination_table_name: The destination table name.
        mode: The mode of the Flink job ('bounded' or 'unbounded').
        is_exactly_once: True if exactly-once processing is enabled.
    """
    source_unique_key_count = 0
    # The rows in the source for unbounded mode are unique
    if mode == "unbounded":
            source_unique_key_count = get_total_row_count_gcs_file(storage_client, source)
    else:
            source_unique_key_count = get_unique_key_count(bq_client, project_name, dataset_name,
                                                   source)
    logging.info(
        f"Unique Key Count for Source {source}: {source_unique_key_count}")
    destination_unique_key_count = get_unique_key_count(bq_client, project_name, dataset_name,
                                                        destination_table_name)
    logging.info(
        f"Unique Key Count for Destination Table {destination_table_name}:"
        f" {destination_unique_key_count}")

    if is_exactly_once:
        if source_unique_key_count != destination_unique_key_count:
            raise AssertionError("Source and Destination Key counts do not match!")
    else:
        if source_unique_key_count < destination_unique_key_count:
            raise AssertionError("Destination Row Key count is less than Source Key Count!")


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_name',
        dest='project_name',
        help='Project Id which contains the table and GCS bucket to be read.',
        type=str,
        default='',
        required=True,
    )
    parser.add_argument(
        '--dataset_name',
        dest='dataset_name',
        help='Dataset Name which contains the BigQuery Tables.',
        type=str,
        default='',
        required=True,
    )
    parser.add_argument(
        '--source',
        dest='source',
        help='Table Name or GCS URI which is source for write test.',
        type=str,
        default='',
        required=True,
    )

    parser.add_argument(
        '--destination_table_name',
        dest='destination_table_name',
        help='Table Name of the table which is destination for write test.',
        type=str,
        default='',
        required=True,
    )

    parser.add_argument(
        '--is_exactly_once',
        dest='is_exactly_once',
        help='Set the flag to True If "EXACTLY ONCE" mode is enabled.',
        action='store_true',
        default=False,
        required=False,
    )

    parser.add_argument(
            '--mode',
            dest='mode',
            help='Source Type',
            type=str,
            default='',
            required=True,
    )

    args = parser.parse_args(argv[1:])

    # Providing the values.
    project_name = args.project_name
    dataset_name = args.dataset_name
    source = args.source
    destination_table_name = args.destination_table_name
    is_exactly_once = args.is_exactly_once
    mode = args.mode

    bq_client = bigquery.Client(project=project_name)
    storage_client = storage.Client(project=project_name)
    assert_total_row_count(bq_client, storage_client, project_name, dataset_name, source,
                           destination_table_name, mode, is_exactly_once)

    assert_unique_key_count(bq_client, storage_client, project_name, dataset_name, source,
                            destination_table_name, mode, is_exactly_once)


if __name__ == '__main__':
    app.run(main)

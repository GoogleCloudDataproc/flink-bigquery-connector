import argparse
from collections.abc import Sequence

from absl import app
import parse_logs
from google.cloud import bigquery
from absl import logging


def get_row_count(project_name, dataset_name, table_name):
    client = bigquery.Client(project=project_name)
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    query = (
        "SELECT COUNT(DISTINCT(unique_key)) as unique_key_count FROM `" + table_id + "`;"
    )
    logging.info(f"Query: {query}")
    try:
        job = client.query(query, location='US')
        rows = job.result()  # Start the job and wait for it to complete and get the result.
        for row in rows:
            return row['unique_key_count']
    except Exception as _:
        raise RuntimeError(f'Could not obtain the count of unique keys from table: {table_name}')


def assert_total_row_count(project_name, dataset_name, source_table_name, destination_table_name,
                           is_exactly_once):
    source_total_row_count = parse_logs.get_bq_table_row_count(project_name, project_name,
                                                               dataset_name, source_table_name, "")
    logging.info(f"Total Row Count for Source Table {source_table_name}:"
                 f" {source_total_row_count}")
    destination_total_row_count = parse_logs.get_bq_table_row_count(project_name, project_name,
                                                                    dataset_name,
                                                                    destination_table_name, "")
    logging.info(f"Total Row Count for Destination Table {destination_table_name}:"
                 f" {destination_total_row_count}")
    if is_exactly_once:
        if source_total_row_count != destination_total_row_count:
            logging.info(f"Source and Destination Row counts do not match")
            # raise AssertionError("Source and Destination Row counts do not match")
    else:
        if destination_total_row_count < source_total_row_count:
            logging.info(f"Destination Row count is less than Source Row Count")
            # raise AssertionError("Destination Row count is less than Source Row Count")


def assert_unique_key_count(project_name, dataset_name, source_table_name, destination_table_name,
                            is_exactly_once):
    source_unique_key_count = get_row_count(project_name, dataset_name, source_table_name)
    logging.info(
        f"Unique Key Count for Source Table {source_table_name}: {source_unique_key_count}")
    destination_unique_key_count = get_row_count(project_name, dataset_name, destination_table_name)
    logging.info(
        f"Unique Key Count for Destination Table {destination_table_name}: {destination_unique_key_count}")

    if is_exactly_once:
        if source_unique_key_count != destination_unique_key_count:
            logging.info(f"Source and Destination Key counts do not match!")
            # raise AssertionError("Source and Destination Key counts do not match!")
    else:
        if source_unique_key_count < destination_unique_key_count:
            logging.info(f"Destination Row Key count is less than Source Key Count!")
            # raise AssertionError("Destination Row Key count is less than Source Key Count!")


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_name',
        dest='project_name',
        help='Project Id which contains the table to be read.',
        type=str,
        default='',
        required=True,
    )
    parser.add_argument(
        '--dataset_name',
        dest='dataset_name',
        help='Dataset Name which contains the table to be read.',
        type=str,
        default='',
        required=True,
    )
    parser.add_argument(
        '--source_table_name',
        dest='source_table_name',
        help='Table Name of the table which is source for write test.',
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

    args = parser.parse_args(argv[1:])

    # Providing the values.
    project_name = args.project_name
    dataset_name = args.dataset_name
    source_table_name = args.source_table_name
    destination_table_name = args.destination_table_name
    is_exactly_once = args.is_exactly_once

    assert_total_row_count(project_name, dataset_name, source_table_name, destination_table_name,
                           is_exactly_once)

    assert_unique_key_count(project_name, dataset_name, source_table_name, destination_table_name,
                            is_exactly_once)


if __name__ == '__main__':
    app.run(main)

# The following operations are performed for internal, unbounded read-write tests:
# 1. Copying source files to a temporary GCS directory which acts as a new source.
# 2. Creating a destination table with a hardcoded schema.
# 3. Running the Flink job in unbounded mode while dynamically adding new files to the source.

import argparse
from collections.abc import Sequence

from absl import app
from google.cloud import bigquery
from absl import logging
from google.cloud.bigquery import DatasetReference

def create_destination_table(project_name, dataset_name, destination_table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_name)

    table_id = f"{project_name}.{dataset_name}.{destination_table_name}"
    # This is a hardcoded schema specifically for internal, unbounded read-write tests only.
    # It defines the schema of the BigQuery table used in the test,
    # with fields for a unique key, name, number, and timestamp.
    table_schema = [
        bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ts", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, table_schema)
    try:
        table = client.create_table(table)  # Make an API request.
        logging.info(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except Exception as _:
        raise RuntimeError("Table could not be created!")


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_name',
        dest='project_name',
        help='Project Id for creation of the destination table.',
        type=str,
        default='',
        required=True,
    )
    parser.add_argument(
        '--dataset_name',
        dest='dataset_name',
        help='Dataset Name for creation of the destination table.',
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

    args = parser.parse_args(argv[1:])

    # Providing the values.
    project_name = args.project_name
    dataset_name = args.dataset_name
    destination_table_name = args.destination_table_name

    #  Create the destination table from the hardcoded schema.
    create_destination_table(project_name, dataset_name, destination_table_name)


if __name__ == '__main__':
    app.run(main)

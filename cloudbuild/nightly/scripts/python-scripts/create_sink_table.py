import argparse
from collections.abc import Sequence

from absl import app
from google.cloud import bigquery
from absl import logging
from google.cloud.bigquery import DatasetReference


def extract_source_table_schema(client, dataset_ref, source_table_name):
    table_ref = dataset_ref.table(source_table_name)
    table = client.get_table(table_ref)  # API Request
    return table.schema


def create_destination_table(project_name, dataset_name, source_table_name, destination_table_name):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_name)
    # Obtain the dataset reference for the dataset
    dataset_ref = DatasetReference(project_name, dataset_name)
    # Obtain the Source Table schema.
    source_table_schema = extract_source_table_schema(client, dataset_ref, source_table_name)
    logging.info(
        "Obtained Schema for the table {}.{}.{}".format(project_name, dataset_name,
                                                        destination_table_name))
    table_id = f"{project_name}.{dataset_name}.{destination_table_name}"
    table = bigquery.Table(table_id, schema=source_table_schema)
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

    args = parser.parse_args(argv[1:])

    # Providing the values.
    project_name = args.project_name
    dataset_name = args.dataset_name
    source_table_name = args.source_table_name
    destination_table_name = args.destination_table_name

    #  Create the destination table from the source table schema.
    create_destination_table(project_name, dataset_name, source_table_name, destination_table_name)


if __name__ == '__main__':
    app.run(main)

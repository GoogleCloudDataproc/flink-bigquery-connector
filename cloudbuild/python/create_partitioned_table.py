"""Python script for BQ Table data append.

Python script to create a BigQuery partitioned table.
"""
from collections.abc import Sequence
import datetime
import threading

from absl import app
from google.cloud import bigquery

from utils import table_type, utils


def create_partitioned_table(table_id):
    """Method to create a partitioned table.

  Args:
    table_id: The table id for the table to be created. Should be of the format:
      project_id.dataset_id.table_name
  """
    client = bigquery.Client()

    partitioned_table_schema = [
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('number', 'INT64'),
        bigquery.SchemaField('ts', 'TIMESTAMP', mode='REQUIRED'),
    ]
    table = bigquery.Table(table_id, schema=partitioned_table_schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.HOUR,
        field='ts',  # name of column to use for partitioning
        expiration_ms=1000 * 60 * 60 * 60,
    )  # expires in 72 hrs.

    table = client.create_table(table)

    print(
        f'Created table {table.project}.{table.dataset_id}.{table.table_id}, '
        f'partitioned on column {table.time_partitioning.field}.'
    )


def main(argv: Sequence[str]) -> None:
    required_arguments = {
        'now_timestamp',
        'project_name',
        'dataset_name',
        'table_name',
    }

    arg_input_utils = utils.ArgumentInputUtils(argv, required_arguments, required_arguments)
    arguments_dictionary = arg_input_utils.input_validate_and_return_arguments()

    project_name = arguments_dictionary['project_name']
    dataset_name = arguments_dictionary['dataset_name']
    table_name = arguments_dictionary['table_name']
    now_timestamp = arguments_dictionary['now_timestamp']

    # Create the partitioned table.
    table_id = f'{project_name}.{dataset_name}.{table_name}'
    create_partitioned_table(table_id)

    # Now add the partitions to the table.
    simple_avro_schema_fields_string = (
        '"fields": [{"name": "name", "type": "string"},{"name": "number",'
        '"type": "long"},{"name" : "ts", "type" : {"type" :'
        '"long","logicalType": "timestamp-micros"}}]'
    )
    simple_avro_schema_string = (
        '{"namespace": "project.dataset","type": "record","name":'
        ' "table","doc": "Avro Schema for project.dataset.table",'
        + simple_avro_schema_fields_string
        + '}'
    )
    number_of_partitions = 3
    number_of_rows_per_partition = 100
    number_of_threads = 10
    number_of_rows_per_batch = int(number_of_rows_per_partition / number_of_threads)
    now_timestamp = datetime.datetime.strptime(
        now_timestamp, '%Y-%m-%d'
    ).astimezone(datetime.timezone.utc) - datetime.timedelta(days=2)
    type_of_table = table_type.PartitionedTable(now_timestamp)
    avro_file_local = 'mockData.avro'
    table_creation_utils = utils.TableCreationUtils(
        simple_avro_schema_string,
        type_of_table,
        avro_file_local,
        number_of_rows_per_batch,
        table_id,
    )

    for partition_number in range(number_of_partitions):
        threads = list()
        for i in range(number_of_threads):
            x = threading.Thread(
                target=table_creation_utils.create_transfer_records,
                kwargs={'thread_number': str(i), 'partition_number': partition_number,
                        'current_timestamp': now_timestamp},
            )
            threads.append(x)
            x.start()
        for _, thread in enumerate(threads):
            thread.join()


if __name__ == '__main__':
    app.run(main)

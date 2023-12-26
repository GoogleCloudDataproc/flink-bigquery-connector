"""Python script for BQ Table data append.

Python script to add partitions to a BigQuery partitioned table.
"""
from collections.abc import Sequence
import datetime
import threading
import time
from absl import app

from utils import table_type, utils


def wait():
    print(
        'Going to sleep, waiting for connector to read existing, Time:'
        f' {datetime.datetime.now()}'
    )
    # This is the time connector takes to read the previous rows
    time.sleep(2.5 * 60)


def main(argv: Sequence[str]) -> None:
    required_arguments = {
        'now_timestamp',
        'refresh_interval',
        'project_name',
        'dataset_name',
        'table_name',
    }

    arg_input_utils = utils.ArgumentInputUtils(argv, required_arguments, required_arguments)
    arguments_dictionary = arg_input_utils.input_validate_and_return_arguments()

    # Providing the values.
    now_timestamp = arguments_dictionary['now_timestamp']
    now_timestamp = datetime.datetime.strptime(
        now_timestamp, '%Y-%m-%d'
    ).astimezone(datetime.timezone.utc)
    refresh_interval = int(arguments_dictionary['refresh_interval'])
    project_name = arguments_dictionary['project_name']
    dataset_name = arguments_dictionary['dataset_name']
    table_name = arguments_dictionary['table_name']

    # Set the partitioned table.
    table_id = f'{project_name}.{dataset_name}.{table_name}'

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

    # partitions[i] * number_of_rows_per_partition are inserted per phase.
    partitions = [2, 1, 2]
    number_of_rows_per_partition = 100
    number_of_threads = 10
    number_of_rows_per_thread = int(
        number_of_rows_per_partition / number_of_threads
    )

    type_of_table = table_type.PartitionedTable(now_timestamp)
    avro_file_local = 'mockData.avro'
    table_creation_utils = utils.TableCreationUtils(
        simple_avro_schema_string,
        type_of_table,
        avro_file_local,
        number_of_rows_per_thread,
        table_id,
    )

    # Insert in phases.
    prev_partitions_offset = 0
    for number_of_partitions in partitions:
        start_time = time.time()
        prev_partitions_offset += 1
        # Wait for the connector to read previously inserted rows.
        wait()

        # This is a phase of insertion.
        for partition_number in range(number_of_partitions):
            threads = list()
            # Insert via concurrent threads.
            for i in range(number_of_threads):
                x = threading.Thread(
                    target=table_creation_utils.create_transfer_records,
                    kwargs={
                        'thread_number': str(i),
                        'partition_number': partition_number + prev_partitions_offset,
                    },
                )
                threads.append(x)
                x.start()
            for _, thread in enumerate(threads):
                thread.join()

        time_elapsed = time.time() - start_time
        prev_partitions_offset += number_of_partitions
        # We wait for the refresh to happen
        # so that the data just created can be read.
        while time_elapsed < float(60 * 2 * refresh_interval):
            time_elapsed = time.time() - start_time


if __name__ == '__main__':
    app.run(main)

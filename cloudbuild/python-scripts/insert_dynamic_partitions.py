"""Python script for BQ Table data append.

Python script to add partitions to a BigQuery partitioned table.
"""
from collections.abc import Sequence
import datetime
import re
import threading
import time
from absl import app
from utils import table_type
from utils import utils


def wait():
    print(
        'Going to sleep, waiting for connector to read existing, Time:'
        f' {datetime.datetime.now()}'
    )
    # This is the time connector takes to read the previous rows
    time.sleep(2.5 * 60)


def validate_arguments(arguments_dictionary, required_arguments):
    for required_argument in required_arguments:
        if required_argument not in arguments_dictionary:
            raise UserWarning(
                f'[Log: parse_logs ERROR] {required_argument} argument not provided'
            )
    for key, _ in arguments_dictionary.items():
        if key not in required_arguments:
            raise UserWarning(
                f'[Log: parse_logs ERROR] Invalid argument "{key}" provided'
            )


def main(argv: Sequence[str]) -> None:
    required_arguments = {
        'now_timestamp',
        'refresh_interval',
        'project_name',
        'dataset_name',
        'table_name',
    }
    if len(argv) != (len(required_arguments) + 1):
        raise app.UsageError(
            '[Log: insert_dynamic_partitions ERROR] Too many or too les'
            ' command-line arguments.'
        )

    argument_pattern = r'--(\w+)=(.+)'
    # Forming a dictionary from the arguments
    try:
        matches = [re.match(argument_pattern, argument) for argument in argv[1:]]
        argument_dictionary = {match.group(1): match.group(2) for match in matches}
        del matches
    except AttributeError:
        raise UserWarning(
            '[Log: parse_logs ERROR] Missing argument. Please check the arguments'
            ' provided again.'
        )
    # Validating if all necessary arguments are available
    validate_arguments(argument_dictionary, required_arguments)
    # Providing the values.
    now_timestamp = argument_dictionary['now_timestamp']
    now_timestamp = datetime.datetime.strptime(
        now_timestamp, '%Y-%m-%d'
    ).astimezone(datetime.timezone.utc)
    refresh_interval = int(argument_dictionary['refresh_interval'])
    project_name = argument_dictionary['project_name']
    dataset_name = argument_dictionary['dataset_name']
    table_name = argument_dictionary['table_name']

    # 1. Create the partitioned table.
    table_id = f'{project_name}.{dataset_name}.{table_name}'

    # 2. Now add the partitions to the table.
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

    # 3. Insert in phases.
    prev_partitions_offset = 0
    for no_partitions in partitions:
        start_time = time.time()
        prev_partitions_offset += 1
        # Wait for the connector to read previously inserted rows.
        wait()

        # 4. This is a phase of insertion.
        for partition_no in range(no_partitions):
            threads = list()
            # Insert via concurrent threads.
            for i in range(number_of_threads):
                x = threading.Thread(
                    target=table_creation_utils.create_transfer_records,
                    kwargs={
                        'thread_no': str(i),
                        'partition_no': partition_no + prev_partitions_offset,
                    },
                )
                threads.append(x)
                x.start()
            for _, thread in enumerate(threads):
                thread.join()

        time_elapsed = time.time() - start_time
        prev_partitions_offset += no_partitions
        # We wait for the refresh to happen
        # so that the data just created can be read.
        while time_elapsed < float(60 * 2 * refresh_interval):
            time_elapsed = time.time() - start_time


if __name__ == '__main__':
    app.run(main)

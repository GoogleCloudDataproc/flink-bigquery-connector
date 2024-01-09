"""Python script to dynamically partitions to a BigQuery partitioned table."""

import argparse
from collections.abc import Sequence
import datetime
import logging
import threading
import time
from absl import app
import utils


def sleep_for_seconds(duration):
    logging.info(
        'Going to sleep, waiting for connector to read existing, Time: %s',
        datetime.datetime.now()
    )
    # Buffer time to ensure that new partitions are created
    # after previous read session and before next split discovery.
    time.sleep(duration)


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--refresh_interval',
        dest='refresh_interval',
        help='Minutes between checking new data',
        type=int,
        required=True,
    )
    parser.add_argument(
        '--project_name',
        dest='project_name',
        help='Project Id which contains the table to be read.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--dataset_name',
        dest='dataset_name',
        help='Dataset Name which contains the table to be read.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--table_name',
        dest='table_name',
        help='Table Name of the table which is read in the test.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '-n',
        '--number_of_rows_per_partition',
        dest='number_of_rows_per_partition',
        help='Number of rows to insert per partition.',
        type=int,
        required=False,
        default=30000,
    )

    args = parser.parse_args(argv[1:])

    # Providing the values.
    project_name = args.project_name
    dataset_name = args.dataset_name
    table_name = args.table_name
    number_of_rows_per_partition = args.number_of_rows_per_partition

    execution_timestamp = datetime.datetime.now(tz=datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    refresh_interval = int(args.refresh_interval)

    # Set the partitioned table.
    table_id = f'{project_name}.{dataset_name}.{table_name}'

    # Now add the partitions to the table.
    # Hardcoded schema. Needs to be same as that in the pre-created table.
    simple_avro_schema_fields_string = (
        '"fields": [{"name": "name", "type": "string"},{"name": "number",'
        '"type": "long"},{"name" : "ts", "type" : {"type" :'
        '"long","logicalType": "timestamp-micros"}}]'
    )
    simple_avro_schema_string = (
        '{"namespace": "project.dataset","type": "record","name":'
        ' "table","doc": "Avro Schema for project.dataset.table",'
        f'{simple_avro_schema_fields_string}'
        '}'
    )

    # hardcoded for e2e test.
    # partitions[i] * number_of_rows_per_partition are inserted per phase.
    partitions = [2, 1, 2]
    #  BQ rate limit is exceeded due to large number of rows.
    number_of_threads = 2
    number_of_rows_per_thread = number_of_rows_per_partition // number_of_threads

    avro_file_local = 'mockData.avro'
    table_creation_utils = utils.TableCreationUtils(
        simple_avro_schema_string,
        number_of_rows_per_thread,
        table_id,
    )

    # Insert iteratively.
    prev_partitions_offset = 0
    for number_of_partitions in partitions:
        start_time = time.time()
        # Wait for read stream formation.
        sleep_for_seconds(2.5 * 60)

        # This represents one iteration.
        for partition_number in range(number_of_partitions):
            threads = list()
            # Insert via concurrent threads.
            for thread_number in range(number_of_threads):
                avro_file_local_identifier = avro_file_local.replace(
                    '.', '_' + str(thread_number) + '.'
                )
                thread = threading.Thread(
                    target=table_creation_utils.avro_to_bq_with_cleanup,
                    kwargs={
                        'avro_file_local_identifier': avro_file_local_identifier,
                        'partition_number': partition_number + prev_partitions_offset,
                        'current_timestamp': execution_timestamp,
                    },
                )
                threads.append(thread)
                thread.start()
            for _, thread in enumerate(threads):
                thread.join()

        time_elapsed = time.time() - start_time
        prev_partitions_offset += number_of_partitions

        # We wait until the read streams are formed again.
        # So that the records just created can be read.
        sleep_for_seconds(float(60 * refresh_interval) - time_elapsed)


if __name__ == '__main__':
    app.run(main)

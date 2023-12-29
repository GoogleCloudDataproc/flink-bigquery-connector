"""Utilities for creation of BQ table.
"""

import datetime
import os
import re
import avro
import avro.datafile
import avro.io
from google.cloud import bigquery


class TableCreationUtils:
    """Class containing all the basic utilities for creation of BQ table.

    Attributes:
      table_type: Table type to be created: can be 'large_table',
        'large_row_table', 'complex_schema_table', 'unbounded_table'
      schema: The table schema to be inserted.
      number_of_rows_per_batch: Number of rows per thread.
      avro_file_local: The name of the local avro file to generate the data to.
      table_id: ID of the table of the form {project_id}.{dataset_id}.{table_id}
    """

    def __init__(
        self,
        simple_avro_schema_string,
        table_type,
        avro_file_local,
        number_of_rows_per_batch,
        table_id,
    ):
        """Constructor definition for the class.

        Args:
          simple_avro_schema_string: Schema of the table in avro format.
          table_type: `TableType` Object. For custom schema and record generation.
          avro_file_local: The name of the local avro file to generate the data to.
          number_of_rows_per_batch: Number of rows per thread.
          table_id: ID of the table of the form {project_id}.{dataset_id}.{table_id}
        """
        self.table_type = table_type
        self.schema = avro.schema.parse(simple_avro_schema_string)
        self.number_of_rows_per_batch = number_of_rows_per_batch
        self.avro_file_local = avro_file_local
        self.table_id = table_id

    def write_avros(self, thread_number, partition_number, current_timestamp):
        """Method to generate fake records for BQ table.

        Args:
          thread_number: The thread number - helps in parallelism
          partition_number: The partition number being created - only relevant in
            partitioned table creation.
          current_timestamp: Timestamp, one hour within which timestamp entries need
            to be generated.

        Raises:
          RuntimeError: When invalid table_type is provided.
        """

        writer = avro.datafile.DataFileWriter(
            open(
                self.avro_file_local.replace('.', '_' + thread_number + '.'), 'wb'
            ),
            avro.io.DatumWriter(),
            self.schema,
        )
        self.table_type.write_rows(
            self.number_of_rows_per_batch,
            writer,
            partition_number,
            current_timestamp,
        )
        writer.close()

    def transfer_avro_rows_to_bq_table(self, thread_number):
        """Method to load the created rows to BQ.

        Args: thread_number: The number of threads that concurrently perform the `operation` of
        generation of records, storing them locally to avro files, uploading them to a BQ table
        and finally deleting the locally generated avro files.
        """
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.AVRO,
            use_avro_logical_types=True,
        )

        # 
        local_avro_file = self.avro_file_local.replace(
            '.', '_' + thread_number + '.'
        )
        with open(local_avro_file, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file, self.table_id, job_config=job_config
            )
        job.result()

    def delete_local_file(self, thread_number):
        file_name = self.avro_file_local.replace('.', '_' + thread_number + '.')
        os.remove(file_name)

    def avro_to_bq_with_cleanup(
        self,
        thread_number,
        partition_number=0,
        current_timestamp=datetime.datetime.now(datetime.timezone.utc),
    ):
        self.write_avros(thread_number, partition_number, current_timestamp)
        self.transfer_avro_rows_to_bq_table(thread_number)
        self.delete_local_file(thread_number)


class ArgumentInputUtils:
    """Class for command like argument intake and validation."""

    def __init__(
        self,
        argv,
        required_arguments,
        acceptable_arguments,
    ):
        self.argv = argv
        self.required_arguments = required_arguments
        self.acceptable_arguments = acceptable_arguments

    def __get_arguments(self):
        """Parse the command line arguments and store them in a dictionary.

        Returns:
          A dictionary {argument_name: argument_value}.
            argument_name: The name of the argument provided.
            argument_value: The value for the concerned named argument.

        """
        # Arguments are provided of the form "--argument_name=argument_value"
        # We need to extract the name and value as a part of a dictionary.
        # i.e
        # {argument1_name: argument1_value, argument2_name: argument2_value, ...}
        # containing all arguments
        # The pattern
        #     --(\w+)=(.*): Searches for the exact '--'
        #         followed by a group of word character (alphanumeric & underscore)
        #         then an '=' sign
        #         followed by any character except linebreaks
        argument_pattern = r'--(\w+)=(.*)'

        # Forming a dictionary from the arguments
        try:
            matches = [
                re.match(argument_pattern, argument) for argument in self.argv[1:]
            ]
            argument_dictionary = {
                match.group(1): match.group(2) for match in matches
            }
            del matches
        except AttributeError as exc:
            raise UserWarning(
                'Missing argument. Please check the arguments'
                ' provided again.'
            ) from exc
        return argument_dictionary

    def __validate_arguments(self, arguments_dictionary):
        for required_argument in self.required_arguments:
            if required_argument not in arguments_dictionary:
                raise UserWarning(
                    f'{required_argument} argument not provided'
                )
        for key, _ in arguments_dictionary.items():
            if key not in self.acceptable_arguments:
                raise UserWarning(
                    f'Invalid argument "{key}" provided'
                )

    def input_validate_and_return_arguments(self):
        arguments_dictionary = self.__get_arguments()
        self.__validate_arguments(arguments_dictionary)
        return arguments_dictionary

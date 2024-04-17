"""Utilities for creation of BQ table."""

import datetime
import os
import random
import string
import avro
import avro.datafile
import avro.io
from google.cloud import bigquery



def is_perfect_hour(datetime_obj):
    """Returns True if the datetime object is a perfect hour, False otherwise."""
    return (
        datetime_obj.minute == 0
        and datetime_obj.second == 0
        and datetime_obj.microsecond == 0
    )


def generate_random_string():
    return ''.join(random.choices(string.ascii_letters, k=random.randint(8, 10)))


def generate_random_long():
    return random.randint(0, 10000000)


def generate_random_timestamp(current_timestamp):
    """Method to generate a random datetime within the given hour.

    Args:
      current_timestamp: Date is generated within one hour of this timestamp.

    Returns:
      datetime object: Containing generated timestamp.
    """
    next_hour = current_timestamp + datetime.timedelta(hours=1)
    random_timestamp = random.randint(
        int(current_timestamp.timestamp()), int(next_hour.timestamp())
    )
    utc = datetime.timezone.utc
    random_timestamp_utc = datetime.datetime.fromtimestamp(random_timestamp, utc)
    # Check if the generated entry is a perfect hour.
    # Note: It is only for the case of hour based partitioning
    # (as created in our test).
    # If the values inserted are changed to DAY or any other partitioning,
    # the prevention of borderline entry generation needs to be
    # changed accordingly.
    while is_perfect_hour(random_timestamp_utc):
        # Keep on regenerating.
        random_timestamp = random.randint(
            int(current_timestamp.timestamp()), int(next_hour.timestamp())
        )
        utc = datetime.timezone.utc
        random_timestamp_utc = datetime.datetime.fromtimestamp(
            random_timestamp, utc
        )
    return random_timestamp_utc


class TableCreationUtils:
    """Class containing all the basic utilities for creation of BQ table.

    Attributes:
      schema: The table schema to be inserted.
      number_of_rows_per_batch: Number of rows per thread.
      table_id: ID of the table of the form {project_id}.{dataset_id}.{table_id}
    """

    def __init__(
        self,
        simple_avro_schema_string,
        number_of_rows_per_batch,
        table_id,
    ):
        """Constructor definition for the class.

        Args:
          simple_avro_schema_string: Schema of the table in avro format. Since used
            for partitioned table creation, This schema is hardcoded for the purpose
            of e2e tests.
          number_of_rows_per_batch: Number of rows per thread.
          table_id: ID of the table of the form {project_id}.{dataset_id}.{table_id}
        """
        self.schema = avro.schema.parse(simple_avro_schema_string)
        self.number_of_rows_per_batch = number_of_rows_per_batch
        self.table_id = table_id

    def write_rows(
        self,
        number_of_rows_per_batch,
        writer,
        partition_number,
        current_timestamp,
        is_write_test=False,
        global_row_counter=None
    ):
        """Method to generate records.

        Args:
          number_of_rows_per_batch: The number of rows to be uploaded by one thread.
          writer: `DatumWriter` Object responsible for writing to local avro file.
          partition_number: The current partition number, the records are being
            inserted to. Helps in a creating a timestamp offset to prevent writing
            records into previously inserted partitions
          current_timestamp: The current timestamp, the base for calculating the
            offset.
          is_write_test: Boolean, indicating if the insertion is being run for a write test.
          global_row_counter: Class, containing a global variable.
        """
        # If the insertion is being run for a write test.
        # Create a unique_key on the basis of a global counter.
        if is_write_test:
            offset_timestamp = current_timestamp + datetime.timedelta(
                hours=partition_number
            )
            # Write the specified number of rows.
            for _ in range(number_of_rows_per_batch):
                name = generate_random_string()
                global_row_counter.global_var += 1
                writer.append({
                    'unique_key': str(global_row_counter.global_var) + "_" + name,
                    'name': name,
                    'number': generate_random_long(),
                    'ts': generate_random_timestamp(offset_timestamp),
                })

        else:
            offset_timestamp = current_timestamp + datetime.timedelta(
                hours=partition_number
            )

            # Write the specified number of rows.
            for _ in range(number_of_rows_per_batch):
                writer.append({
                    'name': generate_random_string(),
                    'number': generate_random_long(),
                    'ts': generate_random_timestamp(offset_timestamp),
                })

    def write_avros(
        self, avro_file_local_identifier, partition_number, current_timestamp, is_write_test,
        global_row_counter
    ):
        """Method to generate fake records for BQ table.

        Args:
          avro_file_local_identifier: The name of the avro file to be used by the
            current thread.
          partition_number: The partition number being created - only relevant in
            partitioned table creation.
          current_timestamp: Timestamp, one hour within which timestamp entries need
            to be generated.
          is_write_test: Boolean, indicating if the insertion is being run for a write test.
          global_row_counter: Class, containing a global variable.

        Raises:
          RuntimeError: When invalid table_type is provided.
        """

        writer = avro.datafile.DataFileWriter(
            open(avro_file_local_identifier, 'wb'),
            avro.io.DatumWriter(),
            self.schema,
        )
        self.write_rows(
            self.number_of_rows_per_batch,
            writer,
            partition_number,
            current_timestamp,
            is_write_test,
            global_row_counter,
        )
        writer.close()

    def transfer_avro_rows_to_bq_table(self, avro_file_local_identifier):
        """Method to load the created rows to BQ.

        Args:
          avro_file_local_identifier: The name of the avro file to be used by the
            current thread.
        """
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.AVRO,
            use_avro_logical_types=True,
        )
        with open(avro_file_local_identifier, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file, self.table_id, job_config=job_config
            )
        job.result()

    def delete_local_file(self, avro_file_local_identifier):
        os.remove(avro_file_local_identifier)

    def avro_to_bq_with_cleanup(
        self,
        avro_file_local_identifier,
        partition_number=0,
        current_timestamp=datetime.datetime.now(datetime.timezone.utc),
        is_write_test=False,
        global_row_counter=None
    ):
        self.write_avros(
            avro_file_local_identifier, partition_number, current_timestamp, is_write_test, global_row_counter
        )
        self.transfer_avro_rows_to_bq_table(avro_file_local_identifier)
        self.delete_local_file(avro_file_local_identifier)

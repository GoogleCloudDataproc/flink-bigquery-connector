"""Python class containing basic utilties required for the creation of BQ Table.

Can create specified number of rows, transfer them to BQ, delete the local file.
"""

import os
import avro
import avro.datafile
import avro.io
import datetime
from google.cloud import bigquery


class TableCreationUtils:
  """Class containing all the bais utilities for creation of BQ table.

  Attributes:
    table_type: Table type to be created: can be 'large_table',
      'large_row_table', 'complex_schema_table', 'unbounded_table'
    schema: the table schema to be inserted.
    no_rows_per_batch: number of rows per thread.
    avro_file_local: The name of the local avro file to generate the data to.
    table_id: id of the table of the form {project_id}.{dataset_id}.{table_id}
  """

  def __init__(
      self,
      simple_avro_schema_string,
      table_type,
      avro_file_local,
      no_rows_per_batch,
      table_id,
  ):
    """Constructor definition for the class.

    Args:
      simple_avro_schema_string:
      table_type: tableType Object. For custom schema and entry generation.
      avro_file_local: The name of the local avro file to generate the data to.
      no_rows_per_batch: number of rows per thread.
      table_id: id of the table of the form {project_id}.{dataset_id}.{table_id}
    """
    self.table_type = table_type
    self.schema = avro.schema.parse(simple_avro_schema_string)
    self.no_rows_per_batch = no_rows_per_batch
    self.avro_file_local = avro_file_local
    self.table_id = table_id

  def write_avros(
      self,
      thread_no,
      partition_no,
      current_timestamp
  ):
    """Method to generate fake records for BQ table.

    Args:
      thread_no: The thread number - helps in parallelism
      partition_no: The partition number being created - only relevant in
        partitioned table creation.
      current_timestamp: timestamp, one hour within which timestamp entries need
        to be generated.

    Raises:
      RuntimeError: when invalid table_type is provided.
    """

    writer = avro.datafile.DataFileWriter(
        open(self.avro_file_local.replace('.', '_' + thread_no + '.'), 'wb'),
        avro.io.DatumWriter(),
        self.schema,
    )
    self.table_type.write_rows(
        self.no_rows_per_batch, writer, partition_no, current_timestamp
    )
    writer.close()

  def transfer_avro_to_bq_table(self, thread_no):
    """Method to load the created rows to BQ.

    Args:
      thread_no: The number of threads to perfrom the function concurrently to
        add the avro rows to.
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.AVRO,
        use_avro_logical_types=True,

    )

    local_avro_file = self.avro_file_local.replace('.', '_' + thread_no + '.')
    with open(local_avro_file, 'rb') as source_file:
      job = client.load_table_from_file(
          source_file, self.table_id, job_config=job_config
      )
    job.result()

  def delete_local_file(self, thread_no):
    file_name = self.avro_file_local.replace('.', '_' + thread_no + '.')
    os.remove(file_name)
    print(f'Deleted {file_name}')

  def create_transfer_records(
      self,
      thread_no,
      partition_no=0,
      current_timestamp=datetime.datetime.now(datetime.timezone.utc),
  ):
    self.write_avros(thread_no, partition_no, current_timestamp)
    self.transfer_avro_to_bq_table(thread_no)
    self.delete_local_file(thread_no)

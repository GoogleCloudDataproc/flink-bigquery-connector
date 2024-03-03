"""Python script for BQ Table data append.

Python script to create a BigQuery table with 15 levels.
"""
import base64
from collections.abc import Sequence
import random
import string

from absl import app
from google.cloud import bigquery

"""Python class containing basic utilties required for the creation of BQ Table.

Can create specified number of rows, transfer them to BQ, delete the local file.
"""

import os
import avro
import avro.datafile
import avro.io
from google.cloud import bigquery

simple_avro_schema_string = ('{"type":"record",'
                             '"name":"root",'
                             '"namespace":"com.google.cloud.flink.bigquery",'
                             '"doc":"Translated Avro Schema for root",'
                             '"fields":[{"name":"array_of_records",'
                             '"type":{"type":"array",'
                             '"items":{"type":"record","name":"array_of_records",'
                             '"doc":"Translated Avro Schema for array_of_records",'
                             '"fields":[{"name":"string_field",'
                             '"type":{"type":"array","items":"string"}},'
                             '{"name":"byte_field","type":["null","bytes"]},'
                             '{"name":"record_field",'
                             '"type":{"type":"record",'
                             '"name":"record_field",'
                             '"doc":"Translated Avro Schema for record_field",'
                             '"fields":[{"name":"int_field_inside",'
                             '"type":["null","long"]}]}}]}}}]}')


def write_avros():
    """Method to generate fake records for BQ table.

    Raises:
      RuntimeError: when invalid table_type is provided.
    """
    schema = avro.schema.parse(simple_avro_schema_string)
    no_rows_per_batch = 1
    writer = avro.datafile.DataFileWriter(
        open('file.avro', 'wb'),
        avro.io.DatumWriter(),
        schema,
    )
    write_rows(no_rows_per_batch, writer)
    writer.close()


def write_rows(no_rows_per_batch, writer):
    # ([STRUCT(["abcd", "efgh"] as string_field, b'1234' as byte_field,
    # STRUCT( 4 as int_field_inside) as record_field)]);
    for _ in range(no_rows_per_batch):
        struct_arr = []
        for no_records in range(random.randint(2, 4)):
            record = {
                "string_field": function_array(),
                "byte_field": function_byte(),
                "record_field": {"int_field_inside": function_int()},
            }
            struct_arr.append(record)
        record_to_insert = {"array_of_records": struct_arr}
        print(record_to_insert)
        writer.append(record_to_insert)



def transfer_avro_to_bq_table():
    """Method to load the created rows to BQ.
    """
    client = bigquery.Client(project="bqrampupprashasti")

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.AVRO,
    )
    table_id = "bqrampupprashasti.Prashasti.record_check_table"
    local_avro_file = 'file.avro'
    with open(local_avro_file, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()
    client.get_table(table_id)


def delete_local_file():
    os.remove('file.avro')


def create_transfer_records():
    write_avros()
    transfer_avro_to_bq_table()
    delete_local_file()


def function_string():
    return ''.join(random.choices(string.ascii_uppercase +
                                  string.digits, k=random.randint(4, 10)))


def function_byte():
    byte_field = function_string().encode()
    # Encode the byte field to base64
    encoded_byte_field = base64.b64encode(byte_field)
    # Print the encoded byte field
    return encoded_byte_field


def function_int():
    return random.randint(0, 10000)


def function_double():
    return random.randint(0, 10000) / 10.0


def function_array():
    arr = []
    for j in range(6):
        arr.append(function_string())
    return arr


def function_array_int():
    arr = []
    for j in range(6):
        arr.append(function_int())
    return arr


def function():
    return {"name": function_string(), "number": function_int(), "array_field": function_array(),
            "byte_field": function_byte(), "float_field": function_double(), "boolean_field": True}


def main(argv: Sequence[str]) -> None:
    if len(argv) > 1:
        raise app.UsageError('Too many command-line arguments.')
    create_transfer_records()
    # no_rows = 10
    # Construct a BigQuery client object.
    # client = bigquery.Client()
    # table_id = "bqrampupprashasti.Prashasti.simple_table_read"
    # rows_to_insert = []
    # for _ in range(no_rows):
    #     to_insert = function()
    #     print(to_insert)
    #     rows_to_insert.append(to_insert)
    # print("\n---------------" * 8)
    # errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))


if __name__ == '__main__':
    app.run(main)

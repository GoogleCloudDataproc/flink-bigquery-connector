"""Python script for BQ Table data append.

Python script to create a BigQuery table with 15 levels.
"""
import base64
from collections.abc import Sequence
import random
import string

from absl import app
from google.cloud import bigquery


def function_string():
    return ''.join(random.choices(string.ascii_uppercase +
                                  string.digits, k=random.randint(4, 10)))

def function_byte():
    # byte_field = b'This is a byte field.'
    # # Encode the byte field to base64
    # encoded_byte_field = base64.b64encode(byte_field)
    # # Print the encoded byte field
    # return encoded_byte_field.decode()
    return

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


simple_avro_schema_string = (
    '{"type":"record",'
    '"name":"root",'
    '"namespace":"com.google.cloud.flink.bigquery",'
    '"doc":"Translated Avro Schema for root",'
    '"fields":'
    '[{"name":"name","type":"string"},'
    '{"name":"number","type":"long"},'
    '{"name":"array_field","type":{"type":"array","items":"string"}},'
    '{"name":"byte_field","type":["null","bytes"]},'
    '{"name":"float_field","type":["null","double"],"doc":"This is a float field"},'
    '{"name":"boolean_field","type":["null","boolean"]},'
    '{"name":"record_field","type":["null",{"type":"record","name":"record_field",'
    '"doc":"Translated Avro Schema for record_field",'
    '"fields":'
    '[{"name":"record_string","type":["null","string"]},'
    '{"name":"record_array","type":{"type":"array","items":"long"}}]}]}]}')


def main(argv: Sequence[str]) -> None:
    if len(argv) > 1:
        raise app.UsageError('Too many command-line arguments.')
    no_rows = 10
    # Construct a BigQuery client object.
    client = bigquery.Client()
    table_id = "bqrampupprashasti.Prashasti.simple_table_read"
    rows_to_insert = []
    for _ in range(no_rows):
        to_insert = function()
        print(to_insert)
        rows_to_insert.append(to_insert)
    print("\n---------------"*8)
    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


if __name__ == '__main__':
    app.run(main)

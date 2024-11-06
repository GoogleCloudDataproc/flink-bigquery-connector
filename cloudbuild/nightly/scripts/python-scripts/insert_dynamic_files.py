"""Python script to dynamically insert files to a GCS Bucket."""

import argparse
from collections.abc import Sequence
import datetime
import logging
import csv
import time
from datetime import datetime, timedelta
from google.cloud import storage
from absl import app


def sleep_for_seconds(duration):
    logging.info(
        'Going to sleep, waiting for connector to read existing, Time: %s',
        datetime.now()
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
        help='Project Id which contains the GCS bucket files to be read.',
        type=str,
        required=True,
    )

    parser.add_argument(
            '--gcs_source_uri',
            dest='gcs_source_uri',
            help='GCS Bucket which has the source csv file',
            type=str,
            required=True,
        )


    args = parser.parse_args(argv[1:])
    sleep_for_seconds(2.5*60)

    # Providing the values.
    project_name = args.project_name
    gcs_source_uri = args.gcs_source_uri
    refresh_interval = int(args.refresh_interval)
    bucket_name = gcs_source_uri.split("/")[2]

    # Split the URI into parts
    parts = gcs_source_uri.split("/")

    # Create the Storage Client
    source_blob_path = "/".join(parts[3:])
    destination_folder = "/".join(parts[3:-1]) + "/"

    storage_client = storage.Client(project=project_name)
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(source_blob_path + "source.csv")

    # Download the CSV file to memory
    data = source_blob.download_as_string().decode('utf-8')
    reader = csv.reader(data.splitlines())

    # Prepare data for the two copies
    copies = []
    counter = 60001  # Initialize the counter to add unique values
    for i in range(3):
            current_time = datetime.utcnow()
            new_rows = []
            for row in reader:
                unique_key = f"{counter}-{row[1]}"  # Combine counter and name
                new_row = [unique_key] + row[1:]  # Create the modified row
                new_rows.append(new_row)
                counter += 1  # Increment the counter for each record

            copies.append((current_time, new_rows))
            reader = csv.reader(data.splitlines())  # Reset the reader for the next copy

    # Upload the modified copies
    copy_count = 1
    for current_time, rows in copies:
        destination_blob_name = f"{destination_folder}source_{copy_count}.csv"
        blob = bucket.blob(destination_blob_name)

        # Write the modified data to a string buffer
        output = '\n'.join([','.join(row) for row in rows])

        blob.upload_from_string(output, content_type='text/csv')

        print(f"Copied and modified file uploaded to gs://{bucket_name}/{destination_blob_name}")
        copy_count += 1

        sleep_for_seconds(refresh_interval*60)


if __name__ == '__main__':
    app.run(main)

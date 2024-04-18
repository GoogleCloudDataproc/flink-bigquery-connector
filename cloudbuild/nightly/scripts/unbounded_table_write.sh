#!/bin/bash

# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
PROPERTIES=$1
timestamp=$2

# Copy the table
bq cp -f "$DATASET_NAME"."$SOURCE_TABLE_NAME" "$DATASET_NAME"."$SOURCE_TABLE_NAME"_"$timestamp"
# Set the table name to above copy for using in this test.
SOURCE_TABLE_NAME="$SOURCE_TABLE_NAME"_"$timestamp"
# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$SOURCE_TABLE_NAME"

# Modify the destination table name for all tests.
DESTINATION_TABLE_NAME="$SOURCE_TABLE_NAME"_"$timestamp"
# Create the destination table from the source table schema.
python3 cloudbuild/nightly/scripts/python-scripts/create_sink_table.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source_table_name "$SOURCE_TABLE_NAME" --destination_table_name "$DESTINATION_TABLE_NAME"
# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$DESTINATION_TABLE_NAME"

# Running this job async to make sure it exits so that dynamic data can be added
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --properties="$PROPERTIES" --async -- --gcp-source-project "$PROJECT_NAME" --bq-source-dataset "$DATASET_NAME" --bq-source-table "$SOURCE_TABLE_NAME" --mode unbounded --ts-prop "$TS_PROP_NAME" --partition-discovery-interval "$PARTITION_DISCOVERY_INTERVAL" --gcp-dest-project "$PROJECT_NAME" --bq-dest-dataset "$DATASET_NAME" --bq-dest-table "$DESTINATION_TABLE_NAME" --sink-parallelism "$UNBOUNDED_JOB_SINK_PARALLELISM"

# Dynamically adding the data. This is timed 2.5 min wait for read and 5 min refresh time.
python3 cloudbuild/nightly/scripts/python-scripts/insert_dynamic_partitions.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --table_name "$SOURCE_TABLE_NAME" --refresh_interval "$PARTITION_DISCOVERY_INTERVAL" --is_write_test

# Now the Dataproc job will automatically succeed after stipulated time (18 minutes hardcoded).
# we wait for it to succeed or finish.
gcloud dataproc jobs wait "$JOB_ID" --region "$REGION" --project "$PROJECT_NAME"


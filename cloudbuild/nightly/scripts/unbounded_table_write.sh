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
UNBOUNDED_JOB_SINK_PARALLELISM=$3
IS_SQL=$4
IS_EXACTLY_ONCE_ENABLED=$5

NEW_GCS_SOURCE_URI="$GCS_SOURCE_URI"temp/"$timestamp"/
if [ "$IS_SQL" == True ]
then
  NEW_GCS_SOURCE_URI="$NEW_GCS_SOURCE_URI"SQL/
fi
if [ "$IS_EXACTLY_ONCE_ENABLED" == True ]
then
  NEW_GCS_SOURCE_URI="$NEW_GCS_SOURCE_URI"EXO/
else
  NEW_GCS_SOURCE_URI="$NEW_GCS_SOURCE_URI"ALO/
fi
# Copy Source File to a temp directory
gcloud storage cp "$GCS_SOURCE_URI"source.csv "$NEW_GCS_SOURCE_URI"source.csv
# Lifecycle policy of deletion in 1 day already set, no need to add expiration to this directory

# Modify the destination table name for all tests.
DESTINATION_TABLE_NAME="$DESTINATION_TABLE_NAME"_"$timestamp"

if [ "$IS_SQL" == True ]
then
  echo "SQL Mode is Enabled!"
  DESTINATION_TABLE_NAME="$DESTINATION_TABLE_NAME"_SQL
fi
if [ "$IS_EXACTLY_ONCE_ENABLED" == True ]
then
  DESTINATION_TABLE_NAME="$DESTINATION_TABLE_NAME"_EXO
else
  DESTINATION_TABLE_NAME="$DESTINATION_TABLE_NAME"_ALO
fi
# Create the destination table from hardcoded table schema.
python3 cloudbuild/nightly/scripts/python-scripts/create_unbounded_sink_table.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --destination_table_name "$DESTINATION_TABLE_NAME"
# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$DESTINATION_TABLE_NAME"

# Running this job async to make sure it exits so that dynamic data can be added
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --properties="$PROPERTIES" --async -- --gcp-source-project "$PROJECT_NAME" --gcs-source-uri "$NEW_GCS_SOURCE_URI" --mode unbounded --file-discovery-interval "$FILE_DISCOVERY_INTERVAL" --gcp-dest-project "$PROJECT_NAME" --bq-dest-dataset "$DATASET_NAME" --bq-dest-table "$DESTINATION_TABLE_NAME" --sink-parallelism "$UNBOUNDED_JOB_SINK_PARALLELISM" --is-sql "$IS_SQL" --exactly-once "$IS_EXACTLY_ONCE_ENABLED"

# Dynamically adding new files. This is timed 2.5 min wait for read and 5 min refresh time.
python3 cloudbuild/nightly/scripts/python-scripts/insert_dynamic_files.py -- --project_name "$PROJECT_NAME" --gcs_source_uri "$NEW_GCS_SOURCE_URI" --refresh_interval "$FILE_DISCOVERY_INTERVAL"

# Now the Dataproc job will automatically succeed after stipulated time (18 minutes hardcoded).
# we wait for it to succeed or finish.
gcloud dataproc jobs wait "$JOB_ID" --region "$REGION" --project "$PROJECT_NAME"

# Explicitly deleting the newly created files in the GCS bucket
gcloud storage rm --recursive "$NEW_GCS_SOURCE_URI"

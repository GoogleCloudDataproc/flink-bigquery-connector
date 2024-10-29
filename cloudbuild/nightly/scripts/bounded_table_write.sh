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
BOUNDED_JOB_SINK_PARALLELISM=$2
IS_SQL=$3
IS_EXACTLY_ONCE_ENABLED=$4

# We won't run this async as we can wait for a bounded job to succeed or fail.
# Create the destination table from the source table schema.
python3 cloudbuild/nightly/scripts/python-scripts/create_sink_table.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source_table_name "$SOURCE_TABLE_NAME" --destination_table_name "$DESTINATION_TABLE_NAME"
# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$DESTINATION_TABLE_NAME"
# Run the sink JAR JOB
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --properties="$PROPERTIES" -- --gcp-source-project "$PROJECT_NAME" --bq-source-dataset "$DATASET_NAME" --bq-source-table "$SOURCE_TABLE_NAME" --gcp-dest-project "$PROJECT_NAME" --bq-dest-dataset "$DATASET_NAME" --bq-dest-table "$DESTINATION_TABLE_NAME" --sink-parallelism "$BOUNDED_JOB_SINK_PARALLELISM" --is-sql "$IS_SQL" --exactly-once "$IS_EXACTLY_ONCE_ENABLED"

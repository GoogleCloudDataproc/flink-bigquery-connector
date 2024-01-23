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

# Get the timestamp to annotate the copy of the partitioned table.
timestamp=$(date +"%Y%m%d%H%M%S")

# Copy the table
bq cp -f "$DATASET_NAME"."$TABLE_NAME" "$DATASET_NAME"."$TABLE_NAME"_"$timestamp"

# Set the table name to above copy for using in this test.
TABLE_NAME="$TABLE_NAME"_"$timestamp"

# Set the expiration time to 1 hour.
bq update --expiration 3600 "$DATASET_NAME"."$TABLE_NAME"

# Running this job async to make sure it exits so that dynamic data can be added
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --properties=taskmanager.numberOfTaskSlots=1,parallelism.default=1 --async -- --gcp-project "$PROJECT_NAME" --bq-dataset "$DATASET_NAME" --bq-table "$TABLE_NAME" --mode unbounded --agg-prop "$AGG_PROP_NAME" --ts-prop "$TS_PROP_NAME" --partition-discovery-interval "$PARTITION_DISCOVERY_INTERVAL"

# Dynamically adding the data. This is timed 2.5 min wait for read and 5 min refresh time.
python3 cloudbuild/nightly/scripts/python-scripts/insert_dynamic_partitions.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --table_name "$TABLE_NAME" --refresh_interval "$PARTITION_DISCOVERY_INTERVAL"

# Now the Dataproc job will automatically succeed after stipulated time (18 minutes hardcoded).
# we wait for it to succeed or finish.
gcloud dataproc jobs wait "$JOB_ID" --region "$REGION" --project "$PROJECT_NAME"

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

# Time at execution, used for deciding timestamp in dynamic insertion.
now=$(date "+%Y-%m-%d")

# Delete the pre-existing partitioned table.
# -f to ensure there is no input prompt.
bq rm -t -f "$PROJECT_NAME":"$DATASET_NAME"."$TABLE_NAME"

# Create a new partitioned table and insert initial values.
python3 cloudbuild/python-scripts/create_partitioned_table.py -- --now_timestamp "$now" --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --table_name "$TABLE_NAME"

# Running this job async to make sure it exits so that dynamic data can be added
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" --async -- --gcp-project "$PROJECT_NAME" --bq-dataset "$DATASET_NAME" --bq-table "$TABLE_NAME" --mode unbounded --agg-prop "$AGG_PROP_NAME" --ts-prop "$TS_PROP_NAME" --partition-discovery-interval "$PARTITION_DISCOVERY_INTERVAL"

# Dynamically adding the data. This is timed 2.5 min wait for read and 5 min refresh time.
python3 cloudbuild/python-scripts/insert_dynamic_partitions.py -- --now_timestamp "$now" --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --table_name "$TABLE_NAME" --refresh_interval "$PARTITION_DISCOVERY_INTERVAL"

# Wait for a bit, as mapping and output of records takes some time.
sleep 3m

# Kill the dataproc job
# quiet flag will not ask for console input.
gcloud dataproc jobs kill "$JOB_ID" --region="$REGION" --quiet

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

# Collect the arguments.
PROJECT_ID=$1
CLUSTER_NAME=$2
REGION=$3
PROJECT_NAME=$4
DATASET_NAME=$5
TABLE_NAME=$6
AGG_PROP_NAME=$7
QUERY=$8

set -euxo pipefail
gcloud config set project "$PROJECT_ID"
# Create a random JOB_ID
JOB_ID=$(echo "$RANDOM" | md5sum | cut -c 1-30)
echo [LOGS: "$PROJECT_NAME"."$DATASET_NAME"."$TABLE_NAME" Read] Created JOB ID: "$JOB_ID"
# We won't run this async as we can wait for a bounded job to succeed or fail.
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" -- --gcp-project "$PROJECT_NAME" --bq-dataset "$DATASET_NAME" --bq-table "$TABLE_NAME" --agg-prop "$AGG_PROP_NAME" --query "$QUERY"
# Wait for the logs to be saved.
sleep 20
# Now check the success of the job
python3 cloudbuild/python-scripts/parse_logs.py -- --job_id="$JOB_ID" --project_id="$PROJECT_ID" --cluster_name="$CLUSTER_NAME" --region="$REGION" --project_name="$PROJECT_NAME" --dataset_name="$DATASET_NAME" --table_name="$TABLE_NAME" --query="$QUERY"
ret=$?

if [ $ret -ne 0 ]
then
   echo Run Failed
   exit 1
else
   echo Run Succeeds
fi


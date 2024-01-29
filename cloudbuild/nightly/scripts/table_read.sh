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
QUERY_STRING=$8
MODE=$9
PROPERTIES=${10}

set -euxo pipefail
gcloud config set project "$PROJECT_ID"

# Create a random JOB_ID
JOB_ID=$(echo "$RANDOM" | md5sum | cut -c 1-30)
# Adds timestamp to jobid to prevent job_id repetition.
JOB_ID="$JOB_ID"_$(date +"%Y%m%d%H%M%S")
echo [LOGS: "$PROJECT_NAME"."$DATASET_NAME"."$TABLE_NAME" Read] Created JOB ID: "$JOB_ID"

if [ "$MODE" == "bounded" ]
then
  echo "Bounded Mode!"
  source cloudbuild/nightly/scripts/bounded_table_read.sh "$PROPERTIES"
  # Wait for the logs to be saved.
  # Logs take some time to be saved and be available.
  # wait for a few seconds to ensure smooth execution.
  sleep 5
elif [ "$MODE" == "unbounded" ]
then
  echo "Unbounded Mode!"
  source cloudbuild/nightly/scripts/unbounded_table_read.sh "$PROPERTIES"
  # Add more sleep time. as IO might take time.
  # Logs take some time to be saved and be available.
  sleep 60
else
  echo "Invalid 'MODE' provided. Please provide 'bounded' or 'unbounded'!"
  exit 1
fi

# Now check the success of the job
# Mode helps in checking for unbounded job separately.
python3 cloudbuild/nightly/scripts/python-scripts/parse_logs.py -- --job_id "$JOB_ID" --project_id "$PROJECT_ID" --cluster_name "$CLUSTER_NAME" --region "$REGION" --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --table_name "$TABLE_NAME" --query "$QUERY_STRING" --mode "$MODE"
ret=$?
if [ $ret -ne 0 ]
then
   echo Run Failed
   exit 1
else
   echo Run Succeeded!
fi


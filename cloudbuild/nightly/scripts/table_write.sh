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
SOURCE=$6
DESTINATION_TABLE_NAME=$7
IS_EXACTLY_ONCE_ENABLED=$8
MODE=$9
PROPERTIES=${10}
SINK_PARALLELISM=${11}
IS_SQL=${12}
set -euxo pipefail
gcloud config set project "$PROJECT_ID"

# Obtain Timestamp
timestamp=$(date +"%Y%m%d%H%M%S")
# Create a random JOB_ID
JOB_ID=$(echo "$RANDOM" | md5sum | cut -c 1-30)
# Adds timestamp to job_id to prevent repetition.
JOB_ID="$JOB_ID"_"$timestamp"

if [ "$MODE" == "bounded" ]
then
  echo "Bounded Mode!"
  echo [LOGS: "$PROJECT_NAME"."$DATASET_NAME"."$SOURCE" Write Test] Created JOB ID: "$JOB_ID"
  # Modify the destination table name.
  DESTINATION_TABLE_NAME="$SOURCE"-"$timestamp"
  if [ "$IS_SQL" == True ]
  then
    echo "SQL Mode is Enabled!"
    DESTINATION_TABLE_NAME="$DESTINATION_TABLE_NAME"-"$IS_SQL"
  fi
  source cloudbuild/nightly/scripts/bounded_table_write.sh "$PROPERTIES" "$SINK_PARALLELISM" "$IS_SQL"
elif [ "$MODE" == "unbounded" ]
then
  echo "Unbounded Mode!"
  echo [LOGS: "$PROJECT_NAME" "$SOURCE" Write Test] Created JOB ID: "$JOB_ID"
  source cloudbuild/nightly/scripts/unbounded_table_write.sh "$PROPERTIES" "$timestamp" "$SINK_PARALLELISM" "$IS_SQL"
else
  echo "Invalid 'MODE' provided. Please provide 'bounded' or 'unbounded'!"
  exit 1
fi

# Now check the success of the job
# Mode helps in checking for unbounded job separately.
if [ "$IS_EXACTLY_ONCE_ENABLED" == True ]
then
  echo "Exactly Once!"
  python3 cloudbuild/nightly/scripts/python-scripts/assert_table_count.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source "$SOURCE" --destination_table_name "$DESTINATION_TABLE_NAME" --mode "$MODE" --is_exactly_once
else
  echo "At-least Once!"
  python3 cloudbuild/nightly/scripts/python-scripts/assert_table_count.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source "$SOURCE" --destination_table_name "$DESTINATION_TABLE_NAME" --mode "$MODE"
fi
ret=$?
if [ $ret -ne 0 ]
then
   echo Run Failed
   exit 1
else
   echo Run Succeeded!
fi


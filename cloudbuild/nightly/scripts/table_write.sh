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
SOURCE_TABLE_NAME=$6
DESTINATION_TABLE_NAME=$7
IS_EXACTLY_ONCE_ENABLED=$8
MODE=$9
PROPERTIES=${10}
set -euxo pipefail
gcloud config set project "$PROJECT_ID"

# Obtain Timestamp
timestamp=$(date +"%Y%m%d%H%M%S")
# Create a random JOB_ID
JOB_ID=$(echo "$RANDOM" | md5sum | cut -c 1-30)
# Adds timestamp to job_id to prevent repetition.
JOB_ID="$JOB_ID"_"$timestamp"
echo [LOGS: "$PROJECT_NAME"."$DATASET_NAME"."$SOURCE_TABLE_NAME" Write Test] Created JOB ID: "$JOB_ID"

if [ "$MODE" == "bounded" ]
then
  echo "Bounded Mode!"
  # Modify the destination table name.
  DESTINATION_TABLE_NAME="$SOURCE_TABLE_NAME"-"$timestamp"
  source cloudbuild/nightly/scripts/bounded_table_write.sh "$PROPERTIES"
elif [ "$MODE" == "unbounded" ]
then
  echo "Unbounded Mode!"
  source cloudbuild/nightly/scripts/unbounded_table_write.sh "$PROPERTIES" "$timestamp"
else
  echo "Invalid 'MODE' provided. Please provide 'bounded' or 'unbounded'!"
  exit 1
fi

# Now check the success of the job
# Mode helps in checking for unbounded job separately.
if [ "$IS_EXACTLY_ONCE_ENABLED" == True ]
then
  echo "Exactly Once!"
  python3 cloudbuild/nightly/scripts/python-scripts/assert_table_count.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source_table_name "$SOURCE_TABLE_NAME" --destination_table_name "$DESTINATION_TABLE_NAME" --is_exactly_once
else
  echo "At-least Once!"
  python3 cloudbuild/nightly/scripts/python-scripts/assert_table_count.py -- --project_name "$PROJECT_NAME" --dataset_name "$DATASET_NAME" --source_table_name "$SOURCE_TABLE_NAME" --destination_table_name "$DESTINATION_TABLE_NAME"
fi
ret=$?
if [ $ret -ne 0 ]
then
   echo Run Failed
   exit 1
else
   echo Run Succeeded!
fi


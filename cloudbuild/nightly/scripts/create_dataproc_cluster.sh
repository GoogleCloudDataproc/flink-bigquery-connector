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

CLUSTER_NAME=$1
REGION_ARRAY_STRING=$2
NUM_WORKERS=$3

# Set the project, location and zone for the cluster creation.
gcloud config set project "$PROJECT_ID"
# Convert the REGION_ARRAY_STRING to actual array which can be iterated upon.
read -a REGION_ARRAY <<< "$REGION_ARRAY_STRING"
for REGION in "${REGION_ARRAY[@]}"
do
  # Change the region.
  gcloud config set compute/region "$REGION"

  # Create the cluster
  # max-age indicates that the cluster will auto delete in an hour.
  # use the default created staging and temp buckets.
#  gcloud dataproc clusters create "$CLUSTER_NAME" \
#      --region="$REGION" \
#      --image-version="$DATAPROC_IMAGE_VERSION" \
#      --optional-components=FLINK \
#      --enable-component-gateway \
#      --num-masters=1 \
#      --max-age=1h \
#      --num-workers="$NUM_WORKERS" \
#      --initialization-actions="$INITIALISATION_ACTION_SCRIPT_URI"
  echo "Hello World"
  # Check if cluster creation succeeds.
  result=$?
  if [[ $result -eq 0 ]]
  then
    echo "Cluster Successfully Created!"
    break
  else
    echo "Cluster Creation Failed."
    sleep 5
  fi
done

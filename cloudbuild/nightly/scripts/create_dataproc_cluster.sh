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
TEMP_BUCKET=$4
STAGING_BUCKET=$5

# Set the project, location and zone for the cluster creation.
gcloud config set project "$PROJECT_ID"
# Convert the REGION_ARRAY_STRING to actual array which can be iterated upon.
read -a REGION_ARRAY <<< "$REGION_ARRAY_STRING"
for REGION in "${REGION_ARRAY[@]}"
do
  # Change the region.
  gcloud config set compute/region "$REGION"

  # Create the temp bucket for the cluster.
  gcloud storage buckets create gs://"$TEMP_BUCKET" \
      --project="$PROJECT_ID" --location="$REGION" \
      --uniform-bucket-level-access \
      --public-access-prevention

  # Create the staging bucket for the cluster.
  gcloud storage buckets create gs://"$STAGING_BUCKET" \
      --project="$PROJECT_ID" --location="$REGION" \
      --uniform-bucket-level-access \
      --public-access-prevention

  # Create the cluster
  python3 cloudbuild/nightly/scripts/python-scripts/create_cluster.py -- --region "$REGION" --project_id \
  "$PROJECT_ID" --cluster_name "$CLUSTER_NAME" --dataproc_image_version "$DATAPROC_IMAGE_VERSION" --num_workers "$NUM_WORKERS" \
  --initialisation_action_script_uri "$INITIALISATION_ACTION_SCRIPT_URI" --temp_bucket_name "$TEMP_BUCKET" --staging_bucket_name "$STAGING_BUCKET"

  # Check if cluster creation succeeds.
  result=$?
  if [[ $result -eq 0 ]]
  then
    echo "Cluster Successfully Created!"
    break
  else
    echo "Cluster Creation Failed."
    # Delete the created buckets.
    gcloud storage rm --recursive "$TEMP_BUCKET"
    gcloud storage rm --recursive "$STAGING_BUCKET"
  fi
done

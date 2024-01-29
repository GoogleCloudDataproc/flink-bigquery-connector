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
REGION_SAVING_FILE=$4

# Set the project, location and zone for the cluster creation.
gcloud config set project "$PROJECT_ID"
# Create the cluster
# The script retries to create from the list of regions provided.
python3 cloudbuild/nightly/scripts/python-scripts/create_cluster.py -- --region_array_string "$REGION_ARRAY_STRING" --project_id \
"$PROJECT_ID" --cluster_name "$CLUSTER_NAME" --dataproc_image_version "$DATAPROC_IMAGE_VERSION" --num_workers "$NUM_WORKERS" \
--initialisation_action_script_uri "$INITIALISATION_ACTION_SCRIPT_URI" --region_saving_file "$REGION_SAVING_FILE"

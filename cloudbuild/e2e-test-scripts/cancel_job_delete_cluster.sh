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

REGION=$1
CLUSTER_NAME=$2
TEMP_BUCKET=$3
STAGING_BUCKET=$4
set -euxo pipefail

# 1. Cancel the jobs running on the cluster.
python3 cloudbuild/python/cancel_dataproc_jobs.py -- --project_id="$PROJECT_ID" --cluster_name="$CLUSTER_NAME" --cluster_region_name="$REGION"
# 2. Then delete the cluster.
gcloud dataproc clusters delete "$CLUSTER_NAME" --region="$REGION" --quiet
# 3. Delete the temp bucket
gcloud storage rm --recursive gs://"$TEMP_BUCKET"/
# 4. Delete the staging bucket
gcloud storage rm --recursive gs://"$STAGING_BUCKET"/

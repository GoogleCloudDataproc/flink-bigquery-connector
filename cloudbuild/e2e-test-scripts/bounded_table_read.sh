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


# We won't run this async as we can wait for a bounded job to succeed or fail.
gcloud dataproc jobs submit flink --id "$JOB_ID" --jar="$GCS_JAR_LOCATION" --cluster="$CLUSTER_NAME" --region="$REGION" -- --gcp-project "$PROJECT_NAME" --bq-dataset "$DATASET_NAME" --bq-table "$TABLE_NAME" --agg-prop "$AGG_PROP_NAME" --query "$QUERY_STRING"


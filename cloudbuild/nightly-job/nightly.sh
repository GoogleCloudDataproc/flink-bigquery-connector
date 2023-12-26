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

set -euxo pipefail
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN clean install -DskipTests
    gcloud storage cp "$MVN_JAR_LOCATION" "$GCS_JAR_LOCATION"
    exit
    ;;

  # Create the cluster
  create_clusters)
    # 1. Create the first cluster for bounded read.
    source cloudbuild/e2e-test-scripts/create_dataproc_cluster.sh "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROPERTIES_SMALL_TEST" "$NUM_WORKERS_SMALL_TEST" "$TEMP_BUCKET_SMALL_TEST" "$STAGING_BUCKET_SMALL_TEST"
    # 2. Create the second cluster for unbounded read.
    source cloudbuild/e2e-test-scripts/create_dataproc_cluster.sh "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST" "$PROPERTIES_UNBOUNDED_TABLE_TEST" "$NUM_WORKERS_UNBOUNDED_TABLE_TEST" "$TEMP_BUCKET_UNBOUNDED_TABLE_TEST" "$STAGING_BUCKET_UNBOUNDED_TABLE_TEST"
    exit
    ;;

  # Run the bounded e2e tests
  e2e_bounded_read_tests)
    # 1. Run the simple bounded table test.
    source cloudbuild/e2e-test-scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SIMPLE_TABLE" "$AGG_PROP_NAME_SIMPLE_TABLE" "" "bounded"
    # 2. Run the complex schema table test.
    source cloudbuild/e2e-test-scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_COMPLEX_SCHEMA_TABLE" "$AGG_PROP_NAME_COMPLEX_SCHEMA_TABLE" "" "bounded"
    # 3. Run the query test.
    source cloudbuild/e2e-test-scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "" "" "$QUERY" "bounded"
    # 4. Run the large table test.
    source cloudbuild/e2e-test-scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_SMALL_TEST" "$REGION_SMALL_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_LARGE_TABLE" "$AGG_PROP_NAME_LARGE_TABLE" "" "bounded"
    ;;

  # Run the e2e tests unbounded partitioned table
  e2e_unbounded_read_test)
    # 5. Run the unbounded source test.
    source cloudbuild/e2e-test-scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_UNBOUNDED_TABLE" "$AGG_PROP_NAME_UNBOUNDED_TABLE" "" "unbounded"
    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac


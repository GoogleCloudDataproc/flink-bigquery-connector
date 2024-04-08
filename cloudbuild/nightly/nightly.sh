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
readonly MVN="./mvnw -B -e -s /workspace/cloudbuild/nightly/gcp-settings.xml -Dmaven.repo.local=/workspace/.repository"
readonly STEP=$1

cd /workspace

# Function to create a cluster with the specified parameters.
create_cluster(){
  CLUSTER_NAME=$1
  REGION_ARRAY_STRING=$2
  NUM_WORKERS=$3
  REGION_FILE=$4
  WORKER_MACHINE_TYPE=$5
  CLUSTER_FILE=$6
   #  Get the timestamp to append to cluster name.
  timestamp=$(date +"%Y%m%d%H%M%S")
  # 1. Create the first cluster for bounded read.
  # - Modify the cluster name for all tests.
  CLUSTER_NAME="$CLUSTER_NAME"-"$timestamp"
  # - call script that creates cluster with retries.
  source cloudbuild/nightly/scripts/create_dataproc_cluster.sh "$CLUSTER_NAME" "$REGION_ARRAY_STRING" "$NUM_WORKERS" "$REGION_FILE" "$WORKER_MACHINE_TYPE"
  # - save the cluster for future uses
  echo "$CLUSTER_NAME" > "$CLUSTER_FILE"
}

# Function to run the test to check BQ Table Read.
run_test(){
  PROJECT_ID=$1
  REGION_FILE=$2
  CLUSTER_FILE=$3
  PROJECT_NAME=$4
  DATASET_NAME=$5
  TABLE_NAME=$6
  AGG_PROP_NAME=$7
  QUERY=$8
  MODE=$9
  PROPERTIES=${10}
  # Get the final region and the cluster name.
  export REGION=$(cat "$REGION_FILE")
  export CLUSTER_NAME=$(cat "$CLUSTER_FILE")
  # Run the simple bounded table test.
  source cloudbuild/nightly/scripts/table_read.sh "$PROJECT_ID" "$CLUSTER_NAME" "$REGION" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME" "$AGG_PROP_NAME" "$QUERY" "$MODE" "$PROPERTIES"
}

# Function to run the test to check BQ Table Read.
# Also, delete the cluster and its buckets.
run_test_delete_cluster(){
  PROJECT_ID=$1
  # Run the test.
  run_test "$PROJECT_ID" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}"
  # REGION and CLUSTER_NAME should be in scope (from previous function).
  # Delete the cluster as well as its staging and temp buckets.
  python3 cloudbuild/nightly/scripts/python-scripts/delete_buckets_and_clusters.py -- --cluster_name "$CLUSTER_NAME" --region "$REGION" --project_id "$PROJECT_ID"
}
case $STEP in
  # Download maven and all the dependencies
  init)
    $MVN clean install -DskipTests -Pflink_1.17
    gcloud storage cp "$MVN_JAR_LOCATION" "$GCS_JAR_LOCATION"
    exit
    ;;

  # Create the cluster - Small Read bounded job.
  create_clusters_bounded_small_table)
    create_cluster "$CLUSTER_NAME_SMALL_TEST" "$REGION_ARRAY_STRING_SMALL_TEST" "$NUM_WORKERS_SMALL_TEST" "$REGION_SMALL_TEST_FILE" "$WORKER_MACHINE_TYPE_SMALL_BOUNDED" "$CLUSTER_SMALL_TEST_FILE"
    exit
    ;;

  # Create the cluster - Large Table Read bounded job.
  create_clusters_bounded_large_table)
    create_cluster "$CLUSTER_NAME_LARGE_TABLE_TEST" "$REGION_ARRAY_STRING_LARGE_TABLE_TEST" "$NUM_WORKERS_LARGE_TABLE_TEST" "$REGION_LARGE_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_LARGE_BOUNDED" "$CLUSTER_LARGE_TABLE_TEST_FILE"
    exit
    ;;

  # Create the cluster - Unbounded Read job.
  create_clusters_unbounded_table)
    create_cluster "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_ARRAY_STRING_UNBOUNDED_TABLE_TEST" "$NUM_WORKERS_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_UNBOUNDED" "$CLUSTER_UNBOUNDED_TABLE_TEST_FILE"
    exit
    ;;

  # Run the small table read bounded e2e test.
  e2e_bounded_read_small_table_test)
    run_test "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SIMPLE_TABLE" "$AGG_PROP_NAME_SIMPLE_TABLE" "" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB"
    exit
    ;;

  # Run the nested schema table read bounded e2e test.
  e2e_bounded_read_nested_schema_table_test)
    run_test "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_COMPLEX_SCHEMA_TABLE" "$AGG_PROP_NAME_COMPLEX_SCHEMA_TABLE" "" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB"
    exit
    ;;

  # Run the query read bounded e2e test.
  e2e_bounded_read_query_test)
    run_test_delete_cluster "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "" "" "$QUERY" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB"
    exit
    ;;

  # Run the large table O(GB's) read bounded e2e test.
  e2e_bounded_read_large_table_test)
    # Run the large table test.
    run_test_delete_cluster "$PROJECT_ID" "$REGION_LARGE_TABLE_TEST_FILE" "$CLUSTER_LARGE_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_LARGE_TABLE" "$AGG_PROP_NAME_LARGE_TABLE" "" "bounded" "$PROPERTIES_LARGE_BOUNDED_JOB"
    exit
    ;;

  # Run the e2e tests unbounded partitioned table
  e2e_unbounded_read_test)
    run_test_delete_cluster "$PROJECT_ID" "$REGION_UNBOUNDED_TABLE_TEST_FILE" "$CLUSTER_UNBOUNDED_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_UNBOUNDED_TABLE" "$AGG_PROP_NAME_UNBOUNDED_TABLE" "" "unbounded" "$PROPERTIES_UNBOUNDED_JOB"
    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac


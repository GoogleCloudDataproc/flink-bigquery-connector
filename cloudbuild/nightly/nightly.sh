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

# Function to run the test to check BQ Table Read and Write.
run_read_write_test(){
  PROJECT_ID=$1
  REGION_FILE=$2
  CLUSTER_FILE=$3
  PROJECT_NAME=$4
  DATASET_NAME=$5
  # SOURCE is Source Table Name in case of bounded mode and GCS URI in case of unbounded mode
  SOURCE=$6
  DESTINATION_TABLE_NAME=$7
  IS_EXACTLY_ONCE_ENABLED=$8
  MODE=$9
  PROPERTIES=${10}
  SINK_PARALLELISM=${11}
  # Take default value = false in case not provided.
  IS_SQL=${12:-False}
  ENABLE_TABLE_CREATION=${13:-False}
  # Get the final region and the cluster name.
  export REGION=$(cat "$REGION_FILE")
  export CLUSTER_NAME=$(cat "$CLUSTER_FILE")
  export GCS_JAR_LOCATION=$(cat "$GCS_JAR_LOCATION_FILE")

  # Run the test.
  source cloudbuild/nightly/scripts/table_write.sh "$PROJECT_ID" "$CLUSTER_NAME" "$REGION" "$PROJECT_NAME" "$DATASET_NAME" "$SOURCE" "$DESTINATION_TABLE_NAME" "$IS_EXACTLY_ONCE_ENABLED" "$MODE" "$PROPERTIES" "$SINK_PARALLELISM" "$IS_SQL" "$ENABLE_TABLE_CREATION"
}

# Delete cluster and its buckets.
delete_cluster(){
  PROJECT_ID=$1
  REGION_FILE=$2
  CLUSTER_FILE=$3
  export REGION=$(cat "$REGION_FILE")
  export CLUSTER_NAME=$(cat "$CLUSTER_FILE")
  python3 cloudbuild/nightly/scripts/python-scripts/delete_buckets_and_clusters.py -- --cluster_name "$CLUSTER_NAME" --region "$REGION" --project_id "$PROJECT_ID"
}

case $STEP in
  # Download maven and all the dependencies
  init)
    timestamp=$(date +"%Y%m%d%H%M%S")
    export GCS_JAR_LOCATION="$GCS_JAR_LOCATION"/"$timestamp"/"$GCS_JAR_NAME"
    $MVN clean install -DskipTests -Pflink_1.17
    gcloud storage cp "$MVN_JAR_LOCATION" "$GCS_JAR_LOCATION"
    echo "$GCS_JAR_LOCATION" > "$GCS_JAR_LOCATION_FILE"
    exit
    ;;

  # Create the cluster - Small Read-Write bounded job.
  create_clusters_bounded_small_table)
    create_cluster "$CLUSTER_NAME_SMALL_TEST" "$REGION_ARRAY_STRING_SMALL_TEST" "$NUM_WORKERS_SMALL_TEST" "$REGION_SMALL_TEST_FILE" "$WORKER_MACHINE_TYPE_SMALL_BOUNDED" "$CLUSTER_SMALL_TEST_FILE"
    exit
    ;;

  # Create the cluster - Large Table Read-Write bounded job.
  create_clusters_bounded_large_table)
    create_cluster "$CLUSTER_NAME_LARGE_TABLE_TEST" "$REGION_ARRAY_STRING_LARGE_TABLE_TEST" "$NUM_WORKERS_LARGE_TABLE_TEST" "$REGION_LARGE_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_LARGE_BOUNDED" "$CLUSTER_LARGE_TABLE_TEST_FILE"
    exit
    ;;

  # Create the cluster - Large Table Read-Write bounded job.
  create_clusters_table_api_bounded_large_table)
    create_cluster "$CLUSTER_NAME_TABLE_API_LARGE_TABLE_TEST" "$REGION_ARRAY_STRING_TABLE_API_LARGE_TABLE_TEST" "$NUM_WORKERS_LARGE_TABLE_TEST" "$REGION_TABLE_API_LARGE_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_LARGE_BOUNDED" "$CLUSTER_TABLE_API_LARGE_TABLE_TEST_FILE"
    exit
    ;;

  # Create the cluster - Unbounded Read-Write job.
  create_clusters_unbounded_table)
    create_cluster "$CLUSTER_NAME_UNBOUNDED_TABLE_TEST" "$REGION_ARRAY_STRING_UNBOUNDED_TABLE_TEST" "$NUM_WORKERS_UNBOUNDED_TABLE_TEST" "$REGION_UNBOUNDED_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_UNBOUNDED" "$CLUSTER_UNBOUNDED_TABLE_TEST_FILE"
    exit
    ;;

  # Create the cluster - Table API Unbounded Read-Write job.
  create_clusters_table_api_unbounded_table)
    create_cluster "$CLUSTER_NAME_TABLE_API_UNBOUNDED_TABLE_TEST" "$REGION_ARRAY_STRING_TABLE_API_UNBOUNDED_TABLE_TEST" "$NUM_WORKERS_UNBOUNDED_TABLE_TEST" "$REGION_TABLE_API_UNBOUNDED_TABLE_TEST_FILE" "$WORKER_MACHINE_TYPE_UNBOUNDED" "$CLUSTER_TABLE_API_UNBOUNDED_TABLE_TEST_FILE"
    exit
    ;;

  # Run the nested schema bounded e2e test.
  e2e_bounded_nested_schema_test)
    IS_SQL=False
    ENABLE_TABLE_CREATION=True
    IS_EXACTLY_ONCE_ENABLED=True
    run_read_write_test "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SOURCE_COMPLEX_SCHEMA_TABLE" "$TABLE_NAME_DESTINATION_COMPLEX_SCHEMA_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB" "$SINK_PARALLELISM_SMALL_BOUNDED_JOB" "$IS_SQL" "$ENABLE_TABLE_CREATION"
    exit
    ;;

  # Run the nested schema bounded Table API e2e test.
  e2e_bounded_table_api_nested_schema_test)
    IS_SQL=True
    ENABLE_TABLE_CREATION=True
    IS_EXACTLY_ONCE_ENABLED=False
    run_read_write_test "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SOURCE_COMPLEX_SCHEMA_TABLE" "$TABLE_NAME_DESTINATION_COMPLEX_SCHEMA_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB" "$SINK_PARALLELISM_SMALL_BOUNDED_JOB" "$IS_SQL" "$ENABLE_TABLE_CREATION"
    exit
    ;;

  # Run the all datatypes bounded Table API e2e test.
  e2e_bounded_table_api_all_datatypes_test)
    IS_SQL=True
    ENABLE_TABLE_CREATION=True
    IS_EXACTLY_ONCE_ENABLED=False
    run_read_write_test "$PROJECT_ID" "$REGION_SMALL_TEST_FILE" "$CLUSTER_SMALL_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SOURCE_ALL_DATATYPES_TABLE" "$TABLE_NAME_DESTINATION_ALL_DATATYPES_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "bounded" "$PROPERTIES_SMALL_BOUNDED_JOB" "$SINK_PARALLELISM_SMALL_BOUNDED_JOB" "$IS_SQL" "$ENABLE_TABLE_CREATION"
    exit
    ;;

  # Run the large table bounded e2e test.
  e2e_bounded_large_table_test)
    # Run the large table test.
    IS_EXACTLY_ONCE_ENABLED=True
    run_read_write_test "$PROJECT_ID" "$REGION_LARGE_TABLE_TEST_FILE" "$CLUSTER_LARGE_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SOURCE_LARGE_TABLE" "$TABLE_NAME_DESTINATION_LARGE_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "bounded" "$PROPERTIES_LARGE_BOUNDED_JOB" "$SINK_PARALLELISM_LARGE_BOUNDED_JOB"
    exit
    ;;

  # Run the Table API large table bounded e2e test.
  e2e_bounded_table_api_large_table_test)
    # Run the large table test.
    IS_SQL=True
    IS_EXACTLY_ONCE_ENABLED=False
    run_read_write_test "$PROJECT_ID" "$REGION_TABLE_API_LARGE_TABLE_TEST_FILE" "$CLUSTER_TABLE_API_LARGE_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$TABLE_NAME_SOURCE_TABLE_API_LARGE_TABLE" "$TABLE_NAME_DESTINATION_TABLE_API_LARGE_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "bounded" "$PROPERTIES_LARGE_BOUNDED_JOB" "$SINK_PARALLELISM_LARGE_BOUNDED_JOB" "$IS_SQL"
    exit
    ;;

  # Run the unbounded e2e test.
  e2e_unbounded_test)
    IS_EXACTLY_ONCE_ENABLED=False
    run_read_write_test "$PROJECT_ID" "$REGION_UNBOUNDED_TABLE_TEST_FILE" "$CLUSTER_UNBOUNDED_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$GCS_SOURCE_URI" "$TABLE_NAME_DESTINATION_UNBOUNDED_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "unbounded" "$PROPERTIES_UNBOUNDED_JOB" "$SINK_PARALLELISM_UNBOUNDED_JOB"
    exit
    ;;

  # Run the Table API unbounded e2e test.
  e2e_table_api_unbounded_test)
    IS_SQL=True
    IS_EXACTLY_ONCE_ENABLED=True
    run_read_write_test "$PROJECT_ID" "$REGION_TABLE_API_UNBOUNDED_TABLE_TEST_FILE" "$CLUSTER_TABLE_API_UNBOUNDED_TABLE_TEST_FILE" "$PROJECT_NAME" "$DATASET_NAME" "$GCS_SOURCE_URI" "$TABLE_NAME_DESTINATION_UNBOUNDED_TABLE" "$IS_EXACTLY_ONCE_ENABLED" "unbounded" "$PROPERTIES_UNBOUNDED_JOB" "$SINK_PARALLELISM_UNBOUNDED_JOB" "$IS_SQL"
    exit
    ;;

  # Relinquish the underlying infra running these tests.
  delete_cluster)
    delete_cluster "$PROJECT_ID" "$REGION_FILE" "$CLUSTER_FILE"
    exit
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac

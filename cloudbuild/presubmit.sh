
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
    $MVN install -DskipTests
    exit
    ;;

  # Run unit tests
  unittest)
    $MVN test jacoco:report jacoco:report-aggregate 
    ;;

  # Run integration tests
  integrationtest)
    $MVN failsafe:integration-test failsafe:verify jacoco:report jacoco:report-aggregate 
    ;;

  # Run e2e tests
  e2etest)
    gcloud config set project testproject-398714
    gcloud dataproc jobs submit flink --jar=gs://connector-buck/flink-bq-connector/flink-app-jars/1.15.4/bounded/BigQueryExample.jar --cluster=flink-bounded-source-connector --region=asia-east2 -- --gcp-project testproject-398714 --bq-dataset babynames --bq-table names_2014 --agg-prop name
    ;;

  *)
    echo "Unknown step $STEP"
    exit 1
    ;;
esac

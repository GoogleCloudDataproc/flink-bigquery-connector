#!/bin/bash
set -xeu

if [ "$#" -ne 3 ] 
  then
    echo "Usage : sh run-example.sh <gcp project> <bq dataset> <bq table>" 
    exit -1
fi

PROJECT_ID=$1
DATASET=$2
TABLE=$3

pushd ..
mvn clean install 
popd

LAUNCH_PARAMS=" \
 --gcp-project=$PROJECT_ID \
 --bq-dataset=$DATASET \
 --bq-table=$TABLE\
 "

mvn compile exec:java -Dexec.mainClass=org.apache.flink.examples.gcp.bigquery.BigQueryExample -Dexec.cleanupDaemonThreads=false -Dexec.args="${LAUNCH_PARAMS}"


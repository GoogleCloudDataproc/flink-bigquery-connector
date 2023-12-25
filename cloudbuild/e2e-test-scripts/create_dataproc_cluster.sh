#This is for the unbounded read.
#PROPERTIES=flink:state.backend.type=rocksdb,flink:state.backend.incremental=true,flink:state.backend.rocksdb.memory.managed=true,flink:state.checkpoints.dir=gs://flink-bq-connector-nightly-job/flink-bq-connector-checkpoint-dir/,flink:taskmanager.numberOfTaskSlots=2,flink:parallelism.default=4,flink:jobmanager.memory.process.size=2708m,flink:taskmanager.memory.process.size=2708m,flink:classloader.resolve-order=parent-first

#This is for the bounded read.
#PROPERTIES=flink:state.backend.type=rocksdb,flink:state.backend.incremental=true,flink:state.backend.rocksdb.memory.managed=true,flink:state.checkpoints.dir=gs://flink-bq-connector-nightly-job/flink-bq-connector-checkpoint-dir/,flink:taskmanager.numberOfTaskSlots=4,flink:parallelism.default=16,flink:jobmanager.memory.process.size=7g,flink:taskmanager.memory.process.size=10g,flink:classloader.resolve-order=parent-first
CLUSTER_NAME=$1
REGION=$2
ZONE=$3
PROPERTIES=$4
NUM_WORKERS=$5
TEMP_BUCKET=$6
STAGING_BUCKET=$7

# Set the project, location and zone for the cluster creation.
gcloud config set project "$PROJECT_ID"
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "$ZONE"

# Create the temp bucket for the cluster.
gcloud storage buckets create gs://"$TEMP_BUCKET" --project="$PROJECT_ID" --location="$REGION"
# Create the staging bucket for the cluster.
gcloud storage buckets create gs://"$STAGING_BUCKET" --project="$PROJECT_ID" --location="$REGION"
# Create the cluster
gcloud dataproc clusters create "$CLUSTER_NAME" \
    --region="$REGION" \
    --image-version="$DATAPROC_IMAGE_VERSION" \
    --optional-components=FLINK \
    --enable-component-gateway \
    --properties="$PROPERTIES" \
    --num-masters=1 \
    --num-workers="$NUM_WORKERS" \
    --bucket="$STAGING_BUCKET" \
    --temp-bucket="$TEMP_BUCKET"

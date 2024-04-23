# Apache Flink Google BigQuery Connector (Under Development)

[![CodeQL](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml)
[![codecov](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector)

The connector supports streaming data from [Google BigQuery](https://cloud.google.com/bigquery/) tables to Apache Flink, 
and writing results back to BigQuery tables.
This is done by using the [Flink’s Datastream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/) 
to communicate with BigQuery.

## Public Preview

This connector is a work in progress, and here we’re providing a preview of its capabilities. It currently offers 
the feature to read data from a BigQuery table into a Flink application, and the ability to write results of Flink jobs 
to BigQuery tables with at-least-once write consistency. Exactly-once consistency will be offered soon. Users 
should note this is an experimental instrument, and we guarantee no SLOs at this stage.

## Apache Flink

Apache Flink is an open source framework and distributed processing engine for stateful computations over unbounded 
and bounded data streams. Learn more about Flink [here](https://flink.apache.org).

## BigQuery Storage APIs

### Write API

The Storage [write API](https://cloud.google.com/bigquery/docs/write-api) is a high-performance data-ingestion API for BigQuery.

#### Stream-level transactions

Write data to a stream and commit the data as a single transaction. If the commit operation fails, safely retry 
the operation. Multiple workers can create their own streams to process data independently.

#### Efficient protocol

The Storage Write API uses gRPC streaming rather than REST over HTTP. The Storage Write API also supports binary 
formats in the form of protocol buffers, which are a more efficient wire format than JSON. Write requests are 
asynchronous with guaranteed ordering.

#### Exactly-once delivery semantics

The Storage Write API supports exactly-once semantics through the use of stream offsets. This will be used when 
offering an exactly-once Sink for BigQuery.

### Read API

The Storage [read API](https://cloud.google.com/bigquery/docs/reference/storage) streams data in parallel directly from 
BigQuery via gRPC without using Google Cloud Storage as an intermediary.

Following are some benefits of using the Storage API:

#### Direct Streaming

It does not leave any temporary files in Google Cloud Storage. Rows are read directly from BigQuery servers using the 
Avro wire format.

#### Filtering

The API allows column and predicate filtering to only read the data you are interested in.

##### Column Filtering

Since BigQuery is backed by a columnar datastore, it can efficiently stream data without reading all columns.

##### Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters.

#### Dynamic Sharding

The API rebalances records between readers until they all complete.

## Requirements

### Enable the BigQuery Storage API

Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api). 
For write APIs, ensure [following pemissions](https://cloud.google.com/bigquery/docs/write-api#required_permissions) are 
granted. 
For read APIs, ensure [following permissions](https://cloud.google.com/bigquery/docs/reference/storage#permissions) are 
granted.

### Prerequisites

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 8

### Downloading the Connector

There are two ways to access the connector.

#### Maven Central

The connector is available on the [Maven Central](https://repo1.maven.org/maven2/com/google/cloud/flink/)
repository.

| Flink version | Connector Artifact                                                       |
|---------------|--------------------------------------------------------------------------|
| Flink 1.17.x  | `com.google.cloud.flink:flink-1.17-connector-bigquery:0.2.0-preview`     |

#### GitHub

Users can obtain the connector artifact from our [GitHub repository](https://github.com/GoogleCloudDataproc/flink-bigquery-connector).

##### Steps to Build Locally

```shell
git clone https://github.com/GoogleCloudDataproc/flink-bigquery-connector
cd flink-bigquery-connector
git checkout tags/v0.2.0-preview
mvn clean install -DskipTests -Pflink_1.17
```

Resulting jars can be found in the target directory of respective modules, i.e. 
`flink-bigquery-connector/flink-1.17-connector-bigquery/flink-connector-bigquery/target` for the connector, 
and `flink-bigquery-connector/flink-1.17-connector-bigquery/flink-connector-bigquery-examples/target` for a sample 
application.

Maven artifacts are installed under `.m2/repository`.

If only the jars are needed, then execute maven `package` instead of `install`.

### Connector to Flink Compatibility

| Connector tag \ Flink version | 1.15.x | 1.17.x |
|-------------------------------|--------|--------|
| 0.1.0-preview                 | ✓      | ✓      |
| 0.2.0-preview                 | ✓      | ✓      |

### Create a Google Cloud Dataproc cluster (Optional)

A Google Cloud Dataproc cluster can be used as an execution environment for Flink runtime. Here we attach relevant 
documentation to execute Flink applications on Cloud Dataproc, but you can deploy the Flink runtime in other Google 
Cloud environments (like [GKE](https://cloud.google.com/kubernetes-engine)) and submit jobs using Flink CLI or web UI.

Dataproc clusters will need the `bigquery` or `cloud-platform` scopes. Dataproc clusters have the `bigquery` scope 
by default, so most clusters in enabled projects should work by default. 

#### Dataproc Flink Component

Follow [this document](https://cloud.google.com/dataproc/docs/concepts/components/flink).

#### Connector to Dataproc Image Compatibility Matrix

| Connector tag \ Dataproc Image | 2.1 | 2.2 |
|--------------------------------|-----|-----|
| 0.1.0-preview                  | ✓   | ✓   |
| 0.2.0-preview                  | ✓   | ✓   |

## Usage

The connector uses Flink’s Datastream API, and can be used in Java applications. For a Flink source, it offers two read 
modes, bounded and unbounded.

### Compiling against the connector

#### Maven Dependency

```xml
<dependency>
  <groupId>com.google.cloud.flink</groupId>
  <artifactId>flink-1.17-connector-bigquery</artifactId>
  <version>0.2.0-preview</version>
</dependency>
```

#### Relevant Files

* Sink factory methods are defined at `com.google.cloud.flink.bigquery.sink.BigQuerySink`.
* Sink configs are defined at `com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig`.
* Source factory methods are defined at `com.google.cloud.flink.bigquery.source.BigQuerySource`.
* Source configs are defined at `com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions`.
* BigQuery connection config is defined at `com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions`.
* Sample Flink application using connector is defined at `com.google.cloud.flink.bigquery.examples.BigQueryExample`.

### At Least Once Sink

Flink [Sink](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/api/connector/sink2/Sink.html) 
is the base interface for developing a sink. With checkpointing enabled, it can offer at-least-once consistency. Our 
implementation uses BigQuery Storage's [default write stream](https://cloud.google.com/bigquery/docs/write-api#default_stream) 
in Sink's [Writers](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/api/connector/sink2/SinkWriter.html).

```java
// StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// env.enableCheckpointing(checkpointInterval);

BigQueryConnectOptions sinkConnectOptions =
        BigQueryConnectOptions.builder()
                .setProjectId(...)
                .setDataset(...)
                .setTable(...)
                .build();
BigQuerySchemaProvider schemaProvider = new BigQuerySchemaProviderImpl(sinkConnectOptions);
BigQuerySinkConfig sinkConfig =
        BigQuerySinkConfig.newBuilder()
                .connectOptions(sinkConnectOptions)
                .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .schemaProvider(schemaProvider)
                .serializer(new AvroToProtoSerializer())
                .build();

Sink<GenericRecord> sink = BigQuerySink.get(sinkConfig, env);
```

* BigQuery sinks require that checkpoint is enabled for at-least-once consistency.
* Delivery guarantee must be [at-least-once](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#AT_LEAST_ONCE).
* [BigQueryConnectOptions](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-connector-bigquery-common/src/main/java/com/google/cloud/flink/bigquery/common/config/BigQueryConnectOptions.java) 
stores information needed to connect to a BigQuery table.
* [AvroToProtoSerializer](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/AvroToProtoSerializer.java) 
is the only out-of-the-box serializer offered for now. It expects data to arrive at the sink as avro's GenericRecord. Other 
relevant data formats will be supported soon. Also, users can create their own implementation of 
[BigQueryProtoSerializer](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryProtoSerializer.java) 
for other data formats.
* [BigQuerySchemaProvider](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQuerySchemaProvider.java) 
exposes schema related information about the BigQuery table. This is needed by the sink to write data to BigQuery tables. It 
can also be used by the serializer if needed (for instance, the AvroToProtoSerializer uses BigQuery table's schema).
* Flink cannot automatically serialize avro's GenericRecord, hence users must explicitly specify type information 
when using the AvroToProtoSerializer. Check Flink's [blog on non-trivial serialization](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#AT_LEAST_ONCE). 
Note that the avro schema needed here can be obtained from BigQuerySchemaProvider.
* The maximum parallelism of BigQuery sinks has been capped at 100. This is to respect BigQuery storage 
[write quotas](https://cloud.google.com/bigquery/quotas#write-api-limits) while adhering to 
[best usage practices](https://cloud.google.com/bigquery/docs/write-api-best-practices). Users should either set 
[sink level parallelism](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/execution/parallel/#operator-level) 
explicitly, or ensure that default job level parallelism is under 100.
* Users are recommended to choose their application's [restart strategy](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/task_failure_recovery/) 
wisely, so as to avoid incessant retries which can potentially disrupt the BigQuery Storage API backend. Regardless of which 
strategy is adopted, the restarts must be finite and graciously spaced.
* If a data record cannot be serialized by BigQuery sink, then the record is dropped with a warning getting logged. Moving on, 
we plan to introduce a Flink metric for tracking such data. Additionally, a dead letter queue will be introduced in the future 
to store this data.

**Important:** Please refer to [data ingestion pricing](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing) to 
understand the BigQuery Storage Write API pricing.

### Unbounded Source

A timestamp [partitioned table](https://cloud.google.com/bigquery/docs/partitioned-tables) will be continuously checked for 
“completed” partitions, which the connector will stream into the Flink application.

```java
BigQuerySource<GenericRecord> source =
    BigQuerySource.streamAvros(
        BigQueryReadOptions.builder()
            .setBigQueryConnectOptions(
                BigQueryConnectOptions.builder()
                    .setProjectId(...)
                    .setDataset(...)
                    .setTable(...)
                    .build())
            .setColumnNames(...)
            .setLimit(...)
            .setMaxRecordsPerSplitFetch(...)
            .setMaxStreamCount(...)
            .setOldestPartitionId(...)
            .setPartitionDiscoveryRefreshIntervalInMinutes(...)
            .setRowRestriction(...)
            .setSnapshotTimestampInMillis(...)
            .build());
```

* A partition is considered “complete” if the table’s write buffer’s oldest entry’s ingestion time is after the partition’s 
end.
* If the table’s write buffer is empty, then a partition is considered complete if `java.time.Instant.now()` is after the 
partition’s end.
* This approach is susceptible to out-of-order data, and we plan to replace it with a lateness tolerance beyond the 
partition’s end in future releases.

### Bounded Source

#### Table

A table will be read once, and its rows at the time will be streamed into the Flink application.

```java
BigQuerySource<GenericRecord> source =
    BigQuerySource.readAvros(
        BigQueryReadOptions.builder()
        .setBigQueryConnectOptions(
            BigQueryConnectOptions.builder()
            .setProjectId(...)
            .setDataset(...)
            .setTable(...)
            .build())
        .setColumnNames(...)
        .setLimit(...)
        .setMaxRecordsPerSplitFetch(...)
        .setMaxStreamCount(...)
        .setRowRestriction(...)
        .setSnapshotTimestampInMillis(...)
        .build());
```

#### Query

A SQL query will be executed in the GCP project, and its [view](https://cloud.google.com/bigquery/docs/views-intro) will 
be streamed into the Flink application.

```java
BigQuerySource<GenericRecord> bqSource =
    BigQuerySource.readAvrosFromQuery(query, projectId, limit);
    // OR
    BigQuerySource.readAvrosFromQuery(query, projectId);
```

* Operations (like JOINs) which can be performed as queries in BigQuery should be executed this way because they’ll be more 
efficient than Flink, and only the result will be transmitted over the wire.
* Since BigQuery executes the query and stores results in a temporary table, this may add additional costs on your BigQuery 
account.
* The connector’s query source offers limited configurability compared to bounded/unbounded table reads. This will be 
addressed in future releases.
* The connector does not manage query generated views beyond creation and read. Users will need to 
[manage these views](https://cloud.google.com/bigquery/docs/managing-views) on their own, until future releases expose a 
configuration in the connector to delete them or assign a time-to-live.

### Connector Source Configurations

The connector supports a number of options to configure the source.

| Property                                     | Data Type          | Description                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `projectId`                                  | String             | Google Cloud Project ID of the table. This config is required, and assumes no default value.                                                                                                                                                                                                                                                              |
| `dataset`                                    | String             | Dataset containing the table. This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                                                            |
| `table`                                      | String             | BigQuery table in the format `[[projectId:]dataset.]table`. This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                              |
| `credentialsOptions`                         | CredentialsOptions | Google credentials for connecting to BigQuery. This config is optional, and default behavior is to use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.<br/>**Note**: The query bounded source only uses default application credentials.                                                                                                       |
| `query`                                      | String             | BigQuery SQL query used in the bounded query source. This config is not required for bounded table or unbounded source.                                                                                                                                                                                                                                   |
| `columnNames`                                | List&lt;String&gt; | Columns to project from the table. This config is used in bounded table or unbounded source. If unspecified, all columns are fetched.                                                                                                                                                                                                                     |
| `limit`                                      | Integer            | Maximum number of rows to read from table. This config is used in all source types. If unspecified, all rows are fetched.                                                                                                                                                                                                                                 |
| `maxRecordsPerSplitFetch`                    | Integer            | Maximum number of records to read from a split once Flink requests fetch. This config is used in bounded table or unbounded source. If unspecified, the default value used is 10000. <br/>**Note**: Configuring this number too high may cause memory pressure in the task manager, depending on the BigQuery record's size and total rows on the stream. |
| `maxStreamCount`                             | Integer            | Maximum read streams to open during a read session. BigQuery can return a lower number of streams than specified based on internal optimizations. This config is used in bounded table or unbounded source. If unspecified, this config is not set and BigQuery has complete control over the number of read streams created.                             |
| `rowRestriction`                             | String             | BigQuery SQL query for row filter pushdown. This config is used in bounded table or unbounded source. If unspecified, all rows are fetched.                                                                                                                                                                                                               |
| `snapshotTimeInMillis`                       | Long               | Time (in milliseconds since epoch) for the BigQuery table snapshot to read. This config is used in bounded table or unbounded source. If unspecified, the latest snapshot is read.                                                                                                                                                                        |
| `oldestPartitionId`                          | String             | Earliest table partition to consider for unbounded reads. This config is used in unbounded source. If unspecified, all partitions are read.                                                                                                                                                                                                               |
| `partitionDiscoveryRefreshIntervalInMinutes` | Integer            | Periodicity (in minutes) of partition discovery in table. This config is used in unbounded source. If unspecified, the default value used is 10 minutes.                                                                                                                                                                                                  |

### Datatypes

All the current BigQuery datatypes are being handled when transforming data from BigQuery to Avro’s `GenericRecord`.

| BigQuery Data Type | Converted Avro Datatype |
|--------------------|-------------------------|
| `STRING`           | `STRING`                |
| `GEOGRAPHY`        | `STRING`                |
| `BYTES`            | `BYTES`                 | 
| `INTEGER`          | `LONG`                  |
| `INT64`            | `LONG`                  |
| `FLOAT`            | `DOUBLE`                |
| `FLOAT64`          | `DOUBLE`                |
| `NUMERIC`          | `BYTES`                 |
| `BIGNUMERIC`       | `BYTES`                 |
| `BOOLEAN`          | `BOOLEAN`               |
| `BOOL`             | `BOOLEAN`               |
| `TIMESTAMP`        | `LONG`                  |
| `RECORD`           | `RECORD`                |
| `STRUCT`           | `RECORD`                |
| `DATE`             | `STRING`, `INT`         |
| `DATETIME`         | `STRING`                |
| `TIME`             | `STRING`, `LONG`        |
| `JSON`             | `STRING`                |

## Example Application

The `flink-1.17-connector-bigquery-examples` module offers a sample Flink application powered by the connector. It can be 
found at `com.google.cloud.flink.bigquery.examples.BigQueryExample`. It offers an intuitive hands-on application with 
elaborate guidance to test out the connector and its various configurations.

## FAQ

### What is the pricing for the Storage API? 

See the [BigQuery Pricing Documentation](https://cloud.google.com/bigquery/pricing#storage-api).

### How do I authenticate outside GCE / Dataproc?

The connector needs an instance of a GoogleCredentials in order to connect to the BigQuery APIs. There are multiple options 
to provide it:
- The default is to load the JSON key from the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, as described 
[here](https://cloud.google.com/docs/authentication/client-libraries).
- In case the environment variable cannot be changed, the credentials file can be configured as a connector option. The 
file should reside on the same path on all the nodes of the cluster.

### How to fix classloader error in Flink application?

Change Flink’s classloader strategy to `parent-first`. This can be made default in the flink-conf yaml.

### How to fix issues with checkpoint storage when running Flink on Dataproc?

Point flink-conf yaml’s `state.checkpoints.dir` to a bucket in Google Storage, as file system storages are more suitable 
for yarn applications.

### How to fix "Attempting to create more Sink Writers than allowed"?

The maximum parallelism of BigQuery sinks has been capped at 100. Please set sink level parallelism or default job level 
parallelism as 100 or less.

### Why are certain records missing even with at-least-once consistency guarantee?

Records that cannot be serialized to BigQuery protobuf format are dropped with a warning being logged. In future, a Flink metric 
and dead letter queues will be supported to better track such records.

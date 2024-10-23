# Apache Flink Google BigQuery Connector (Under Development)

[![CodeQL](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml)
[![codecov](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector)

The connector supports streaming data from [Google BigQuery](https://cloud.google.com/bigquery/) tables to Apache Flink, 
and writing results back to BigQuery tables. 
This data exchange with BigQuery is supported via [Flink’s Datastream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/) 
as well as [Flink's Table API and SQL](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/overview/).

## Public Preview

This connector is in public preview stage, with GA planned for Q1 2025. It offers the feature to read data 
from a BigQuery table into a Flink application, and the ability to write results of Flink jobs 
to BigQuery tables. The BigQuery sink supports at-least-once and exactly-once write consistencies.

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

The Storage Write API supports exactly-once semantics through the use of stream offsets.

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
* Java 11

### Downloading the Connector

There are two ways to access the connector.

#### Maven Central

The connector is available on the [Maven Central](https://repo1.maven.org/maven2/com/google/cloud/flink/)
repository.

| Flink version | Connector Artifact                                           | Key Features                |
|---------------|--------------------------------------------------------------|-----------------------------| 
| Flink 1.17.x  | `com.google.cloud.flink:flink-1.17-connector-bigquery:0.2.0` | At-least Once Sink Support  | 
| Flink 1.17.x  | `com.google.cloud.flink:flink-1.17-connector-bigquery:0.3.0` | Table API Support           |
| Flink 1.17.x  | `com.google.cloud.flink:flink-1.17-connector-bigquery:0.4.0` | Exactly Once Sink Support   |

#### GitHub

Users can obtain the connector artifact from our [GitHub repository](https://github.com/GoogleCloudDataproc/flink-bigquery-connector).

##### Steps to Build Locally

```shell
git clone https://github.com/GoogleCloudDataproc/flink-bigquery-connector
cd flink-bigquery-connector
git checkout tags/0.4.0
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
| 0.2.0                         | ✓      | ✓      |
| 0.3.0                         | ✓      | ✓      |
| 0.4.0                         | ✓      | ✓      |

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
| 0.2.0                          | ✓   | ✓   |
| 0.3.0                          | ✓   | ✓   |
| 0.4.0                          | ✓   | ✓   |

## Usage
The connector can be used with Flink's Datastream and Table APIs in Java applications.
The source offers two read modes, bounded and unbounded. 
The sink offers at-least-once delivery guarantee.

### Compiling against the connector

#### Maven Dependency

```xml
<dependency>
  <groupId>com.google.cloud.flink</groupId>
  <artifactId>flink-1.17-connector-bigquery</artifactId>
  <version>0.4.0</version>
</dependency>
```

#### Relevant Files

* Sink can be created using `get` method at `com.google.cloud.flink.bigquery.sink.BigQuerySink`.
* Sink configuration is defined at `com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig`.
* Source factory methods are defined at `com.google.cloud.flink.bigquery.source.BigQuerySource`.
* Source configuration is defined at `com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions`.
* BigQuery connection configuration is defined at `com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions`.
* Sample Flink application using connector is defined at `com.google.cloud.flink.bigquery.examples.BigQueryExample` for the Datastream API,
  and at `com.google.cloud.flink.bigquery.examples.BigQueryTableExample` for the Table API and SQL.

### Datastream API

#### Sink

Flink [Sink](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/api/connector/sink2/Sink.html)
is the base interface for developing a sink. With checkpointing enabled, it can offer at-least-once consistency. Our
implementation uses BigQuery Storage's [default write stream](https://cloud.google.com/bigquery/docs/write-api#default_stream)
in Sink's [Writers](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/api/connector/sink2/SinkWriter.html).

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(checkpointInterval);

// Via DataStream API

BigQueryConnectOptions sinkConnectOptions =
        BigQueryConnectOptions.builder()
                .setProjectId(...) // REQUIRED
                .setDataset(...) // REQUIRED
                .setTable(...) // REQUIRED
                .build();
BigQuerySchemaProvider schemaProvider = new BigQuerySchemaProviderImpl(sinkConnectOptions);
DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE; // or DeliveryGuarantee.EXACTLY_ONCE
BigQuerySinkConfig sinkConfig =
        BigQuerySinkConfig.newBuilder()
                .connectOptions(sinkConnectOptions) // REQUIRED
                .streamExecutionEnvironment(env) // REQUIRED
                .deliveryGuarantee(deliveryGuarantee) // REQUIRED
                .schemaProvider(schemaProvider) // REQUIRED
                .serializer(new AvroToProtoSerializer()) // REQUIRED
                .build();

Sink<GenericRecord> sink = BigQuerySink.get(sinkConfig, env);
```

* BigQuery sinks require that checkpoint is enabled.
* Delivery guarantee can be [at-least-once](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#AT_LEAST_ONCE) or 
  [exactly-once](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#EXACTLY_ONCE).
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
* The maximum parallelism of BigQuery sinks has been capped at 128. This is to respect BigQuery storage
  [write quotas](https://cloud.google.com/bigquery/quotas#write-api-limits) while adhering to
  [best usage practices](https://cloud.google.com/bigquery/docs/write-api-best-practices). Users should either set
  [sink level parallelism](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/execution/parallel/#operator-level)
  explicitly, or ensure that default job level parallelism is under 128.
* BigQuerySinkConfig requires the StreamExecutionEnvironment if delivery guarantee is exactly-once. This is to [validate](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/92db3690c741fb2cdb99e28c575e19affb5c8b69/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/BigQuerySinkConfig.java#L185) 
  the [restart strategy](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/task_failure_recovery/). 
  Users are recommended to choose their application's restart strategy wisely, to avoid incessant retries which can potentially 
  disrupt the BigQuery Storage API backend. Regardless of which strategy is adopted, the restarts must be finite and graciously 
  spaced.
* BigQuery sink's exactly-once mode follows the `Two Phase Commit` protocol. This means the data will be written to the 
  BigQuery table only at checkpoints. All data between two checkpoints is buffered in BigQuery's write streams, and committed 
  to the destination BigQuery table upon successful checkpoint completion.
* If a data record cannot be serialized by BigQuery sink, then the record is dropped with a warning getting logged. In future,
  we plan to use dead letter queues to capture such data.

**Important:** Please refer to [data ingestion pricing](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing) to
understand the BigQuery Storage Write API pricing.

#### Source: Unbounded

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
            .setColumnNames(...) // OPTIONAL
            .setLimit(...) // OPTIONAL
            .setMaxRecordsPerSplitFetch(...) // OPTIONAL
            .setMaxStreamCount(...) // OPTIONAL
            .setOldestPartitionId(...) // OPTIONAL
            .setPartitionDiscoveryRefreshIntervalInMinutes(...) // OPTIONAL
            .setRowRestriction(...) // OPTIONAL
            .setSnapshotTimestampInMillis(...) // OPTIONAL
            .build());
```

* A partition is considered “complete” if the table’s write buffer’s oldest entry’s ingestion time is after the partition’s
  end.
* If the table’s write buffer is empty, then a partition is considered complete if `java.time.Instant.now()` is after the
  partition’s end.
* This approach is susceptible to out-of-order data, and we plan to replace it with a lateness tolerance beyond the
  partition’s end in future releases.

#### Source: Bounded

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

##### Query

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

##### Connector Source Configurations

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


#### Datatypes

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

### Table API Support
* Table API is a high-level declarative API that allows users to describe what they want to do rather than how to do it. 
* This results in simpler customer code and higher level pipelines that are more easily optimized in a managed service.
* The Table API is a superset of the SQL language and is specially designed for working with Apache Flink.
* It also allows language-embedded style support for queries in Java, Scala or Python besides the always available String values as queries in SQL.

#### Sink
```java
// Note: Users must create and register a catalog table before reading and writing to them.
// Schema of the source and sink catalog table must be the same

// final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// env.enableCheckpointing(CHECKPOINT_INTERVAL);
// final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Create the Config.
BigQueryTableConfig sinkTableConfig = BigQuerySinkTableConfig.newBuilder()
        .table(...) // REQUIRED
        .project(...) // REQUIRED
        .dataset(...) // REQUIRED
        .streamExecutionEnvironment(env) // REQUIRED if deliveryGuarantee is EXACTLY_ONCE
        .sinkParallelism(...) // OPTIONAL; Should be atmost 128
        .deliveryGuarantee(...) // OPTIONAL; Default is AT_LEAST_ONCE
        .build();

// Register the Sink Table
tEnv.createTable(
        "bigQuerySinkTable",
        BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

// Insert entries in this sinkTable
sourceTable.executeInsert("bigQuerySinkTable");
```
Note: For jobs running on a dataproc cluster, via "gcloud dataproc submit", explicitly call `await()` after `executeInsert` to 
wait for the job to complete.

#### Source
```java
// Note: Users must create and register a catalog table before reading and writing to them.

// final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// env.enableCheckpointing(CHECKPOINT_INTERVAL);
// final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Create the Config.
BigQueryTableConfig readTableConfig =  new BigQueryReadTableConfig.Builder()
        .table(...) // REQUIRED
        .project(...) // REQUIRED
        .dataset(...) // REQUIRED
        .partitionDiscoveryInterval(...) // OPTIONAL; only in CONTINUOUS_UNBOUNDED source
        .boundedness(...) // OPTIONAL; Defaults to Boundedness.BOUNDED
        .limit(...) // OPTIONAL
        .columnProjection(...) // OPTIONAL
        .snapshotTimestamp(...) // OPTIONAL
        .rowRestriction(...) // OPTIONAL
        .build();

// Create the catalog table.
tEnv.createTable(
        "bigQuerySourceTable",
         BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));
Table sourceTable = tEnv.from("bigQuerySourceTable");

// Fetch entries in this sourceTable
sourceTable = sourceTable.select($("*"));
```

#### More Details:
* Input and Output tables (catalog tables) must be registered in the TableEnvironment.
* The schema of the registered table must match the schema of the query.</b>
* Boundedness must be either `Boundedness.CONTINUOUS_UNBOUNDED` or `Boundedness.BOUNDED`.
* Checkpointing must be enabled as mentioned above. Delivery guarantee must be at-least-once.
* [BigQueryTableConfig](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/table/config/BigQueryTableConfig.java) stores information needed to connect to a BigQuery table. It could even be used to obtain the TableDescriptor required for the creation of Catalog Table. <br/>Please refer to:
  * [BigQueryReadTableConfig](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/table/config/BigQueryReadTableConfig.java) for more details on available read configurations.
  * [BigQuerySinkTableConfig](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/table/config/BigQuerySinkTableConfig.java) for more details on available sink configurations.
* [RowDataToProtoSerializer](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/RowDataToProtoSerializer.java) is offered for serialization of `RowData` (since Table API read/writes `RowData` format records) records to BigQuery Proto Rows. This out-of-box serializer is automatically provided to the sink during runtime.
* [BigQueryTableSchemaProvider](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryTableSchemaProvider.java) is a helper class which contains the method `getTableDescriptor()` which could be used to obtain a [TableDescriptor](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/table/api/TableDescriptor.html) for creation of catalog table via `BigQueryTableConfig` (`BigQuerySinkTableConfig` for sink options and `BigQueryReadTableConfig` for read options). 
Users could also create their own catalog tables; provided the schema of the registered table, and the associated BigQuery table is the same.
* The connector supports a number of options to configure.

| Property                                     | Data Type         | Description                                                                                                                                                                                                                                                                                                                   | Availability                                         |
|----------------------------------------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------| 
| `projectId`                                  | String            | Google Cloud Project ID of the table. This config is required, and assumes no default value.                                                                                                                                                                                                                                  | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` | 
| `dataset`                                    | String            | Dataset containing the table. This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                                | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` |
| `table`                                      | String            | BigQuery table. This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                                              | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` |
| `credentialAccessToken`                      | String            | [Google Access token](https://cloud.google.com/docs/authentication/token-types#access) for connecting to BigQuery. This config is optional, and default behavior is to use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.                                                                                         | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` |
| `credentialFile`                             | String            | [Google credentials](https://developers.google.com/workspace/guides/create-credentials) for connecting to BigQuery. This config is optional, and default behavior is to use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.                                                                                        | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` |
| `credentialKey`                              | String            | [Google credentials Key](https://cloud.google.com/docs/authentication/api-keys) for connecting to BigQuery. This config is optional, and default behavior is to use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.                                                                                                | `BigQueryReadTableConfig`, `BigQuerySinkTableConfig` |
| `limit`                                      | Integer           | Maximum number of rows to read from table. This config is used in all source types. If unspecified, all rows are fetched.                                                                                                                                                                                                     | `BigQueryReadTableConfig`                            |
| `rowRestriction`                             | String            | BigQuery SQL query for row filter pushdown. This config is used in bounded table or unbounded source. If unspecified, all rows are fetched.                                                                                                                                                                                   | `BigQueryReadTableConfig`                            |
| `columnProjection`                           | String            | Columns (comma separated list of values) to project from the table. This config is used in bounded table or unbounded source. If unspecified, all columns are fetched.                                                                                                                                                        | `BigQueryReadTableConfig`                            |
| `maxStreamCount`                             | Integer           | Maximum read streams to open during a read session. BigQuery can return a lower number of streams than specified based on internal optimizations. This config is used in bounded table or unbounded source. If unspecified, this config is not set and BigQuery has complete control over the number of read streams created. | `BigQueryReadTableConfig`                            |
| `snapshotTimeInMillis`                       | Long              | Time (in milliseconds since epoch) for the BigQuery table snapshot to read. This config is used in bounded table or unbounded source. If unspecified, the latest snapshot is read.                                                                                                                                            | `BigQueryReadTableConfig`                            |
| `partitionDiscoveryRefreshIntervalInMinutes` | Integer           | Periodicity (in minutes) of partition discovery in table. This config is used in unbounded source. If unspecified, the default value used is 10 minutes.                                                                                                                                                                      | `BigQueryReadTableConfig`                            |
| `sinkParallelism`                            | Integer           | Integer value indicating the parallelism for the sink. This config is used in unbounded source and is optional. If unspecified, the application decides the optimal parallelism. <br/>Maximum value: 128.                                                                                                                     | `BigQuerySinkTableConfig`                            |
| `boundedness`                                | Boundedness       | Enum value indicating boundedness of the source. <br/> Possible values: `Boundedness.CONTINUOUS_UNBOUNDED` or `Boundedness.BOUNDED`. <br/> Default Value: `Boundedness.BOUNDED`                                                                                                                                               | `BigQueryReadTableConfig`                            |
| `deliveryGuarantee`                          | DeliveryGuarantee | Enum value indicating delivery guarantee of the source. <br/> Possible values: `DeliveryGuarantee.EXACTLY_ONCE` or `DeliveryGuarantee.AT_LEAST_ONCE`. <br/> Default Value: `DeliveryGuarantee.AT_LEAST_ONCE`                                                                                                                  | `BigQueryReadTableConfig`                            |
* Limitations:
    * Inability to read and then write `TIME` type BigQuery records. Reading `TIME` type records and subsequently writing them to BigQuery would result in an error due to misconfigured types between 
BigQuery and Flink's RowData. <br/> This misconfiguration only happens when BigQuery is used as both the source and sink, connector works as expected for correctly formatted `RowData` records read from other sources.
    * Incorrect value obtained during read and write of `BIGNUMERIC` type BigQuery Records. Reading `BIGNUMERIC` type records from a BigQuery table and subsequently writing them to 
BigQuery would result in incorrect value being written to BigQuery as Flink's RowData does not support NUMERIC Types with precision more than 38 (BIGNUMERIC supports precision up to 76). <br/> This mismatch only occurs due to bigquery's support for NUMERIC values with > 38 precision. The connector works as expected for other sources(even BigQuery) within the permitted(up to 38) range.
    * Supports only `INSERT` type operations such as `SELECT`/`WHERE`, `UNION`, `JOIN`, etc.

#### Catalog Tables:
* Catalog Table usage helps hide the complexities of interacting with different external systems behind a common interface.
* In Apache Flink, a CatalogTable represents the unresolved metadata of a table stored within a catalog.
* It is an encapsulation of all the characteristics that would typically define an SQL CREATE TABLE statement.
* This includes the table's schema (column names and data types), partitioning information, constraints etc.
  It doesn't contain the actual table data.
* SQL Command for Catalog Table Creation
  ```java
    CREATE TABLE sample_catalog_table
    (name STRING) // Schema Details
    WITH
    ('connector' = 'bigquery',
    'project' = '<bigquery_project_name>',
    'dataset' = '<bigquery_dataset_name>',
    'table' = '<bigquery_table_name>');
  ```
  
## Example Application

The `flink-1.17-connector-bigquery-examples`  and `flink-1.17-connector-bigquery-table-api-examples` modules offer a sample Flink application powered by the connector.
It can be found at `com.google.cloud.flink.bigquery.examples.BigQueryExample` for the Datastream API 
and at `com.google.cloud.flink.bigquery.examples.BigQueryTableExample` for the Table API and SQL.
It offers an intuitive hands-on application with elaborate guidance to test out the connector and 
its various configurations.

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

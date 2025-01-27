# Apache Flink Google BigQuery Connector

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
| Flink 1.17.x  | `com.google.cloud.flink:flink-1.17-connector-bigquery:0.5.0` | Table Creation by Sink      |

#### GitHub

Users can obtain the connector artifact from our [GitHub repository](https://github.com/GoogleCloudDataproc/flink-bigquery-connector).

##### Steps to Build Locally

```shell
git clone https://github.com/GoogleCloudDataproc/flink-bigquery-connector
cd flink-bigquery-connector
git checkout tags/0.5.0
mvn clean install -DskipTests -Pflink_1.17
```

Resulting jars can be found in the target directory of respective modules, i.e. 
`flink-bigquery-connector/flink-1.17-connector-bigquery/flink-connector-bigquery/target` for the connector, 
and `flink-bigquery-connector/flink-1.17-connector-bigquery/flink-connector-bigquery-examples/target` for a sample 
application.

Maven artifacts are installed under `.m2/repository`.

If only the jars are needed, then execute maven `package` instead of `install`.

#### Compilation Dependency (Maven)

```xml
<dependency>
  <groupId>com.google.cloud.flink</groupId>
  <artifactId>flink-1.17-connector-bigquery</artifactId>
  <version>0.5.0</version>
</dependency>
```

### Connector to Flink Compatibility

| Connector tag \ Flink version | 1.15.x | 1.17.x |
|-------------------------------|--------|--------|
| 0.1.0-preview                 | ✓      | ✓      |
| 0.2.0-preview                 | ✓      | ✓      |
| 0.2.0                         | ✓      | ✓      |
| 0.3.0                         | ✓      | ✓      |
| 0.4.0                         | ✓      | ✓      |
| 0.5.0                         | ✓      | ✓      |

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
| 0.5.0                          | ✓   | ✓   |


## Table API

* Table API is a high-level declarative API that allows users to describe what they want to do rather than how to do it. 
* This results in simpler customer code and higher level pipelines that are more easily optimized in a managed service.
* The Table API is a superset of the SQL language and is specially designed for working with Apache Flink.
* It also allows language-embedded style support for queries in Java, Scala or Python besides the always available String values as queries in SQL.

### Catalog Tables
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


## Sink

Flink [Sink](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/api/connector/sink2/Sink.html)
is the base interface for developing a sink. With checkpointing enabled, it can offer at-least-once or exactly-once 
consistency. It uses BigQuery Storage's [default write stream](https://cloud.google.com/bigquery/docs/write-api#default_stream) 
for at-least-once, and [buffered write stream](https://cloud.google.com/bigquery/docs/write-api#buffered_type) for 
exactly-once.

### Sink In Datastream API

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(checkpointInterval);

BigQueryConnectOptions sinkConnectOptions =
        BigQueryConnectOptions.builder()
                .setProjectId(...) // REQUIRED
                .setDataset(...) // REQUIRED
                .setTable(...) // REQUIRED
                .build();
DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE; // or EXACTLY_ONCE
BigQuerySinkConfig sinkConfig =
        BigQuerySinkConfig.newBuilder()
                .connectOptions(sinkConnectOptions) // REQUIRED
                .streamExecutionEnvironment(env) // REQUIRED
                .deliveryGuarantee(deliveryGuarantee) // REQUIRED
                .serializer(new AvroToProtoSerializer()) // REQUIRED
                .enableTableCreation(...) // OPTIONAL
                .partitionField(...) // OPTIONAL
                .partitionType(...) // OPTIONAL
                .partitionExpirationMillis(...) // OPTIONAL
                .clusteredFields(...) // OPTIONAL
                .region(...)  // OPTIONAL
                .build();

Sink<GenericRecord> sink = BigQuerySink.get(sinkConfig);
```

### Sink In Table API

```java
// final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// env.enableCheckpointing(CHECKPOINT_INTERVAL);
// final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Create the Config.
BigQuerySinkTableConfig sinkTableConfig = BigQuerySinkTableConfig.newBuilder()
        .table(...) // REQUIRED
        .project(...) // REQUIRED
        .dataset(...) // REQUIRED
        .streamExecutionEnvironment(env) // REQUIRED if deliveryGuarantee is EXACTLY_ONCE
        .sinkParallelism(...) // OPTIONAL;
        .deliveryGuarantee(...) // OPTIONAL; Default is AT_LEAST_ONCE
        .enableTableCreation(...) // OPTIONAL
        .partitionField(...) // OPTIONAL
        .partitionType(...) // OPTIONAL
        .partitionExpirationMillis(...) // OPTIONAL
        .clusteredFields(...) // OPTIONAL
        .region(...)  // OPTIONAL
        .build();

// Register the Sink Table
// If destination table already exists, then use:
tEnv.createTable(
        "bigQuerySinkTable",
        BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

// Else, define the table schema (ensure this matches the schema of records sent to sink)
org.apache.flink.table.api.Schema tableSchema = ...
// ... and use:
tEnv.createTable(
        "bigQuerySinkTable",
        BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig, tableSchema));

// Insert entries in this sinkTable
sourceTable.executeInsert("bigQuerySinkTable");
```
Note: For jobs running on a dataproc cluster, via "gcloud dataproc submit", explicitly call `await()` after `executeInsert` to 
wait for the job to complete.

### Sink Configurations

The connector supports a number of options to configure the source.

| Property                                     | Data Type              | Description                                                                                                                                                            |
|----------------------------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `projectId`                                  | String                 | Google Cloud Project ID of the table. This config is required.                                                                                                         |
| `dataset`                                    | String                 | Dataset containing the table. This config is required.                                                                                                                 |
| `table`                                      | String                 | BigQuery table name (not the full ID). This config is required.                                                                                                        |
| `credentialsOptions`                         | CredentialsOptions     | Google credentials for connecting to BigQuery. This config is optional, and default behavior is to use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.      |
| `deliveryGuarantee`                          | DeliveryGuarantee      | Write consistency guarantee of the sink. This config is required.                                                                                                      |
| `enableTableCreation`                        | Boolean                | Allows the sink to create the destination BigQuery table (mentioned above) if it doesn't already exist. This config is optional.                                       |
| `partitionField`                             | String                 | Column to partition new sink table. This config is optional, and considered if enableTableCreation is true.                                                            |
| `partitionType`                              | TimePartitioning.Type  | Column to partition new sink table. This config is optional, and considered if enableTableCreation is true.                                                            |
| `partitionExpirationMillis`                  | Long                   | Expiration time of partitions in new sink table. This config is optional, and considered if enableTableCreation is true.                                               |
| `clusteredFields`                            | List&lt;String&gt;     | Columns used for clustering new sink table. This config is optional, and considered if enableTableCreation is true.                                                    |
| `region`                                     | String                 | BigQuery region to create the dataset (mentioned above) if it doesn't already exist. This config is optional, and considered if enableTableCreation is true.           |
| `sinkParallelism`                            | Integer                | Sink's parallelism. This config is optional, and available only when sink is used with Table API.                                                                      |

Knowing that this sink offers limited configurability when creating destination BigQuery table, we'd like to highlight that almost all
configurations or modifications to a BigQuery table are possible after table creation. For example, adding primary keys requires
[this simple SQL query](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#alter_table_add_primary_key_statement).
Check out BigQuery SQL's [DDL](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language),
[DML](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax) and
[DCL](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language) syntax for details.

### Sink Details [MUST READ]

* BigQuery sinks require that checkpoint is enabled.
* When using a BigQuery sink, checkpoint timeout should be liberal (1+ minutes). This is because sink writers are
  [throttled](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/05fe7b14f2dc688bc808f553c3f863ba2380e317/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/throttle/BigQueryWriterThrottler.java#L26) 
  before they start sending data to BigQuery. Depending on the sink's parallelism, this throttling can be as high as 40 seconds. 
  Throttling is necessary to gracefully handle BigQuery's rate limiting on certain APIs used by the sink writers. Note that this only
  affects the first checkpoint.
* Delivery guarantee can be [at-least-once](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#AT_LEAST_ONCE) or 
  [exactly-once](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#EXACTLY_ONCE).
* The at-least-once sink enables BigQuery client [multiplexing](https://cloud.google.com/bigquery/docs/write-api-streaming#use_multiplexing)
  by default, which optimizes usage of BigQuery write APIs. This is not possible in exactly-once sink.
* [AvroToProtoSerializer](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/AvroToProtoSerializer.java)
  is the only out-of-the-box serializer offered for Datastream API. It expects data to arrive at the sink as avro's GenericRecord.
  Users can create their own implementation of
  [BigQueryProtoSerializer](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQueryProtoSerializer.java)
  for other data formats.
* Flink cannot automatically serialize avro's GenericRecord, hence users must explicitly specify type information
  when maintaining records in avro format. Check Flink's [blog on non-trivial serialization](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/base/DeliveryGuarantee.html#AT_LEAST_ONCE).
  Note that avro schema of an existing BigQuery table can be obtained from
  [BigQuerySchemaProviderImpl](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/serializer/BigQuerySchemaProviderImpl.java).
* The maximum parallelism of BigQuery sinks has been capped at **512** for multi-regions US or EU, and **128** for the rest.
  This is to respect BigQuery storage [write quotas](https://cloud.google.com/bigquery/quotas#write-api-limits) while keeping
  [throughput](https://cloud.google.com/bigquery/docs/write-api#connections) and
  [best usage practices](https://cloud.google.com/bigquery/docs/write-api-best-practices) in mind. Users should either set
  [sink level parallelism](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/execution/parallel/#operator-level)
  explicitly, or ensure that default job level parallelism is under region-specific maximums (512 or 128).
* BigQuerySinkConfig requires the StreamExecutionEnvironment if delivery guarantee is exactly-once.   
  **Restart strategy must be explicitly set in the StreamExecutionEnvironment**.  
  This is to [validate](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/92db3690c741fb2cdb99e28c575e19affb5c8b69/flink-1.17-connector-bigquery/flink-connector-bigquery/src/main/java/com/google/cloud/flink/bigquery/sink/BigQuerySinkConfig.java#L185) 
  the [restart strategy](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/task_failure_recovery/). 
  Users are recommended to choose their application's restart strategy wisely, to avoid incessant retries which can potentially 
  exhaust your BigQuery resource quota, and disrupt the BigQuery Storage API backend. Regardless of which strategy is 
  adopted, the restarts must be finite and graciously spaced.  
  **Using fixed delay restart is strongly discouraged, as a potential crash loop can quickly evaporate your project's Biguery resource quota.**
* BigQuery sink's exactly-once mode follows the `Two Phase Commit` protocol. All data between two checkpoints is buffered in 
  BigQuery's write streams, and committed to the destination BigQuery table upon successful checkpoint completion. This means 
  that new data will be visible in the BigQuery table only at checkpoints.
* If a data record cannot be serialized by BigQuery sink, then the record is dropped with a warning getting logged. In future,
  we plan to use dead letter queues to capture such data.

**Important:** Please refer to [data ingestion pricing](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing) to
understand the BigQuery Storage Write API pricing.

### Relevant Files

* Sink can be created using `get` method at `com.google.cloud.flink.bigquery.sink.BigQuerySink`.
* Sink configuration for Datastream API is defined at `com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig`.
* Sink configuration for Table/SQL API is defined at `com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig`.
* BigQuery connection configuration is defined at `com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions`.
* Sample Flink application using connector is defined at `com.google.cloud.flink.bigquery.examples.BigQueryExample` for the Datastream API,
  and at `com.google.cloud.flink.bigquery.examples.BigQueryTableExample` for the Table API and SQL.


## Source

### Bounded Source In Datastream API

A table will be read once, and its rows at the time will be streamed into the Flink application.

```java
// Sets source boundedness to Boundedness.BOUNDED
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

#### Bounded Source Using BigQuery Query In Datastream API

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

### Source In Table API

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
        .boundedness(...) // DEPRECATED: Defaults to Boundedness.BOUNDED
        .partitionDiscoveryInterval(...) // DEPRECATED: Use if boundedness is CONTINUOUS_UNBOUNDED
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

### [DEPRECATED] Unbounded Source In Datastream API

A timestamp [partitioned table](https://cloud.google.com/bigquery/docs/partitioned-tables) will be continuously checked for
“completed” partitions, which the connector will stream into the Flink application.

```java
// Sets source boundedness to Boundedness.CONTINUOUS_UNBOUNDED
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

### Source Configurations

The connector supports a number of options to configure the source.

| Property                                     | Data Type          | Description                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `projectId`                                  | String             | Google Cloud Project ID of the table. This config is required, and assumes no default value.                                                                                                                                                                                                                                                              |
| `dataset`                                    | String             | Dataset containing the table. This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                                                            |
| `table`                                      | String             | BigQuery table name (not the full ID). This config is required for standard tables, but not when loading query results.                                                                                                                                                                                                                                   |
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
| `boundedness`                                | Boundedness        | Enum value indicating boundedness of the source. Only used with Flink's Table API, when setting BigQueryReadTableConfig. Default Value is `Boundedness.BOUNDED`                                                                                                                                                                                           |


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

### Relevant Files

* Source factory methods are defined at `com.google.cloud.flink.bigquery.source.BigQuerySource`.
* Source configuration for Datastream API is defined at `com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions`.
* Source configuration for Table/SQL API is defined at `com.google.cloud.flink.bigquery.table.config.BigQueryReadTableConfig`.
* BigQuery connection configuration is defined at `com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions`.
* Sample Flink application using connector is defined at `com.google.cloud.flink.bigquery.examples.BigQueryExample` for the Datastream API,
  and at `com.google.cloud.flink.bigquery.examples.BigQueryTableExample` for the Table API and SQL.


## Flink Metrics
Apache Flink allows collecting metrics internally to better understand the status of jobs and 
clusters during the development process.
Each operator in Flink maintains its own set of metrics,
which are collected by the Task Manager where the operator is running.
Currently, the Flink-BigQuery Connector 
supports collection and reporting of the following metrics in BigQuery sink:

| Metric Name                                        | Metric Description                                                                                                                                                                                                                                                                                                                                                               | Supported By (At-least Once Sink /Exactly Once Sink) |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `numberOfRecordsSeenByWriter`                      | Counter to keep track of the total number of records seen by the writer.                                                                                                                                                                                                                                                                                                         | At-least Once Sink, Exactly Once Sink                |
| `numberOfRecordsSeenByWriterSinceCheckpoint `      | Counter to keep track of the number of records seen by the writer since the last checkpoint.                                                                                                                                                                                                                                                                                     | At-least Once Sink, Exactly Once Sink                |
| `numberOfRecordsWrittenToBigQuery`                 | Counter to keep track of the number of records successfully written to BigQuery until now.                                                                                                                                                                                                                                                                                       | At-least Once Sink, Exactly Once Sink                |
| `numberOfRecordsWrittenToBigQuerySinceCheckpoint`  | Counter to keep track of the number of records successfully written to BigQuery since the last checkpoint.                                                                                                                                                                                                                                                                       | At-least Once Sink                                   |
| `numberOfRecordsBufferedByBigQuerySinceCheckpoint` | Counter to keep track of the number of records currently buffered by the Storage Write API stream before committing them to the BigQuery Table. These records will be added to the Table following [Two Phase Commit Protocol's](https://nightlies.apache.org/flink/flink-docs-release-1.20/api/java/org/apache/flink/api/connector/sink2/Committer.html) `commit()` invocation. | Exactly Once Sink                                    |


## Example Application

The `flink-1.17-connector-bigquery-examples`  and `flink-1.17-connector-bigquery-table-api-examples` modules offer a sample Flink application powered by the connector.
It can be found at `com.google.cloud.flink.bigquery.examples.BigQueryExample` for the Datastream API 
and at `com.google.cloud.flink.bigquery.examples.BigQueryTableExample` for the Table API and SQL.
It offers an intuitive hands-on application with elaborate guidance to test out the connector and 
its various configurations.


## FAQ

Detailed guide for known issues and troubleshooting can be found [here](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/TROUBLESHOOT.md).

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

### Why are certain records missing despite at-least-once or exactly-once consistency guarantee in the sink?

Records that cannot be serialized to BigQuery protobuf format are dropped with a warning being logged. In future, dead letter 
queues will be supported to store such records.

### Why is Flink checkpoint timing out with exactly-once consistency guarantee in the sink?

Exactly-once sink uses BigQuery's buffered write streams, which must be created at a rate of 3 per second. This requires 
throttling the sink's writers before they can send data to BigQuery. A result of this throttling is a delay in the first 
checkpoint's completion. The recommended solution here is to increase the checkpoint timeout, to accommodate this slow start.

### Why is data not visible in BigQuery table with exactly-once consistency guarantee in the sink?

Exactly-once sink used the Two Phase Commit protocol, where data is committed to BigQuery tables only at checkpoints. A high 
level view of the architecture is:
- data between two checkpoints (say, n-1 and n) is buffered in BigQuery write streams, and
- this buffered data is committed to the BigQuery table when checkpoint (here, n) is successfully completed.
The first phase can be viewed as buffering all the data between two checkpoints, and second phase is committing that data 
upon confirmation of first phase's success.

### Why does the exactly-once sink require explicitly setting the restart strategy?
Users are recommended to choose their application's restart strategy wisely and explicitly, to avoid incessant retries 
which can potentially eat up your BigQuery resource quota, and disrupt the BigQuery Storage API backend. Regardless of 
which strategy is adopted, the restarts must be finite and graciously spaced.

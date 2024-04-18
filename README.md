# Apache Flink Google BigQuery Connector (Under Development)

[![CodeQL](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/actions/workflows/codeql-analysis.yml)
[![codecov](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudDataproc/flink-bigquery-connector)

The connector supports streaming data from [Google BigQuery](https://cloud.google.com/bigquery/) tables to Apache Flink, 
and writing results back to BigQuery tables.
This is done by using the [Flink’s Datastream API](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/overview/) 
to communicate with BigQuery.

## Public Preview

This connector is a work in progress, and here we’re providing the first preview of its capabilities. It currently offers 
the feature to read data from a BigQuery table into a Flink application, and the ability to write results of Flink jobs 
to BigQuery tables with at-least-once write consistency. Exactly-once consistency will be offered in the near future. Users 
should note this is an experimental instrument, and we guarantee no SLOs at this stage.

## Apache Flink

Apache Flink is an open source framework and distributed processing engine for stateful computations over unbounded and 
bounded data streams. Learn more about Flink [here](https://flink.apache.org).

## BigQuery Storage API

The [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) streams data in parallel directly from 
BigQuery via gRPC without using Google Cloud Storage as an intermediary.

Following are some benefits of using the Storage API:

### Direct Streaming

It does not leave any temporary files in Google Cloud Storage. Rows are read directly from BigQuery servers using the 
Avro wire format.

### Filtering

The API allows column and predicate filtering to only read the data you are interested in.

#### Column Filtering

Since BigQuery is backed by a columnar datastore, it can efficiently stream data without reading all columns.

#### Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters.

### Dynamic Sharding

The API rebalances records between readers until they all complete.

## Requirements

### Enable the BigQuery Storage API

Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

### Downloading and Using the Connector

Users are expected to clone and build this repository locally in order to obtain the connector jar and/or maven artifact. 
Being an experimental preview, this connector has not been published to maven central yet.

#### Prerequisites

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 8

#### Steps

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

#### Connector to Flink Compatibility

| Connector tag \ Flink version | 1.15.x | 1.17.x |
|-------------------------------|--------|--------|
| 0.1.0-preview                 | ✓      | ✓      |
| 0.2.0-preview                 | ✓      | ✓      |

### Create a Google Cloud Dataproc cluster (Optional)

If you do not have an Apache Flink environment you can create a Cloud Dataproc cluster with pre-configured auth. Here we 
attach relevant documentation to execute Flink applications on Cloud Dataproc, but you can deploy the Flink runtime in any 
environment and submit jobs using Flink CLI or web UI.

Any Dataproc cluster using the API needs the `bigquery` or `cloud-platform` scopes. Dataproc clusters have the `bigquery` 
scope by default, so most clusters in enabled projects should work by default. 

#### Dataproc Flink Component

Follow [this document](https://cloud.google.com/dataproc/docs/concepts/components/flink).

#### Connector to Dataproc Image Compatibility Matrix

| Connector tag \ Dataproc Image | 2.1 |
|--------------------------------|-----|
| 0.1.0-preview                  | ✓   |
| 0.2.0-preview                  | ✓   |

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
elaborate guidance to test out the connector and its various configurations across different source read modes.

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

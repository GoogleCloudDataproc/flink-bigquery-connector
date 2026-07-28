# Release Notes

## Next

* Support indirect writes in DataStream and Table/SQL API using BigQuery Load Jobs via GCS staging, including support for large workloads (>15 TB) and configurable job project.
* Support querying BigQuery views in Flink Table/SQL API.
* Support BigQuery upsert in Flink 2.1 and add integration tests for Flink 2.1.
* Support nested projection pushdown and nested fields in row-restrictions (filter pushdown).
* Add timeout handling for buffered and default writers in BigQuery sink.
* Fix LIKE filter pushdown never being applied.
* Update filter pushdown logic so only filters rejected by BigQuery remain in Flink.
* Fix critical data loss bug in reader split handling by signaling no-more-splits per reader and removing completed readers from queue.
* Populate default columns when selecting zero columns to prevent querying empty column sets from BigQuery.
* Encode optimal default read stream counts based on task parallelism.
* Fix missing transitive dependencies in unshaded JAR's published POM.
* Update dependencies (Avro 1.11.5, Log4j 2.25.4, Hadoop Common 3.4.0, Flink Table Runtime 2.1.2).

## 1.1.0 - 2026-02-11
* PR #260: Upgrade dependencies versions.
* PR #258: Fix temporal type formatting in BigQueryRestriction to consistently use 6 decimal digits

## 1.0.0 - 2025-02-25

* Propagate generics to BigQuerySink and BigQuerySinkConfig. Users of DataStream API
will need to strongly type the sink's input in BigQuerySinkConfig. SQL/Table API users
will not be affected.
* Increase maximum allowed sink parallelism to 512 for BigQuery's multi-regions (US and EU).
* Remove unbounded source and bounded query source.
* Create a shaded jar for the connector library.
* Allow sink to throw a fatal error if record cannot be serialized to BigQuery's input format in sink.
* Fix integer and float handling in sink by upcasting to long and double respectively.
Check [issue 219](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/issues/219) for details.

## 0.5.0 - 2025-01-15

* Support creation of new table in BigQuery sink. This is integrated with Datastream and Table/SQL API.
* Remove need for BigQuerySchemaProvider in BigQuery sink configs.
* Deprecate unbounded source. To be completely removed in next release.

## 0.4.0 - 2024-11-04

* Support exactly-once consistency in BigQuery sink. This is integrated with Datastream and Table/SQL API.
* Add Flink metrics for monitoring BigQuery sink.
* Package unshaded guava dependency for enforcing the correct version used by BigQuery client.

## 0.3.0 - 2024-08-07

* Support BigQuery sink in Flink's Table API.
* BigQuery sink's maximum parallelism is increased from 100 to 128, beyond which the application will fail.
* Modifies the following config keys for connector source in Table API:

| Before                    | After                      |
|---------------------------|----------------------------|
| `read.discoveryinterval`  | `read.discovery-interval`  |
| `credentials.accesstoken` | `credentials.access-token` |
| `read.streams.maxcount`   | `read.streams.max-count`   |

## 0.2.0 - 2024-05-13

* Release BigQuery sink with at-least-once support.
* Avro's GenericRecord to BigQuery proto is the only out-of-the-box serializer offered for now.
* BigQuery sink's maximum parallelism is capped at 100, beyond which the application with fail.

## 0.1.0-preview - 2023-12-14

* Initial release with BQ source support

# Release Notes

## Next

* Propagate generics to BigQuerySink and BigQuerySinkConfig. Users of DataStream API
will need to strongly type the sink's input in BigQuerySinkConfig. SQL/Table API users
will not be affected.
* Increase maximum allowed sink parallelism to 512 for BigQuery's multi-regions (US and EU).
* Remove unbounded source and bounded query source.
* Create a shaded jar for the connector library.
* Allow sink to throw a fatal error if record cannot be serialized to BigQuery's input format in sink.
* Force upcasting of integer and float to long and double in sink.

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

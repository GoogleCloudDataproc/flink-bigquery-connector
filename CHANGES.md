# Release Notes

## Next

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

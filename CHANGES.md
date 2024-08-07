# Release Notes

## Next

## 0.3.0 - 2024-08-07
* Release BigQuery sink with Table API Support.
* BigQuery sink's maximum parallelism is capped at 128, beyond which the application will fail.
* Modifies a few config keys for the pre-existing source implementation for the Table API:

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

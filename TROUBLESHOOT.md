# Debug Manual Flink - BigQuery Connector

## Overview
When problems occur, debug tools enable users to quickly pinpoint the root cause, whether it's 
within the connector itself, the BigQuery API, or the network infrastructure.
Detailed logs, traces, and debugging capabilities help you effectively troubleshoot issues, 
reducing the time and effort required to resolve them.
This document is a debugging manual highlighting possible issues that could be faced by the users 
and how to troubleshoot them. It also aids in identifying the error causes and proposed steps for 
mitigating them.

<i>Note: This is a running document containing the issues reported to the Developers of the 
[Flink - BigQuery Connector](https://github.com/GoogleCloudDataproc/flink-bigquery-connector). 
Please feel free to contribute as per any new issues that may arise.</i>

## Flink Metrics
### Writing to BigQuery
As an effort to increase observability, the Flink-BigQuery Connector Team provides support to 
collect and report Flink Metrics for a Flink Application.
The details of the metrics supported so far are available in the 
[README](https://github.com/GoogleCloudDataproc/flink-bigquery-connector/blob/main/README.md#flink-metrics).

An overview of records seen by the writer and number of records successfully written to 
BigQuery would enable users to track the flow of records through their application. 
Comparison of the counts would help troubleshoot if records are being seen by the Sink (Writer) 
at all, are being serialized and send to the Write API and if BigQuery is able to write 
these records.

See Flink’s documentation on 
[Metric Reporters](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/metric_reporters/) 
to deploy the reporter most conveniently as per user’s need.

## General Debugging
### Records are not being written to BigQuery
With the help of metrics available as a part of 0.4.0 release of the connector, 
users should be able to track the number of records that enter the sink(writer) and the 
number of records successfully written to BigQuery. 
If records are not being written to BigQuery, then records are stuck in either of the two phases:
#### The records are not arriving at the sink
- The problem lies with the pipeline, the previous chain of subtasks that are performed before 
sink is called.
- The pipeline is not processing and passing the records forward for the sink.

#### The records are arriving at the sink but not being successfully written to BigQuery.
- Check the logs or error message for the following errors:
     - #### `BigQuerySerializationException`
     - This message illustrates that the record(s) could not be serialized by the connector. 
     - The error message would also contain the actual cause for the same.
     - Note: This error is not thrown but logged, 
indicating that the connector was "Unable to serialize record" due to this error.
     - #### `BigQueryConnectorException`
     - Refer to Java Docs for the BigQuery Storage Write API proper usage and the errors that might
arise from the same.
- Ensure checkpointing is enabled and properly configured. Setting too large checkpointing 
intervals can cause records to not be written for long periods of time.

## Known Issues/Limitations
<b><i>Note: Users must go through the readme documentation for the connector first to 
ensure proper usage of the connector. 
The connector also has certain limitations which are documented in the readme.</i></b>
### Flink Application with Windows
- Windows are tricky to deal with in Flink. Flink relies on watermarks to close windows. 
- In case no records are written to BigQuery, users can observe the flow of records through 
the application to ensure the sink (writer) is receiving records. 
- In case the sink is not receiving any records, then the windows are not being closed. 
- The incoming records are windowed together by Flink Application that is continuously waiting 
for closing events (as per the windowing condition) to arrive.
- [Flink's Documentation on Debugging Windows](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/debugging/debugging_event_time/)

### BigQuery sink does not appear to be writing consistently
- The Connector in At Least Once mode writes to BigQuery at two instances - when the append request 
reaches its limit and when checkpoints are triggered. 
- The write to BigQuery API would not be at a steady rate, it would contain spiking intervals 
pertaining to each checkpoint event when data is being written to BigQuery.
- This is applicable for exactly once mode.

### Misconfigured Table Schema goes uncaught
- It must be ensured that the avro records passed via the connector have the correct schema or 
compatible schema to that of the BigQuery Table.
- It is also expected that the value passed in the Avro Generic Record follows the Schema.  
Here the "records passed" indicates the modified records after passing through the series of 
subtasks defined in the application pipeline.
- In case of a mismatch between the passed avro record value type and expected type,
`BigQueryConnectorException` is thrown.
- Flink does not impose a check on the value of a Generic Record, which means that the Avro 
Record's Schema could indicate a `INTEGER` type but yet store a `STRING` type value in the field.
- Hence, a few cases arise due to a mismatch of the avro record value, its schema and expected 
BigQuery Schema:
#### For Datastream API
##### Case 1: The Record's Schema is incompatible with the BigQuery Table Schema
- <b>Record value matches the expected BQ schema type</b> (but does not follow the schema rules)
    - <i>Example: Read from a table having field of type `STRING` and write to BQ table having 
field of type `INTEGER`, but the record value is modified from the read string input to a 
long value.</i>
  - This case works without any problem since the serializer matches the expected BQ Schema type
to the passed record value.
- <b>Record value matches the record schema type (but incompatible with BQ Table Schema)
<br>
OR
<br>
- Record value does not match either schema type </b>
  - This is a mismatch between the expected BQ Schema type and the passed value. 
So, `BigQueryConnectorException` would be thrown indicating that the pipeline is unable to 
forward the record to the next operator (sink).
##### Case 2: The Record's Schema is compatible with BigQuery Table Schema
- <b> Record value matches the record schema type and the expected BQ schema type </b>

  - The desired case, no issues.
- <b> Record value does not match either schema type </b>

  - This is a mismatch between the expected BQ Schema type and the passed value. 
So, `BigQueryConnectorException` would be thrown indicating that the pipeline is unable to 
forward the record to the next operator (sink).

####  For Table API
##### Case 1: The record schema is incompatible with the BigQuery Table Schema
- The schema of the source (transformed source table) should match the schema of the sink table 
(BQ Table).
- In case of a mismatch `org.apache.flink.table.api.ValidationException` is thrown.
##### Case 2: The records schema is compatible with BigQuery table schema
- <b> Record value matches the record schema type and the expected BQ schema type </b>
  
  - The desired case, no issues.
- <b> Record value does not match either schema type </b>

  - `org.apache.flink.table.api.ValidationException` is thrown when the RowData values do not match 
the described schema.

### Records are not available in the BigQuery Table when checkpointing is disabled.
- The connector relies on checkpoints for triggering writes to the BigQuery table.
- It is important to enable checkpointing and configure the interval suitably.

### Problems related to unbounded reads
- Unbounded reads currently have limitations, 
and may not work as expected, so users should avoid using them for now.
- It will be temporarily unavailable as we develop a significantly improved version.
- Expect an enhanced feature in 2025!

[//]: # (READ PROBLEMS)
### Flink BigQuery Connector Fails to read record fields of type `RANGE`
- The connector does not support reading from and writing to BigQuery's `RANGE` data type.

### Flink BigQuery Connector Fails to read record fields of type `RECORD` and MODE `NULLABLE`
- Reading a NULLABLE field of type `record` is not supported and throws an exception. 
- The read works fine when the record field has the value null.
- The above failure is due to the inbuilt `org.apache.flink.formats.avro.typeutils.AvroSerializer` 
is unable to serialize nullable type records.

### BigQuery `INTEGER` type field is read as a long value and not int.
- BigQuery `INTEGER` is used to represent numbers upto 64 bit.
- However, java int is only capable of holding 32 bit numbers.
- Hence, to accommodate all the values BQ field is capable of holding, the `INTEGER`
field is read as a java long instead of java int.

## Additional Debugging facilities offered by Apache Flink
Flink Offers these extra features that might help users to expand their explainability of the 
application beyond what is mentioned above.
- [End to end latency tracking](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/metrics/#end-to-end-latency-tracking)
- [Structuring Flink Logs](https://logback.qos.ch/manual/mdc.html)
    - Users can then parse these logs for constructing dashboards
- [Flamegraphs](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/debugging/flame_graphs/)


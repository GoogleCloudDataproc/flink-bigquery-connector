/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.protobuf.ByteString;

import java.util.concurrent.ExecutionException;

/**
 * Writer implementation for {@link BigQueryDefaultSink}.
 *
 * <p>This writer appends records to the BigQuery table's default write stream. This means that
 * records are written directly to the table with no additional commit required, and available for
 * querying immediately.
 *
 * <p>In case of stream replay upon failure recovery, records will be written again, regardless of
 * appends prior to the application's failure.
 *
 * <p>Records are grouped to maximally utilize the BigQuery append request's payload.
 *
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: at-least-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled: no write consistency.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryDefaultWriter<IN> extends BaseWriter<IN> {
    // Counter specific to at-least-once Implementation since records are written after
    // checkpointing
    // in Exactly Once mode.
    Counter numberOfRecordsWrittenToBigQuerySinceCheckpoint;

    public BigQueryDefaultWriter(
            String tablePath,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer,
            InitContext context) {
        super(context.getSubtaskId(), tablePath, connectOptions, schemaProvider, serializer);
        streamName = String.format("%s/streams/_default", tablePath);
        totalRecordsSeen = 0L;
        totalRecordsWritten = 0L;
        initializeAtleastOnceFlinkMetrics(context);
    }

    /**
     * Initialize Flink Metrics for at-least-once approach.
     *
     * @param context Sink Context to derive the Metric Group.ß
     */
    void initializeAtleastOnceFlinkMetrics(InitContext context) {
        SinkWriterMetricGroup sinkWriterMetricGroup = context.metricGroup();
        // Call BaseWriter's initializeMetrics() for common metrics.
        super.initializeMetrics(sinkWriterMetricGroup);
        this.numberOfRecordsWrittenToBigQuerySinceCheckpoint =
                sinkWriterMetricGroup.counter("numberOfRecordsWrittenToBigQuerySinceCheckpoint");
    }

    /**
     * Accept record for writing to BigQuery table.
     *
     * @param element Record to write
     * @param context {@link Context} for input record
     */
    @Override
    public void write(IN element, Context context) {
        totalRecordsSeen++;
        this.numberOfRecordsSeenByWriter.inc();
        this.numberOfRecordsSeenByWriterSinceCheckpoint.inc();
        try {
            ByteString protoRow = getProtoRow(element);
            if (!fitsInAppendRequest(protoRow)) {
                validateAppendResponses(false);
                append();
            }
            addToAppendRequest(protoRow);
        } catch (BigQuerySerializationException e) {
            logger.error(String.format("Unable to serialize record %s. Dropping it!", element), e);
        }
    }

    /** Overwriting flush() method for updating Flink Metrics in at-least-once Approach. */
    @Override
    public void flush(boolean endOfInput) {
        super.flush(endOfInput);
        // Writer's flush() is called at checkpoint,
        // resetting the counters to 0 after all operations in BaseWriter's flush() are complete.
        this.numberOfRecordsSeenByWriterSinceCheckpoint.dec(
                this.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        this.numberOfRecordsWrittenToBigQuerySinceCheckpoint.dec(
                this.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
    }

    /** Asynchronously append to BigQuery table's default stream. */
    @Override
    void sendAppendRequest(ProtoRows protoRows) {
        if (streamWriter == null) {
            createStreamWriter(true);
        }
        ApiFuture<AppendRowsResponse> response = streamWriter.append(protoRows);
        appendResponseFuturesQueue.add(
                new AppendInfo(response, -1L, Long.valueOf(protoRows.getSerializedRowsCount())));
    }

    /** Throws a RuntimeException if an error is found with append response. */
    @Override
    void validateAppendResponse(AppendInfo appendInfo) {
        // Offset has no relevance when appending to the default write stream.
        ApiFuture<AppendRowsResponse> appendResponseFuture = appendInfo.getFuture();
        long recordsAppended = appendInfo.getRecordsAppended();
        AppendRowsResponse response;
        try {
            response = appendResponseFuture.get();
            if (response.hasError()) {
                logAndThrowFatalException(response.getError().getMessage());
            }
            totalRecordsWritten += recordsAppended;
            // the request succeeded without errors (records are in BQ)
            this.numberOfRecordsWrittenToBigQuery.inc(recordsAppended);
            this.numberOfRecordsWrittenToBigQuerySinceCheckpoint.inc(recordsAppended);
        } catch (ExecutionException | InterruptedException e) {
            logAndThrowFatalException(e);
        }
    }
}

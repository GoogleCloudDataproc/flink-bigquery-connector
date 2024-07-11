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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions.OffsetAlreadyExists;
import com.google.cloud.bigquery.storage.v1.Exceptions.OffsetOutOfRange;
import com.google.cloud.bigquery.storage.v1.Exceptions.StreamFinalizedException;
import com.google.cloud.bigquery.storage.v1.Exceptions.StreamNotFound;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.throttle.Throttler;
import com.google.cloud.flink.bigquery.sink.throttle.WriteStreamCreationThrottler;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Writer implementation for {@link BigQueryBufferedSink}.
 *
 * <p>Each {@link BigQueryBufferedWriter} will write to an exclusive write stream, implying same
 * number of active write streams as writers at any given point of time.
 *
 * <p>This writer appends records to the BigQuery table's buffered write stream. This means that
 * records are buffered in the stream until flushed (BigQuery write API, different from sink
 * writer's flush). Records will be written to the destination table after the BigQuery flush API is
 * invoked by {@link BigQueryCommitter}, at which point it will be available for querying.
 *
 * <p>In case of stream replay upon failure recovery, previously buffered data will be discarded and
 * records will be buffered again from the latest checkpoint.
 *
 * <p>Records are grouped to maximally utilize the BigQuery append request's payload.
 *
 * <p>Depending on the checkpointing mode, this writer offers following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: exactly-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>{Checkpointing disabled}: no write consistency.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryBufferedWriter<IN> extends BaseWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    // Write stream creation must be throttled to ensure proper client usage.
    private final Throttler writeStreamCreationThrottler;

    // Offset position where next append should occur in current stream.
    private long streamOffset;

    // Number of rows appended by this writer to current stream.
    private long appendRequestRowCount;

    // Number of stream appends invoked by this writer on current stream.
    private long streamAppendCount;

    public BigQueryBufferedWriter(
            int subtaskId,
            String streamName,
            long streamOffset,
            String tablePath,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer) {
        super(subtaskId, tablePath, connectOptions, schemaProvider, serializer);
        this.streamName = streamName;
        this.streamOffset = streamOffset;
        writeStreamCreationThrottler = new WriteStreamCreationThrottler(subtaskId);
        appendRequestRowCount = 0L;
        streamAppendCount = 0L;
    }

    /**
     * Accept record for writing to BigQuery table.
     *
     * @param element Record to write
     * @param context {@link Context} for input record
     */
    @Override
    public void write(IN element, Context context) {
        try {
            ByteString protoRow = getProtoRow(element);
            if (!fitsInAppendRequest(protoRow)) {
                validateAppendResponses(false);
                append();
            }
            addToAppendRequest(protoRow);
            appendRequestRowCount++;
        } catch (BigQuerySerializationException e) {
            logger.error(String.format("Unable to serialize record %s. Dropping it!", element), e);
        }
    }

    /**
     * Asynchronously append to BigQuery table's buffered stream.
     *
     * <p>If a writer has been initialized for the very first time, then it will not have an
     * associated write stream and must create one before appending data to it.
     *
     * <p>If a writer has been restored after failure recovery, then it already has an associated
     * stream. Before appending data to it again, the writer needs to check if this stream is still
     * usable. The stream may be corrupt due to several reasons (listed below in code), in which
     * case it must be discarded and the writer will create a new write stream. If the stream was
     * not corrupt and is indeed usable, then the writer will continue appending to it.
     */
    @Override
    void sendAppendRequest(ProtoRows protoRows) {
        if (streamAppendCount == 0 && streamName != null && !streamName.isEmpty()) {
            // Writer has an associated write stream. Try appending data to it.
            try {
                // Connectrion pool can be disabled only for default stream.
                streamWriter = createStreamWriter(false);
            } catch (BigQueryConnectorException e) {
                // If StreamWriter could not be created for this write stream, then discard it.
                logStreamNotUsable(e);
                discardStreamAndResendAppendRequest(protoRows);
                return;
            }
            ApiFuture<AppendRowsResponse> future = streamWriter.append(protoRows, streamOffset);
            AppendRowsResponse response;
            try {
                // Get this future immediately to analyse whether append worked or not, i.e stream
                // is usable or not.
                response = future.get();
            } catch (ExecutionException e) {
                if (e.getCause().getClass() == OffsetAlreadyExists.class
                        || e.getCause().getClass() == StreamFinalizedException.class
                        || e.getCause().getClass() == OffsetOutOfRange.class) {
                    logStreamNotUsable(e);
                    try {
                        writeClient.finalizeWriteStream(streamName);
                    } catch (Exception innerException) {
                        // Do not fret!
                        // This is not fatal and can be ignored.
                        logger.trace(
                                String.format(
                                        "Failed while finalizing write stream %s in subtask %d",
                                        streamName, subtaskId),
                                innerException);
                    }
                    discardStreamAndResendAppendRequest(protoRows);
                    return;
                }
                if (e.getCause().getClass() == StreamNotFound.class) {
                    logStreamNotUsable(e);
                    discardStreamAndResendAppendRequest(protoRows);
                    return;
                }
                // Append failed for some unexpected reason. This "might be" fatal and the invoker
                // should intervene.
                logger.error(String.format("Subtask %d cannot append to BigQuery", subtaskId), e);
                throw new BigQueryConnectorException(
                        String.format("Subtask %d cannot append to BigQuery", subtaskId), e);
            } catch (InterruptedException e) {
                logger.error(
                        String.format(
                                "Error while retrieving AppendRowsResponse in subtask %s",
                                subtaskId),
                        e);
                throw new BigQueryConnectorException(
                        "Error getting response for BigQuery write API", e);
            }
            if (response.hasError()) {
                logger.error(
                    String.format(
                            "Request to AppendRows failed in subtask %d with error %s",
                            subtaskId, response.getError().getMessage()));
                throw new BigQueryConnectorException(
                        String.format(
                                "Error in subtask %d while writing to BigQuery: %s",
                                subtaskId, response.getError().getMessage()));
            }
            postAppendOps(future);
            return;
        }
        if (streamName == null || streamName.isEmpty()) {
            streamWriter =
                    createWriteStreamAndStreamWriter(
                            writeStreamCreationThrottler, WriteStream.Type.BUFFERED, false);
        }
        ApiFuture<AppendRowsResponse> future = streamWriter.append(protoRows, streamOffset);
        postAppendOps(future);
    }

    /** Throws a RuntimeException if an error is found with append response. */
    @Override
    void validateAppendResponse(
            ApiFuture<AppendRowsResponse> appendResponseFuture, long expectedOffset) {
        AppendRowsResponse response;
        try {
            response = appendResponseFuture.get();
        } catch (ExecutionException e) {
            if (e.getCause().getClass() == OffsetAlreadyExists.class) {
                logger.info("TODO");
                return;
            }
            logger.error("TODO", e);
            throw new BigQueryConnectorException("TODO", e);
        } catch (InterruptedException e) {
            logger.error(
                    String.format(
                            "Exception while retrieving AppendRowsResponse in subtask %s",
                            subtaskId),
                    e);
            throw new BigQueryConnectorException(
                    String.format(
                            "Error in subtask %d while getting response for BigQuery append API",
                            subtaskId),
                    e);
        }
        if (response.hasError()) {
            logger.error(
                    String.format(
                            "Request to AppendRows failed in subtask %d with error %s",
                            subtaskId, response.getError().getMessage()));
            throw new BigQueryConnectorException(
                    String.format(
                            "Error in subtask %d while writing to BigQuery: %s",
                            subtaskId, response.getError().getMessage()));
        }
        if (response.getAppendResult().getOffset().getValue() != expectedOffset) {
            logger.error("TODO");
            throw new BigQueryConnectorException("TODO");
        }
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("prepareCommit not implemented");
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) throws IOException {
        throw new UnsupportedOperationException("snapshotState not implemented");
    }

    private void discardStreamAndResendAppendRequest(ProtoRows protoRows) {
        streamName = "";
        sendAppendRequest(protoRows);
    }

    private void logStreamNotUsable(Exception e) {
        logger.info(
                String.format(
                        "Writer %d cannot use stream %s. Discarding this stream and creating new one.",
                        subtaskId, streamName),
                e);
    }

    private void postAppendOps(ApiFuture<AppendRowsResponse> future) {
        appendResponseFuturesQueue.add(Pair.of(future, streamOffset));
        streamAppendCount++;
        streamOffset += appendRequestRowCount;
        appendRequestRowCount = 0L;
    }
}

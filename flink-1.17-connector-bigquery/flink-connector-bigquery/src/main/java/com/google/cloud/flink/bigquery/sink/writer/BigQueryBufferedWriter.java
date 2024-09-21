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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.StringUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
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
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommitter;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.throttle.Throttler;
import com.google.cloud.flink.bigquery.sink.throttle.WriteStreamCreationThrottler;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: exactly-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled: no write consistency.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryBufferedWriter<IN> extends BaseWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    // Write stream creation must be throttled to ensure proper client usage.
    private final Throttler writeStreamCreationThrottler;

    // Write stream name stored in writer's state. In case of a new writer, this will be an empty
    // string until first checkpoint.
    private String streamNameInState;

    // Offset position where next append should occur in current stream.
    private long streamOffset;

    // Write stream offset stored in writer's state. In case of a new writer, this will be an 0
    // until first checkpoint.
    private long streamOffsetInState;

    // Number of rows appended by this writer to current stream.
    private long appendRequestRowCount;

    public BigQueryBufferedWriter(
            int subtaskId,
            String streamName,
            long streamOffset,
            String tablePath,
            long totalRecordsSeen,
            long totalRecordsWritten,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer,
            Sink.InitContext context) {
        super(subtaskId, tablePath, connectOptions, schemaProvider, serializer);
        this.streamNameInState = StringUtils.isNullOrWhitespaceOnly(streamName) ? "" : streamName;
        this.streamName = this.streamNameInState;
        this.streamOffsetInState = streamOffset;
        this.streamOffset = streamOffset;
        this.totalRecordsSeen = totalRecordsSeen;
        this.totalRecordsWritten = totalRecordsWritten;
        writeStreamCreationThrottler = new WriteStreamCreationThrottler(subtaskId);
        appendRequestRowCount = 0L;
        // Initialize the metric counters.
        successfullyAppendedRecordsCounter =
                context.metricGroup().counter("successfullyAppendedRecords");
        // Update the metrics to values saved at checkpoint.
        successfullyAppendedRecordsCounter.inc(totalRecordsWritten);
        context.metricGroup().getIOMetricGroup().getNumRecordsInCounter().inc(totalRecordsSeen);
        // ..SinceChkpt Counters restart at 0.
        numRecordsInSinceChkptCounter = context.metricGroup().counter("numRecordsInSinceChkpt");
        successfullyAppendedRecordsSinceChkptCounter =
                context.metricGroup().counter("successfullyAppendedRecordsSinceChkpt");
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
        numRecordsInSinceChkptCounter.inc();
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
        long rowCount = protoRows.getSerializedRowsCount();
        if (streamOffset == streamOffsetInState
                && streamName.equals(streamNameInState)
                && !StringUtils.isNullOrWhitespaceOnly(streamName)) {
            // Writer has an associated write stream and is invoking append for the first
            // time since re-initialization.
            performFirstAppendOnRestoredStream(protoRows, rowCount);
            return;
        }
        if (StringUtils.isNullOrWhitespaceOnly(streamName)) {
            // Throttle stream creation to ensure proper usage of BigQuery createWriteStream API.
            logger.info("Throttling creation of BigQuery write stream in subtask {}", subtaskId);
            writeStreamCreationThrottler.throttle();
            createWriteStream(WriteStream.Type.BUFFERED);
            createStreamWriter(false);
        }
        ApiFuture<AppendRowsResponse> future = streamWriter.append(protoRows, streamOffset);
        postAppendOps(future, rowCount);
    }

    /** Throws a RuntimeException if an error is found with append response. */
    @Override
    void validateAppendResponse(AppendInfo appendInfo) {
        ApiFuture<AppendRowsResponse> appendResponseFuture = appendInfo.getFuture();
        long expectedOffset = appendInfo.getExpectedOffset();
        long recordsAppended = appendInfo.getRecordsAppended();
        AppendRowsResponse response;
        try {
            response = appendResponseFuture.get();
            if (response.hasError()) {
                logAndThrowFatalException(response.getError().getMessage());
            }
            long offset = response.getAppendResult().getOffset().getValue();
            if (offset != expectedOffset) {
                logAndThrowFatalException(
                        String.format(
                                "Inconsistent offset in BigQuery API response. Found %d, expected %d",
                                offset, expectedOffset));
            }
            totalRecordsWritten += recordsAppended;
            successfullyAppendedRecordsCounter.inc(recordsAppended);
            successfullyAppendedRecordsSinceChkptCounter.inc(recordsAppended);
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause().getClass() == OffsetAlreadyExists.class) {
                logger.info(
                        "Ignoring OffsetAlreadyExists error in subtask {} as this can be due to faulty retries",
                        subtaskId);
                return;
            }
            logAndThrowFatalException(e);
        }
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        logger.info("Preparing commit in subtask {}", subtaskId);
        if (streamOffset == 0
                || streamNameInState.equals(streamName) && streamOffset == streamOffsetInState) {
            logger.info("No new data appended in subtask {}. Nothing to commit.", subtaskId);
            return Collections.EMPTY_LIST;
        }
        return Collections.singletonList(
                new BigQueryCommittable(subtaskId, streamName, streamOffset));
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) throws IOException {
        logger.info("Snapshotting state in subtask {} for checkpoint {}", subtaskId, checkpointId);
        streamNameInState = streamName;
        streamOffsetInState = streamOffset;
        return Collections.singletonList(
                // Note that it's possible to store the associated checkpointId in writer's state.
                // For now, we're not leveraging this due to absence of a use case.
                new BigQueryWriterState(
                        streamName, streamOffset, totalRecordsSeen, totalRecordsWritten));
    }

    @Override
    public void close() {
        if (!streamNameInState.equals(streamName) || streamOffsetInState != streamOffset) {
            // Either new stream was created which will not be stored in any state, or something was
            // appended to the existing stream which will not be committed. In both scenarios, the
            // stream is not usable and must be finalized, i.e. "closed".
            finalizeStream();
        }
        super.close();
    }

    private void performFirstAppendOnRestoredStream(ProtoRows protoRows, long rowCount) {
        try {
            // Connection pool (method parameter below) can be enabled only for default stream.
            createStreamWriter(false);
        } catch (BigQueryConnectorException e) {
            // If StreamWriter could not be created for this write stream, then discard it.
            discardStreamAndResendAppendRequest(e, protoRows);
            return;
        }
        ApiFuture<AppendRowsResponse> future = streamWriter.append(protoRows, streamOffset);
        AppendRowsResponse response;
        try {
            // Get this future immediately to check whether append worked or not, inferring stream
            // is usable or not.
            response = future.get();
            postAppendOps(ApiFutures.immediateFuture(response), rowCount);
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause().getClass() == OffsetAlreadyExists.class
                    || e.getCause().getClass() == OffsetOutOfRange.class
                    || e.getCause().getClass() == StreamFinalizedException.class
                    || e.getCause().getClass() == StreamNotFound.class) {
                discardStreamAndResendAppendRequest(e, protoRows);
                return;
            }
            // Append failed for some unexpected reason. This "might be" fatal and the job owner
            // should intervene.
            logAndThrowFatalException(e);
        }
    }

    private void discardStreamAndResendAppendRequest(Exception e, ProtoRows protoRows) {
        discardStream(e);
        sendAppendRequest(protoRows);
    }

    private void discardStream(Exception e) {
        logger.info(
                String.format(
                        "Writer %d cannot use stream %s. Discarding this stream.",
                        subtaskId, streamName),
                e);
        finalizeStream();
        // Empty streamName will prompt following sendAppendRequest invocation to create anew write
        // stream.
        streamName = "";
        // Also discard the offset.
        streamOffset = 0L;
    }

    private void finalizeStream() {
        logger.debug("Finalizing write stream {} in subtask {}", streamName, subtaskId);
        try {
            writeClient.finalizeWriteStream(streamName);
        } catch (Exception innerException) {
            // Do not fret!
            // This is not fatal.
            logger.debug(
                    String.format(
                            "Failed while finalizing write stream %s in subtask %d",
                            streamName, subtaskId),
                    innerException);
        }
    }

    private void postAppendOps(ApiFuture<AppendRowsResponse> future, long rowCount) {
        appendResponseFuturesQueue.add(new AppendInfo(future, streamOffset, rowCount));
        streamOffset += appendRequestRowCount;
        appendRequestRowCount = 0L;
    }

    /**
     * Following "getters" expose some internal fields required for testing.
     *
     * <p>In addition to keeping these methods package private, ensure that exposed field cannot be
     * changed in a way that alters the class instance's state.
     *
     * <p>Do NOT use these methods outside tests!
     */
    @Internal
    long getStreamOffset() {
        return streamOffset;
    }

    @Internal
    long getStreamOffsetInState() {
        return streamOffsetInState;
    }

    @Internal
    String getStreamNameInState() {
        return streamNameInState;
    }
}

/*
 * Copyright (C) 2023 Google Inc.
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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.ProtobufUtils;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** */
public class BigQueryBufferedWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryBufferedWriter.class);

    private final int writerId;
    private ProtoRows.Builder protoRowsBuilder;
    private long writeCount;
    private long streamOffset;
    private final StreamWriter streamWriter;

    public BigQueryBufferedWriter(
            int writerId,
            BigQueryConnectOptions connectOptions,
            String tablePath,
            ProtoSchema protoSchema) {
        int sleepSeconds = writerId % 10;
        try {
            Thread.sleep(sleepSeconds * 1000L);
        } catch (InterruptedException e) {
            LOG.error(Thread.currentThread().getId() + ": Writer sleep was interrupted!");
        }
        this.writerId = writerId;
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            Instant start = Instant.now();
            String writeStreamName =
                    writeClient
                            .createWriteStream(
                                    CreateWriteStreamRequest.newBuilder()
                                            .setParent(tablePath)
                                            .setWriteStream(
                                                    WriteStream.newBuilder()
                                                            .setType(WriteStream.Type.BUFFERED)
                                                            .build())
                                            .build())
                            .getName();
            long timeElapsed = Duration.between(start, Instant.now()).toMillis();
            LOG.info(
                    "Initiated stream creation for writer " + writerId + " at " + start.toString());
            LOG.info(
                    "Created write stream for writer "
                            + writerId
                            + " in "
                            + timeElapsed
                            + " milliseconds");
            this.streamWriter =
                    writeClient.createStreamWriter(
                            protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
        } catch (Exception e) {
            throw new RuntimeException(
                    Thread.currentThread().getId() + ": Failed to create writer", e);
        }
        protoRowsBuilder = ProtoRows.newBuilder();
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        TempUtils.globalPrepareCommitCounter++;
        LOG.info(
                writerId
                        + ": PrepareCommit["
                        + TempUtils.globalPrepareCommitCounter
                        + "] called for "
                        + streamWriter.getStreamName()
                        + " after "
                        + writeCount
                        + " writes");
        if (writeCount == 0) {
            LOG.info(
                    writerId
                            + ": PrepareCommit["
                            + TempUtils.globalPrepareCommitCounter
                            + "] for "
                            + streamWriter.getStreamName()
                            + " is returning no committable");
            return Collections.EMPTY_LIST;
        }
        //        if (TempUtils.globalPrepareCommitCounter == 1) {
        //            LOG.error(
        //                    Thread.currentThread().getId()
        //                            + ": Intentionally failing first prepareCommit for "
        //                            + streamWriter.getStreamName());
        //            throw new RuntimeException(
        //                    Thread.currentThread().getId()
        //                            + ": Intentional failure at prepareCommit for "
        //                            + streamWriter.getStreamName());
        //        }
        writeCount = 0;
        LOG.info(
                writerId
                        + ": Preparing to commit "
                        + streamWriter.getStreamName()
                        + " till offset "
                        + (streamOffset - 1));
        return Collections.singletonList(
                new BigQueryCommittable(streamOffset, streamWriter.getStreamName()));
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        try {
            assert element instanceof String;
            protoRowsBuilder.addSerializedRows(
                    ProtobufUtils.createMessage(element.toString()).toByteString());
            writeCount++;
            if (protoRowsBuilder.getSerializedRowsCount() == 1000) {
                appendRows();
            }
            //            TempUtils.globalWriteCounter++;
            //            if (writeCount == 5000) {
            //                LOG.error(
            //                        Thread.currentThread().getId()
            //                                + ": Intentionally failing write for "
            //                                + streamWriter.getStreamName());
            //                throw new RuntimeException(
            //                        Thread.currentThread().getId()
            //                                + ": Intentional failure at write for "
            //                                + streamWriter.getStreamName());
            //            }
        } catch (Exception e) {
            LOG.error(writerId + ": Error while appending to " + streamWriter.getStreamName());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        LOG.info("Calling flush for writer {}", writerId);
        if (protoRowsBuilder.getSerializedRowsCount() > 0) {
            appendRows();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info(writerId + ": writer closed");
        // TODO: finalize buffered stream!
        streamWriter.close();
        protoRowsBuilder.clear();
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) throws IOException {
        LOG.info(writerId + ": Call to snapshot state for checkpointId: " + checkpointId);
        return Collections.singletonList(new BigQueryWriterState());
    }

    private void appendRows() {
        LOG.info(
                writerId
                        + ": Calling append on "
                        + streamWriter.getStreamName()
                        + " after "
                        + writeCount
                        + " writes");
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response;
        try {
            response = streamWriter.append(rowsToAppend, streamOffset).get();
        } catch (Exception e) {
            LOG.error(
                    writerId
                            + ": Could not get append rows response for "
                            + streamWriter.getStreamName(),
                    e);
            throw new RuntimeException(e);
        }
        if (response.hasError()) {
            LOG.error(
                    writerId
                            + ": Could not append rows to "
                            + streamWriter.getStreamName()
                            + " : "
                            + response.getError().getCode()
                            + " : "
                            + response.getError().getMessage());
            throw new RuntimeException(response.getError().getMessage());
        } else {
            LOG.info(
                    writerId
                            + ": AppendRows rpc for "
                            + streamWriter.getStreamName()
                            + " returned offset "
                            + response.getAppendResult().getOffset().getValue());
            streamOffset += rowsToAppend.getSerializedRowsCount();
        }
        protoRowsBuilder = ProtoRows.newBuilder();
    }
}

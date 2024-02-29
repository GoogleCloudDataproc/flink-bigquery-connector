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

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.protobuf.Descriptors;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** */
public class BigQueryWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriter.class);

    private final boolean enableExactlyOnce;
    private ProtoRows.Builder protoRowsBuilder;
    private long writeCount;
    private long streamOffset;
    private final StreamWriter streamWriter;

    private final Descriptors.Descriptor descriptor;

    public BigQueryWriter(
            boolean enableExactlyOnce,
            StreamWriter streamWriter,
            Descriptors.Descriptor descriptor) {
        this.enableExactlyOnce = enableExactlyOnce;
        protoRowsBuilder = ProtoRows.newBuilder();
        this.streamWriter = streamWriter;
        this.descriptor = descriptor;
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        TempUtils.globalPrepareCommitCounter++;
        LOG.info(
                Thread.currentThread().getId()
                        + ": "
                        + "PrepareCommit["
                        + TempUtils.globalPrepareCommitCounter
                        + "] called for "
                        + streamWriter.getStreamName()
                        + " after "
                        + writeCount
                        + " writes");
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
        if (!enableExactlyOnce) {
            return Collections.EMPTY_LIST;
        } else {
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Preparing to commit "
                            + streamWriter.getStreamName()
                            + " till offset "
                            + (streamOffset - 1));
        }
        return Collections.singletonList(
                new BigQueryCommittable(streamOffset, streamWriter.getStreamName()));
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        try {
            LOG.info("@prashastia: Element schema description: " + element.getClass());
            LOG.info("@prashastia: Element schema description: " + element);
            protoRowsBuilder.addSerializedRows(
                    AvroToProtoSerializer.getDynamicMessageFromGenericRecord(
                                    (GenericRecord) element, this.descriptor)
                            .toByteString());
            writeCount++;
            if (protoRowsBuilder.getSerializedRowsCount() == 5) {
                appendRows();
            }
            TempUtils.globalWriteCounter++;
            if (TempUtils.globalWriteCounter == 5000) {
                LOG.error(
                        Thread.currentThread().getId()
                                + ": Intentionally failing write for "
                                + streamWriter.getStreamName());
                throw new RuntimeException(
                        Thread.currentThread().getId()
                                + ": Intentional failure at write for "
                                + streamWriter.getStreamName());
            }
        } catch (Exception e) {
            LOG.error(
                    Thread.currentThread().getId()
                            + ": Error while appending to "
                            + streamWriter.getStreamName());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (protoRowsBuilder.getSerializedRowsCount() > 0) {
            appendRows();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info(Thread.currentThread().getId() + ": writer closed");
        // TODO: finalize buffered stream!
        this.streamWriter.close();
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) throws IOException {
        LOG.info(
                Thread.currentThread().getId()
                        + ": Call to "
                        + "snapshot state for checkpointId: "
                        + checkpointId);
        return Collections.singletonList(new BigQueryWriterState());
    }

    private void appendRows() {
        LOG.info(
                Thread.currentThread().getId()
                        + ": Calling append on "
                        + streamWriter.getStreamName()
                        + " after "
                        + writeCount
                        + " writes");
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response;
        try {
            if (enableExactlyOnce) {
                response = streamWriter.append(rowsToAppend, streamOffset).get();
            } else {
                response = streamWriter.append(rowsToAppend).get();
            }
        } catch (Exception e) {
            LOG.error(
                    Thread.currentThread().getId()
                            + ": Could not get append rows response for "
                            + streamWriter.getStreamName(),
                    e);
            throw new RuntimeException(e);
        }
        if (response.hasError()) {
            LOG.error(
                    Thread.currentThread().getId()
                            + ": Could not append rows to "
                            + streamWriter.getStreamName()
                            + " : "
                            + response.getError().getCode()
                            + " : "
                            + response.getError().getMessage());
            throw new RuntimeException(response.getError().getMessage());
        } else {
            LOG.info(
                    Thread.currentThread().getId()
                            + ": AppendRows rpc for "
                            + streamWriter.getStreamName()
                            + " returned offset "
                            + response.getAppendResult().getOffset().getValue());
            streamOffset += rowsToAppend.getSerializedRowsCount();
        }
        protoRowsBuilder = ProtoRows.newBuilder();
    }
}

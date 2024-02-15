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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.ProtobufUtils;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittableSerializer;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommitter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterStateSerializer;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/** */
public class BigQuerySink<IN> implements TwoPhaseCommittingStatefulSink<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
    private final boolean enableExactlyOnce;
    private final BigQueryConnectOptions connectOptions;
    private final ProtoSchema protoSchema;
    private final String tablePath;
    private int writerCount = 0;

    public BigQuerySink(
            boolean enableExactlyOnce, BigQueryConnectOptions connectOptions, String tablePath) {
        this.connectOptions = connectOptions;
        this.enableExactlyOnce = enableExactlyOnce;
        this.protoSchema =
                ProtoSchema.newBuilder().setProtoDescriptor(ProtobufUtils.DESCRIPTOR_PROTO).build();
        this.tablePath = tablePath;
        LOG.info(Thread.currentThread().getId() + ": BigQuerySink has been initialized");
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(InitContext context) throws IOException {
        //        try {
        //            experiment();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: experiment failed!", e);
        //        }
        writerCount++;
        LOG.info(Thread.currentThread().getId() + ": Calling createWriter " + writerCount);
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            String writeStreamName =
                    enableExactlyOnce
                            ? writeClient
                                    .createWriteStream(
                                            CreateWriteStreamRequest.newBuilder()
                                                    .setParent(tablePath)
                                                    .setWriteStream(
                                                            WriteStream.newBuilder()
                                                                    .setType(
                                                                            WriteStream.Type
                                                                                    .BUFFERED)
                                                                    .build())
                                                    .build())
                                    .getName()
                            : String.format("%s/streams/_default", tablePath);
            StreamWriter streamWriter =
                    writeClient.createStreamWriter(
                            protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Created writer for stream "
                            + writeStreamName);
            return new BigQueryWriter(enableExactlyOnce, streamWriter);
        } catch (Exception e) {
            throw new RuntimeException(
                    Thread.currentThread().getId() + ": Failed to create writer", e);
        }
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            restoreWriter(InitContext context, Collection<BigQueryWriterState> recoveredStates)
                    throws IOException {
        LOG.info(Thread.currentThread().getId() + ": Calling restoreWriter");
        return createWriter(context);
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter() throws IOException {
        LOG.info(Thread.currentThread().getId() + ": Calling createCommitter");
        return new BigQueryCommitter(connectOptions, enableExactlyOnce);
    }

    @Override
    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        return new BigQueryCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigQueryWriterState> getWriterStateSerializer() {
        return new BigQueryWriterStateSerializer();
    }

    private void experiment() throws InterruptedException {
        BigQueryServices.StorageWriteClient writeClient;
        try {
            writeClient = BigQueryServicesFactory.instance(connectOptions).storageWrite();
        } catch (IOException e) {
            throw new RuntimeException("Could not create write client for experiment in sink");
        }
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

        //        try {
        //            streamWriter
        //                    .append(
        //                            ProtoRows.newBuilder()
        //                                    .addSerializedRows(ByteString.copyFromUtf8("FooBar"))
        //                                    .build(),
        //                            1)
        //                    .get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: dummy append at wrong offset (1) failed", e);
        //            Thread.sleep(5000L);
        //        }

        //        try {
        //            streamWriter.append(ProtoRows.newBuilder().build(), 0).get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: empty append failed", e);
        //        }
        //
        //        try {
        //            streamWriter.append(ProtoRows.newBuilder().build(), 1).get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: empty append at wrong offset failed", e);
        //        }

        try (StreamWriter streamWriter =
                writeClient.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName)) {
            streamWriter
                    .append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(
                                            ProtobufUtils.createMessage("Jayant").toByteString())
                                    .build(),
                            0)
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: proper append failed.. WTF!?", e);
            Thread.sleep(5000L);
            LOG.error("Returning ...");
            return;
        }

        try (StreamWriter streamWriter =
                writeClient.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName)) {
            streamWriter
                    .append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(ByteString.copyFromUtf8("FooBar"))
                                    .build(),
                            1)
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: dummy append failed", e);
            Thread.sleep(5000L);
        }

        try (StreamWriter streamWriter =
                writeClient.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName)) {
            streamWriter
                    .append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(ByteString.copyFromUtf8("FooBar"))
                                    .build(),
                            2)
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: dummy append at wrong offset (2) failed", e);
            Thread.sleep(5000L);
        }

        try {
            writeClient
                    .finalizeWriteStream(
                            FinalizeWriteStreamRequest.newBuilder()
                                    .setName(writeStreamName)
                                    .build())
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: finalize failed.. WTF!?", e);
            Thread.sleep(5000L);
            LOG.error("Returning ...");
            return;
        }

        try (StreamWriter streamWriter =
                writeClient.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName)) {
            streamWriter
                    .append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(ByteString.copyFromUtf8("FooBar"))
                                    .build(),
                            1)
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: dummy append on finalized stream failed", e);
            Thread.sleep(5000L);
        }

        try (StreamWriter streamWriter =
                writeClient.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName)) {
            streamWriter
                    .append(
                            ProtoRows.newBuilder()
                                    .addSerializedRows(ByteString.copyFromUtf8("FooBar"))
                                    .build(),
                            2)
                    .get();
        } catch (Exception e) {
            LOG.error("LOOK_ME_UP: dummy append on finalized stream at wrong offset failed", e);
            Thread.sleep(5000L);
        }

        //        try {
        //            streamWriter.append(ProtoRows.newBuilder().build(), 1).get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: empty append on finalized stream failed", e);
        //        }

        //        try {
        //            streamWriter.append(ProtoRows.newBuilder().build(), 2).get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: empty append on finalized stream at wrong offset
        // failed", e);
        //        }

        //        try {
        //            streamWriter
        //                    .append(
        //                            ProtoRows.newBuilder()
        //                                    .addSerializedRows(
        //
        // ProtobufUtils.createMessage("Jayant").toByteString())
        //                                    .build(),
        //                            1)
        //                    .get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: proper append on finalized stream failed", e);
        //        }

        //        try {
        //            writeClient
        //                    .flushRows(
        //                            FlushRowsRequest.newBuilder()
        //                                    .setOffset(Int64Value.of(0L))
        //                                    .setWriteStream(writeStreamName)
        //                                    .build())
        //                    .get();
        //        } catch (Exception e) {
        //            LOG.error("LOOK_ME_UP: flush on finalized stream failed.. weird", e);
        //        }
    }
}

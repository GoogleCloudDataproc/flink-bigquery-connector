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
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittableSerializer;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommitter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterStateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/** */
class ExactlyOnceSink<IN> extends AbstractSink<IN> implements TwoPhaseCommittingStatefulSink<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceSink.class);
    private int writerCount = 0;

    ExactlyOnceSink(BigQueryConnectOptions connectOptions, int maxParallelism) {
        super(connectOptions, maxParallelism);
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(InitContext context) throws IOException {
        writerCount++;
        LOG.info(Thread.currentThread().getId() + ": Calling createWriter " + writerCount);
        super.validateParallelism(context);
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
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
            StreamWriter streamWriter =
                    writeClient.createStreamWriter(
                            protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Created writer for stream "
                            + writeStreamName);
            return new BigQueryBufferedWriter(streamWriter);
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
        return new BigQueryCommitter(connectOptions);
    }

    @Override
    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        return new BigQueryCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigQueryWriterState> getWriterStateSerializer() {
        return new BigQueryWriterStateSerializer();
    }
}

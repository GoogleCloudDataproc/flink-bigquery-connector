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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.cloud.flink.bigquery.services.BigQueryServicesImpl;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittableSerializer;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommitter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterStateSerializer;

import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;

/**
 * Sink to write data into a BigQuery table using {@link BigQueryBufferedWriter}.
 *
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: exactly-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled (NOT RECOMMENDED!): no consistency guarantee.
 *
 * @param <IN> Type of input to sink.
 */
public class BigQueryExactlyOnceSink<IN> extends BigQueryBaseSink<IN>
        implements TwoPhaseCommittingStatefulSink<IN, BigQueryWriterState, BigQueryCommittable> {

    BigQueryExactlyOnceSink(BigQuerySinkConfig<IN> sinkConfig) {
        super(sinkConfig);
        traceId = BigQueryServicesImpl.generateTraceId(UUID.randomUUID().toString());
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(WriterInitContext context) {
        checkParallelism(context.getTaskInfo().getNumberOfParallelSubtasks());
        return new BigQueryBufferedWriter<>(
                tablePath,
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                fatalizeSerializer,
                maxParallelism,
                traceId,
                context);
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            restoreWriter(
                    WriterInitContext context, Collection<BigQueryWriterState> recoveredState) {
        if (recoveredState == null || recoveredState.isEmpty()) {
            return createWriter(context);
        }
        // If multiple states are found, restore one with the latest checkpoint.
        BigQueryWriterState stateToRestore =
                recoveredState.stream()
                        .max(Comparator.comparingLong(state -> state.getCheckpointId()))
                        .get();
        return new BigQueryBufferedWriter<>(
                stateToRestore.getStreamName(),
                stateToRestore.getStreamOffset(),
                tablePath,
                stateToRestore.getTotalRecordsSeen(),
                stateToRestore.getTotalRecordsWritten(),
                stateToRestore.getTotalRecordsCommitted(),
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                fatalizeSerializer,
                maxParallelism,
                traceId,
                context);
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter(CommitterInitContext context) {
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

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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;

import java.io.IOException;
import java.util.Collection;

/**
 * Sink to write data into a BigQuery table using {@link BigQueryBufferedWriter}.
 *
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: exactly-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled (NOT RECOMMENDED!): no consistency guarantee.
 *
 * @param <IN> Type of records written to BigQuery
 */
public class BigQueryExactlyOnceSink<IN> extends BigQueryBaseSink<IN>
        implements TwoPhaseCommittingStatefulSink<IN, BigQueryWriterState, BigQueryCommittable> {

    BigQueryExactlyOnceSink(BigQuerySinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(InitContext context) throws IOException {
        throw new UnsupportedOperationException("createWriter not implemented");
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            restoreWriter(InitContext context, Collection<BigQueryWriterState> recoveredState)
                    throws IOException {
        throw new UnsupportedOperationException("restoreWriter not implemented");
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter() throws IOException {
        throw new UnsupportedOperationException("createCommitter not implemented");
    }

    @Override
    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        throw new UnsupportedOperationException("getCommittableSerializer not implemented");
    }

    @Override
    public SimpleVersionedSerializer<BigQueryWriterState> getWriterStateSerializer() {
        throw new UnsupportedOperationException("getWriterStateSerializer not implemented");
    }
}

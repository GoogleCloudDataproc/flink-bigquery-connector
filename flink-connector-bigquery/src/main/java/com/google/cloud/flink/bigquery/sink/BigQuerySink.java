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

import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;

import java.io.IOException;
import java.util.Collection;

/**
 * Sink to write data from a Flink job into a BigQuery table.
 *
 * <p>The sink can be used with at-least-once and exactly-once consistency guarantees.
 * <li>Flink's checkpointing mode must be either {@link CheckpointingMode#AT_LEAST_ONCE} or {@link
 *     CheckpointingMode#EXACTLY_ONCE} for the sink to offer at-least-once guarantee.
 * <li>Flink's checkpointing mode must be {@link CheckpointingMode#EXACTLY_ONCE} for the sink to
 *     offer exactly-once guarantee.
 * <li>If Flink's checkpointing is disabled, then the sink must be configured in at-least-once mode,
 *     else no data will be written to the BigQuery table.
 *
 *     <p>Depending on the configured {@link DeliveryGuarantee}, sink will behave as follows:
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE}: The sink will write to destination BigQuery table as
 *     soon as data is received. When Flink restarts due to some error, data is replayed from
 *     previous checkpoint, leading to duplication of elements that were already seen by the sink.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: The sink will stage data in BigQuery write streams,
 *     and commit to destination BigQuery table on a checkpoint. Since data is committed only on
 *     checkpoints, no duplication will occur in case of a restart. However, this delays data
 *     availability in BigQuery table until a checkpoint is complete, so adjust checkpoint frequency
 *     accordingly.
 * <li>{@link DeliveryGuarantee#NONE}: Not supported.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQuerySink<IN>
        implements TwoPhaseCommittingStatefulSink<IN, BigQueryWriterState, BigQueryCommittable> {

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            createWriter(InitContext context) throws IOException {
        throw new UnsupportedOperationException("Method createWriter is not supported");
    }

    @Override
    public PrecommittingStatefulSinkWriter<IN, BigQueryWriterState, BigQueryCommittable>
            restoreWriter(InitContext context, Collection<BigQueryWriterState> recoveredStates)
                    throws IOException {
        throw new UnsupportedOperationException("Method restoreWriter is not supported");
    }

    @Override
    public Committer<BigQueryCommittable> createCommitter() throws IOException {
        throw new UnsupportedOperationException("Method createCommitter is not supported");
    }

    @Override
    public SimpleVersionedSerializer<BigQueryCommittable> getCommittableSerializer() {
        throw new UnsupportedOperationException("Method getCommittableSerializer is not supported");
    }

    @Override
    public SimpleVersionedSerializer<BigQueryWriterState> getWriterStateSerializer() {
        throw new UnsupportedOperationException("Method getWriterStateSerializer is not supported");
    }
}

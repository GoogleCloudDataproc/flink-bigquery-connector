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
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Writer implementation for {@link BigQueryBufferedSink}.
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
 * <li>{Checkpointing disabled}: no write consistency.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryBufferedWriter<IN> extends BaseWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    public BigQueryBufferedWriter(
            int subtaskId,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer) {
        super(subtaskId, connectOptions, schemaProvider, serializer);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("write not implemented");
    }

    @Override
    ApiFuture sendAppendRequest(ProtoRows protoRows) {
        throw new UnsupportedOperationException("sendAppendRequest not implemented");
    }

    @Override
    void validateAppendResponse(ApiFuture<AppendRowsResponse> appendResponseFuture) {
        throw new UnsupportedOperationException("validateAppendResponse not implemented");
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
}

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

import com.google.cloud.flink.bigquery.sink.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.TwoPhaseCommittingStatefulSink;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Writer implementation for {@link BigQuerySink}.
 *
 * <p>This class writes records to BigQuery storage write streams.
 *
 * <p>Depending on the sink's {@link DeliveryGuarantee}, the writer will behave as follows:
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE}: The writer will write to BigQuery table's default
 *     stream. Records will be available in the table immediately.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: The writer will write to BigQuery table's buffered
 *     stream. Records will be staged in the buffered stream until committed by a {@link
 *     BigQueryCommitter}.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, BigQueryWriterState, BigQueryCommittable> {

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Method write is not supported");
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Method flush is not supported");
    }

    @Override
    public Collection<BigQueryCommittable> prepareCommit()
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Method prepareCommit is not supported");
    }

    @Override
    public List<BigQueryWriterState> snapshotState(long checkpointId) throws IOException {
        throw new UnsupportedOperationException("Method snapshotState is not supported");
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Method close is not supported");
    }
}

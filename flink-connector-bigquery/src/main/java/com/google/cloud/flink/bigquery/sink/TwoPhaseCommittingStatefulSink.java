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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;

import java.io.IOException;
import java.util.Collection;

/** */
@Internal
public interface TwoPhaseCommittingStatefulSink<InputT>
        extends TwoPhaseCommittingSink<InputT, BigQueryCommittable>,
                StatefulSink<InputT, BigQueryWriterState> {

    @Override
    PrecommittingStatefulSinkWriter<InputT, BigQueryWriterState, BigQueryCommittable> createWriter(
            InitContext context) throws IOException;

    @Override
    PrecommittingStatefulSinkWriter<InputT, BigQueryWriterState, BigQueryCommittable> restoreWriter(
            InitContext context, Collection<BigQueryWriterState> recoveredState) throws IOException;

    /** A combination of {@link PrecommittingSinkWriter} and {@link StatefulSinkWriter}. */
    interface PrecommittingStatefulSinkWriter<InputT, BigQueryWriterState, BigQueryCommitable>
            extends PrecommittingSinkWriter<InputT, BigQueryCommitable>,
                    StatefulSinkWriter<InputT, BigQueryWriterState> {}
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import java.io.IOException;
import java.util.Collection;

/**
 * A combination of {@link TwoPhaseCommittingSink} and {@link StatefulSink}.
 *
 * <p>Interface for a sink that supports TPC protocol and statefulness.
 *
 * @param <IN> Type of the sink's input.
 * @param <WriterStateT> Type of the sink writer's state.
 * @param <CommittableT> Type of the committables.
 */
@Internal
public interface TwoPhaseCommittingStatefulSink<IN, WriterStateT, CommittableT>
        extends TwoPhaseCommittingSink<IN, CommittableT>, StatefulSink<IN, WriterStateT> {

    @Override
    PrecommittingStatefulSinkWriter<IN, WriterStateT, CommittableT> createWriter(
            Sink.InitContext context) throws IOException;

    @Override
    PrecommittingStatefulSinkWriter<IN, WriterStateT, CommittableT> restoreWriter(
            Sink.InitContext context, Collection<WriterStateT> recoveredState) throws IOException;

    /**
     * A combination of {@link PrecommittingSinkWriter} and {@link StatefulSinkWriter}.
     *
     * <p>Interface for a writer that supports TPC protocol and statefulness.
     *
     * @param <IN> Type of the sink's input.
     * @param <WriterStateT> Type of the sink writer's state.
     * @param <CommittableT> Type of the committables.
     */
    interface PrecommittingStatefulSinkWriter<IN, WriterStateT, CommittableT>
            extends TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, CommittableT>,
                    StatefulSink.StatefulSinkWriter<IN, WriterStateT> {}
}

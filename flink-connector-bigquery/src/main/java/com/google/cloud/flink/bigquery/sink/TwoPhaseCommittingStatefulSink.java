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
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import java.io.IOException;
import java.util.Collection;

/**
 * A combination of {@link TwoPhaseCommittingSink} and {@link StatefulSink}.
 *
 * <p>This interface outlines the topology of {@link BigQuerySink}.
 *
 * @param <InputT> Type of sink's input.
 * @param <WriterStateT> Type of sink writer's state.
 * @param <CommT> Type of committable.
 */
@Internal
public interface TwoPhaseCommittingStatefulSink<InputT, WriterStateT, CommT>
        extends TwoPhaseCommittingSink<InputT, CommT>, StatefulSink<InputT, WriterStateT> {

    @Override
    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> createWriter(InitContext context)
            throws IOException;

    @Override
    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> restoreWriter(
            InitContext context, Collection<WriterStateT> recoveredState) throws IOException;

    /**
     * A combination of {@link PrecommittingSinkWriter} and {@link StatefulSinkWriter}.
     *
     * @param <InputT> Type of sink's input.
     * @param <WriterStateT> Type of sink writer's state.
     * @param <CommT> Type of committable.
     */
    interface PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT>
            extends PrecommittingSinkWriter<InputT, CommT>,
                    StatefulSinkWriter<InputT, WriterStateT> {}
}

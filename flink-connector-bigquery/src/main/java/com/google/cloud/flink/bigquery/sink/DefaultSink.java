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

import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * Sink to write data into a BigQuery table using {@link DefaultWriter}.
 *
 * <p>Depending on the checkpointing mode, this sink offers either at-least-once or at-most-once
 * consistency guarantee.
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: at-least-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>{@link CheckpointingMode#NONE}: at-most-once write consistency.
 */
public class DefaultSink extends BaseSink {

    @Override
    public SinkWriter createWriter(InitContext context) {
        throw new UnsupportedOperationException("createWriter method is not supported");
    }
}

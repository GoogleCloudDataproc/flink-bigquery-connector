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

import com.google.cloud.flink.bigquery.sink.writer.BigQueryDefaultWriter;

/**
 * Sink to write data into a BigQuery table using {@link BigQueryDefaultWriter}.
 *
 * <p>Depending on the checkpointing mode, this sink offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: at-least-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>Checkpointing disabled (NOT RECOMMENDED!): no consistency guarantee.
 */
class BigQueryDefaultSink extends BigQueryBaseSink {

    BigQueryDefaultSink(BigQuerySinkConfig sinkConfig) {
        super(sinkConfig);
    }

    @Override
    public SinkWriter createWriter(InitContext context) {
        checkParallelism(context.getNumberOfParallelSubtasks());
        return new BigQueryDefaultWriter(
                tablePath,
                connectOptions,
                schemaProvider,
                serializer,
                createTableOptions(),
                traceId,
                context);
    }
}

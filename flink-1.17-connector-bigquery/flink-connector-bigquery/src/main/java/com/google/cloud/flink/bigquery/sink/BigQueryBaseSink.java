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

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for developing a BigQuery sink. */
abstract class BigQueryBaseSink implements Sink {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // BigQuery write streams can offer over 10 MBps throughput, and per project throughput quotas
    // are in the order of single digit GBps. With each sink writer maintaining a single and unique
    // write connection to BigQuery, maximum parallelism for sink is intentionally restricted to
    // 128 for initial releases of this connector. This is also the default max parallelism of
    // Flink applications.
    // Based on performance observations and user feedback, this number can be increased in the
    // future.
    public static final int MAX_SINK_PARALLELISM = 128;

    final BigQueryConnectOptions connectOptions;
    final BigQuerySchemaProvider schemaProvider;
    final BigQueryProtoSerializer serializer;
    final String tablePath;

    BigQueryBaseSink(BigQuerySinkConfig sinkConfig) {
        validateSinkConfig(sinkConfig);
        this.connectOptions = sinkConfig.getConnectOptions();
        this.schemaProvider = sinkConfig.getSchemaProvider();
        this.serializer = sinkConfig.getSerializer();
        this.tablePath =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
    }

    private void validateSinkConfig(BigQuerySinkConfig sinkConfig) {
        if (sinkConfig.getConnectOptions() == null) {
            throw new IllegalArgumentException("BigQuery connect options cannot be null");
        }
        if (sinkConfig.getSerializer() == null) {
            throw new IllegalArgumentException("BigQuery serializer cannot be null");
        }
        if (sinkConfig.getSchemaProvider() == null) {
            throw new IllegalArgumentException("BigQuery schema provider cannot be null");
        }
    }

    /** Ensures Sink's parallelism does not exceed the allowed maximum when scaling Flink job. */
    void checkParallelism(int numberOfParallelSubtasks) {
        if (numberOfParallelSubtasks > MAX_SINK_PARALLELISM) {
            logger.error(
                    "Maximum allowed parallelism for Sink is {}, but attempting to create Writer number {}",
                    MAX_SINK_PARALLELISM,
                    numberOfParallelSubtasks);
            throw new IllegalStateException("Attempting to create more Sink Writers than allowed");
        }
    }
}

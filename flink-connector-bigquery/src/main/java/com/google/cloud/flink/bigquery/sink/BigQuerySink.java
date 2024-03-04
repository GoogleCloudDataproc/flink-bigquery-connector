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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class wrapping BigQuery sinks with appropriate configurations. */
public class BigQuerySink {

    public static final int MAX_SINK_PARALLELISM = 200;
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    public static Sink get(BigQuerySinkConfig sinkConfig, StreamExecutionEnvironment env) {
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            LOG.error("Exactly once write consistency is not supported in BigQuery sink");
            throw new UnsupportedOperationException("Exactly once guarantee not supported");
        }
        validateSinkConfig(sinkConfig, env);
        return new BigQueryDefaultSink(sinkConfig);
    }

    private static void validateSinkConfig(
            BigQuerySinkConfig sinkConfig, StreamExecutionEnvironment env) {
        validateParallelism(sinkConfig, env);
    }

    private static void validateParallelism(
            BigQuerySinkConfig sinkConfig, StreamExecutionEnvironment env) {
        if (sinkConfig.getParallelism() < 1) {
            LOG.warn(
                    "Usable parallelism not found in BigQuery sink config. Inferring from "
                            + "execution environment.");
            sinkConfig.setParallelism(env.getParallelism());
        }
        if (sinkConfig.getParallelism() > MAX_SINK_PARALLELISM) {
            LOG.error(
                    "Sink's parallelism of {} exceeds the allowed maximum of {}",
                    sinkConfig.getParallelism(),
                    MAX_SINK_PARALLELISM);
            throw new IllegalArgumentException(
                    "Sink's parallelism is more than the allowed maximum");
        }
    }
}

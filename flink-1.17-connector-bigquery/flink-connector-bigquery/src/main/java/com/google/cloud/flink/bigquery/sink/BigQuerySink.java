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

/**
 * Class wrapping BigQuery sinks with appropriate configurations.
 *
 * <p>With {@link DeliveryGuarantee#AT_LEAST_ONCE}, the Sink added to Flink job will be {@link
 * BigQueryDefaultSink}.
 *
 * <p>Eventual data consistency at destination is also dependent on checkpointing mode. With {@link
 * CheckpointingMode#AT_LEAST_ONCE} or {@link CheckpointingMode#EXACTLY_ONCE}, the {@link
 * BigQueryDefaultSink} will offer at-least-once consistency. We recommend enabling checkpointing to
 * avoid any unexpected behavior.
 *
 * <p>Support for exactly-once consistency in BigQuerySink will be offered soon!
 */
public class BigQuerySink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    public static Sink get(BigQuerySinkConfig sinkConfig, StreamExecutionEnvironment env) {
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
            System.out.println(".get() called");
            return new BigQueryDefaultSink(sinkConfig);
        }
        LOG.error(
                "Only at-least-once write consistency is supported in BigQuery sink. Found {}",
                sinkConfig.getDeliveryGuarantee());
        throw new UnsupportedOperationException(
                String.format("%s is not supported", sinkConfig.getDeliveryGuarantee()));
    }
}

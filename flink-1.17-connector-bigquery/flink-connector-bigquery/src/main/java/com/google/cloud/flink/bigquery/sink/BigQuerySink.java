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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class wrapping BigQuery sinks with appropriate configurations.
 *
 * <p>With {@link DeliveryGuarantee#AT_LEAST_ONCE}, the Sink added to Flink job will be {@link
 * BigQueryDefaultSink}.
 *
 * <p>With {@link DeliveryGuarantee#EXACTLY_ONCE}, the Sink added to Flink job will be {@link
 * BigQueryExactlyOnceSink}.
 *
 * <p>Eventual data consistency at destination is also dependent on checkpointing mode. Look at
 * {@link BigQueryDefaultSink} and {@link BigQueryExactlyOnceSink} for write consistencies offered
 * across combinations of {@link CheckpointingMode} and sink's {@link DeliveryGuarantee}. It is
 * recommended that checkpointing is enabled to avoid unexpected behavior.
 */
public class BigQuerySink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    public static <IN> Sink<IN> get(BigQuerySinkConfig<IN> sinkConfig) {
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
            return new BigQueryDefaultSink<>(sinkConfig);
        }
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            return new BigQueryExactlyOnceSink<>(sinkConfig);
        }
        LOG.error(
                "BigQuery sink does not support {} delivery guarantee. Use AT_LEAST_ONCE or EXACTLY_ONCE.",
                sinkConfig.getDeliveryGuarantee());
        throw new UnsupportedOperationException(
                String.format("%s is not supported", sinkConfig.getDeliveryGuarantee()));
    }
}

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

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    public static Sink get(BigQuerySinkConfig sinkConfig, StreamExecutionEnvironment env) {
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            LOG.error("Exactly once write consistency is not supported in BigQuery sink");
            throw new UnsupportedOperationException("Exactly once guarantee not supported");
        }
        return new BigQueryDefaultSink(sinkConfig);
    }
}

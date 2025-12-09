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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.time.Duration;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.noRestartStrategyConfig;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQuerySink}. */
public class BigQuerySinkTest {

    private static Configuration invalidFixedDelayRestartStrategyConfig() {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 20);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
        return config;
    }

    @Test
    public void testGet_withAtLeastOnce() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            BigQuerySinkConfig<Object> sinkConfig =
                    BigQuerySinkConfig.<Object>newBuilder()
                            .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                            .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                            .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                            .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .streamExecutionEnvironment(env)
                            .build();
            assertTrue(BigQuerySink.get(sinkConfig) instanceof BigQueryDefaultSink);
        }
    }

    @Test
    public void testGet_withExactlyOnce() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            BigQuerySinkConfig<Object> sinkConfig =
                    BigQuerySinkConfig.<Object>newBuilder()
                            .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                            .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                            .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                            .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .streamExecutionEnvironment(env)
                            .build();
            assertTrue(BigQuerySink.get(sinkConfig) instanceof BigQueryExactlyOnceSink);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGet_withNoneDeliveryGuarantee() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(noRestartStrategyConfig())) {
            BigQuerySinkConfig<Object> sinkConfig =
                    BigQuerySinkConfig.<Object>newBuilder()
                            .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                            .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                            .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                            .deliveryGuarantee(DeliveryGuarantee.NONE)
                            .streamExecutionEnvironment(env)
                            .build();
            BigQuerySink.get(sinkConfig);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_withInvalidRestartStrategy() throws Exception {
        try (StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(invalidFixedDelayRestartStrategyConfig())) {
            BigQuerySinkConfig<Object> sinkConfig =
                    BigQuerySinkConfig.<Object>newBuilder()
                            .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                            .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                            .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                            .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .streamExecutionEnvironment(env)
                            .build();
            assertTrue(BigQuerySink.get(sinkConfig) instanceof BigQueryExactlyOnceSink);
        }
    }
}

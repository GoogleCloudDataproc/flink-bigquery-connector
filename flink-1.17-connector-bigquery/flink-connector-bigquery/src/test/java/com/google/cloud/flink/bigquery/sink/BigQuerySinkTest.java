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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQuerySink}. */
public class BigQuerySinkTest {

    private StreamExecutionEnvironment env;

    private static final RestartStrategyConfiguration NO_RESTART_STRATEGY =
            RestartStrategies.noRestart();
    private static final RestartStrategyConfiguration INVALID_FIXED_DELAY_RESTART_STRATEGY =
            RestartStrategies.fixedDelayRestart(20, Time.seconds(5));

    @Before
    public void setUp() {
        env = new StreamExecutionEnvironment();
    }

    @After
    public void tearDown() throws Exception {
        env.close();
    }

    @Test
    public void testGet_withAtLeastOnce() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
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

    @Test
    public void testGet_withExactlyOnce() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGet_withNoneDeliveryGuarantee() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
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

    @Test(expected = IllegalArgumentException.class)
    public void testGet_withInvalidRestartStrategy() {
        env.setRestartStrategy(INVALID_FIXED_DELAY_RESTART_STRATEGY);
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

    @Test
    public void testGet_withCdcEnabled_atLeastOnce() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .streamExecutionEnvironment(env)
                        .enableCdc(true)
                        .cdcSequenceField("timestamp")
                        .build();
        // CDC with AT_LEAST_ONCE should return BigQueryDefaultSink
        assertTrue(BigQuerySink.get(sinkConfig) instanceof BigQueryDefaultSink);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_withCdcEnabled_exactlyOnce_shouldFail() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .streamExecutionEnvironment(env)
                        .enableCdc(true)
                        .build();
        // CDC with EXACTLY_ONCE should throw IllegalArgumentException
        BigQuerySink.get(sinkConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGet_withCdcEnabled_noneDeliveryGuarantee_shouldFail() {
        env.setRestartStrategy(NO_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.NONE)
                        .streamExecutionEnvironment(env)
                        .enableCdc(true)
                        .build();
        // CDC with NONE delivery guarantee should throw IllegalArgumentException
        BigQuerySink.get(sinkConfig);
    }
}

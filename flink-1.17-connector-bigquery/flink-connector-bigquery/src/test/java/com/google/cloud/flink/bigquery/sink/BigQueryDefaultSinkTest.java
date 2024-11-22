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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryDefaultSink}. */
public class BigQueryDefaultSinkTest {

    private static final RestartStrategies.RestartStrategyConfiguration
            FIXED_DELAY_RESTART_STRATEGY = RestartStrategies.fixedDelayRestart(5, Time.seconds(3));

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = new StreamExecutionEnvironment();
    }

    @After
    public void tearDown() throws Exception {
        env.close();
    }

    @Test
    public void testConstructor() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        assertNotNull(new BigQueryDefaultSink(sinkConfig));
    }

    @Test
    public void testConstructor_withoutConnectOptions() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(null)
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> new BigQueryDefaultSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("connect options cannot be null");
    }

    @Test
    public void testConstructor_withoutSerializer() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(null)
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> new BigQueryDefaultSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("serializer cannot be null");
    }

    @Test
    public void testCreateWriter() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(50);
        Mockito.when(mockedContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSink defaultSink = new BigQueryDefaultSink(sinkConfig);
        assertNotNull(defaultSink.createWriter(mockedContext));
    }

    @Test
    public void testCreateWriter_withMoreWritersThanAllowed() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(129);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> new BigQueryDefaultSink(sinkConfig).createWriter(mockedContext));
        assertThat(exception)
                .hasMessageThat()
                .contains("Attempting to create more Sink Writers than allowed");
    }
}

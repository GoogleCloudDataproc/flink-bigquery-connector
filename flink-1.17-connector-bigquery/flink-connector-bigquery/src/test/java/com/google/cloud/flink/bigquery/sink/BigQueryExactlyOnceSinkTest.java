/*
 * Copyright 2024 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryBufferedWriter;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriterState;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryExactlyOnceSink}. */
public class BigQueryExactlyOnceSinkTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = new StreamExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(3)));
    }

    @After
    public void tearDown() throws Exception {
        env.close();
    }

    @Test
    public void testConstructor() {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        assertNotNull(new BigQueryExactlyOnceSink(sinkConfig));
    }

    @Test
    public void testConstructor_withoutConnectOptions() {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(null)
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQueryExactlyOnceSink(sinkConfig));
        assertThat(exception)
                .hasMessageThat()
                .contains("connect options in sink config cannot be null");
    }

    @Test
    public void testCreateWriter() {
        Sink.InitContext mockedContext = Mockito.mock(Sink.InitContext.class);
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
        BigQueryExactlyOnceSink exactlyOnceSink = new BigQueryExactlyOnceSink(sinkConfig);
        assertNotNull(exactlyOnceSink.createWriter(mockedContext));
    }

    @Test
    public void testCreate_withMoreWritersThanAllowed() {
        Sink.InitContext mockedContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(129);
        Mockito.when(mockedContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
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
                        () -> new BigQueryExactlyOnceSink(sinkConfig).createWriter(mockedContext));
        assertThat(exception)
                .hasMessageThat()
                .contains("Attempting to create more Sink Writers than allowed");
    }

    @Test
    public void testRestoreWriter() {
        Sink.InitContext mockedContext = Mockito.mock(Sink.InitContext.class);
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
        BigQueryExactlyOnceSink exactlyOnceSink = new BigQueryExactlyOnceSink(sinkConfig);
        BigQueryBufferedWriter restoredWriter =
                (BigQueryBufferedWriter)
                        exactlyOnceSink.restoreWriter(
                                mockedContext,
                                Collections.singletonList(
                                        new BigQueryWriterState(
                                                "some_stream", 100L, 210L, 200L, 100L, 3L)));
        BigQueryWriterState state = (BigQueryWriterState) restoredWriter.snapshotState(4).get(0);
        assertEquals("some_stream", state.getStreamName());
        assertEquals(100, state.getStreamOffset());
        assertEquals(210, state.getTotalRecordsSeen());
        assertEquals(200, state.getTotalRecordsWritten());
        assertEquals(100, state.getTotalRecordsCommitted());
    }

    @Test
    public void testCreateCommitter() {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSink exactlyOnceSink = new BigQueryExactlyOnceSink(sinkConfig);
        assertNotNull(exactlyOnceSink.createCommitter());
    }

    @Test
    public void testGetCommittableSerializer() {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSink exactlyOnceSink = new BigQueryExactlyOnceSink(sinkConfig);
        assertNotNull(exactlyOnceSink.getCommittableSerializer());
    }

    @Test
    public void testGetWriterStateSerializer() {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryExactlyOnceSink exactlyOnceSink = new BigQueryExactlyOnceSink(sinkConfig);
        assertNotNull(exactlyOnceSink.getWriterStateSerializer());
    }
}

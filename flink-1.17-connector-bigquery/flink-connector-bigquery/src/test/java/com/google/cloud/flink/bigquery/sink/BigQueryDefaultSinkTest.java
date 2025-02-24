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

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSink<Object> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("projects/project/datasets/dataset/tables/table", sink.tablePath);
        assertNotNull(sink.serializer);
        assertNotNull(sink.schemaProvider.getAvroSchema());
        assertFalse(sink.schemaProvider.schemaUnknown());
        assertFalse(sink.enableTableCreation);
        assertFalse(sink.fatalizeSerializer);
    }

    @Test
    public void testConstructor_withoutConnectOptions() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(null)
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQueryDefaultSink<>(sinkConfig));
        assertThat(exception)
                .hasMessageThat()
                .contains("connect options in sink config cannot be null");
    }

    @Test
    public void testConstructor_withoutSerializer() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(null)
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQueryDefaultSink<>(sinkConfig));
        assertThat(exception).hasMessageThat().contains("serializer in sink config cannot be null");
    }

    @Test
    public void testConstructor_withCreateTable_withoutExistingTable() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(false))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .enableTableCreation(true)
                        .build();
        BigQueryDefaultSink<Object> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertNull(sink.schemaProvider.getAvroSchema());
        assertTrue(sink.schemaProvider.schemaUnknown());
        assertTrue(sink.enableTableCreation);
    }

    @Test
    public void testConstructor_withCreateTable_withExistingTable() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .enableTableCreation(true)
                        .build();
        BigQueryDefaultSink<Object> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertNotNull(sink.schemaProvider.getAvroSchema());
        assertFalse(sink.schemaProvider.schemaUnknown());
        assertTrue(sink.enableTableCreation);
    }

    @Test
    public void testConstructor_withoutCreateTable_withoutExistingTable() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(false))
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class, () -> new BigQueryDefaultSink<>(sinkConfig));
        assertThat(exception)
                .hasMessageThat()
                .contains("table does not exist and table creation is not enabled");
    }

    @Test
    public void testCreateWriter() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(50);
        Mockito.when(mockedContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSink defaultSink = new BigQueryDefaultSink<>(sinkConfig);
        assertNotNull(defaultSink.createWriter(mockedContext));
    }

    @Test
    public void testCreateWriter_withFatalSerializer() throws IOException, InterruptedException {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(50);
        Mockito.when(mockedContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(FakeBigQuerySerializer.getErringSerializer())
                        .streamExecutionEnvironment(env)
                        .fatalizeSerializer(true)
                        .build();
        BigQueryDefaultSink<Object> defaultSink = new BigQueryDefaultSink<>(sinkConfig);
        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> defaultSink.createWriter(mockedContext).write(null, null));
        assertThat(exception).hasMessageThat().contains("Unable to serialize record");
    }

    @Test
    public void testCreateWriter_withMoreWritersThanAllowed() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(129);
        BigQuerySinkConfig<Object> sinkConfig =
                BigQuerySinkConfig.<Object>newBuilder()
                        .connectOptions(getConnectOptions(true))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .streamExecutionEnvironment(env)
                        .build();
        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> new BigQueryDefaultSink<>(sinkConfig).createWriter(mockedContext));
        assertThat(exception)
                .hasMessageThat()
                .contains("Attempting to create more Sink Writers than allowed");
    }

    @Test
    public void testRegionAndParallelism_datasetExists_providedRegionOverridden() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(
                                        true, "us-central1"))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us")
                        .build();
        BigQueryDefaultSink<GenericRecord> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("us-central1", sink.region);
        assertEquals(128, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_datasetExistsMultiRegion_providedRegionOverridden() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForQuery(true, "us"))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us-central1")
                        .build();
        BigQueryDefaultSink<GenericRecord> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("us", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useProvidedRegion() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("us-central1")
                        .build();
        BigQueryDefaultSink<GenericRecord> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("us-central1", sink.region);
        assertEquals(128, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useProvidedMultiRegion() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .region("US")
                        .build();
        BigQueryDefaultSink<GenericRecord> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("US", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    @Test
    public void testRegionAndParallelism_useDefaultRegion() {
        env.setRestartStrategy(FIXED_DELAY_RESTART_STRATEGY);
        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(
                                StorageClientFaker.createConnectOptionsForQuery(false, null))
                        .serializer(new AvroToProtoSerializer())
                        .enableTableCreation(true)
                        .streamExecutionEnvironment(env)
                        .build();
        BigQueryDefaultSink<GenericRecord> sink = new BigQueryDefaultSink<>(sinkConfig);
        assertEquals("us", sink.region);
        assertEquals(512, sink.maxParallelism);
    }

    private static BigQueryConnectOptions getConnectOptions(boolean tableExists) {
        return StorageClientFaker.createConnectOptions(
                null,
                new StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient(null),
                new StorageClientFaker.FakeBigQueryServices.FakeQueryDataClient(
                        tableExists, null, null, null));
    }
}

/*
 * Copyright (C) 2023 Google Inc.
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
import org.apache.flink.api.connector.sink2.Sink.InitContext;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryDefaultSink}. */
public class BigQueryDefaultSinkTest {

    @Test
    public void testConstructor() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .build();
        assertNotNull(new BigQueryDefaultSink(sinkConfig));
    }

    @Test
    public void testConstructorWithoutConnectOptions() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(null)
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> new BigQueryDefaultSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("connect options cannot be null");
    }

    @Test
    public void testConstructorWithoutSerializer() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(null)
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> new BigQueryDefaultSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("serializer cannot be null");
    }

    @Test
    public void testConstructorWithoutSchemaProvider() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(null)
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .build();
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> new BigQueryDefaultSink(sinkConfig));
        assertThat(exception).hasMessageThat().contains("schema provider cannot be null");
    }

    @Test
    public void testCreateWriter() throws IOException {
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(50);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .build();
        Sink defaultSink = new BigQueryDefaultSink(sinkConfig);
        assertNotNull(defaultSink.createWriter(mockedContext));
    }

    @Test
    public void testCreateMoreWritersThanAllowed() throws IOException {
        InitContext mockedContext = Mockito.mock(InitContext.class);
        Mockito.when(mockedContext.getSubtaskId()).thenReturn(1);
        Mockito.when(mockedContext.getNumberOfParallelSubtasks()).thenReturn(129);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
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

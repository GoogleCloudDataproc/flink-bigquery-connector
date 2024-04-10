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

package com.google.cloud.flink.bigquery.sink.writer;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySerializer;
import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQueryDefaultWriter}. */
public class BigQueryDefaultWriterTest {

    @Test
    public void testConstructor() throws IOException {
        BigQueryDefaultWriter defaultWriter = createDefaultWriter(null, null);
        assertNotNull(defaultWriter);
        assertNull(defaultWriter.streamWriter);
        assertEquals(
                "/projects/project/datasets/dataset/tables/table/streams/_default",
                defaultWriter.streamName);
        assertEquals(0, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
        assertTrue(defaultWriter.protoRowsBuilder.getSerializedRowsList().isEmpty());
        assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
    }

    @Test
    public void testWriteWithoutAppend() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new TestBigQuerySerializer(ByteString.copyFromUtf8("foo")), null);
        // ByteString for "foo" will be 3 bytes in size, and serialization overhead of 2 will be
        // added.
        defaultWriter.write(new Object(), null);
        assertEquals(5, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
        assertEquals(
                ByteString.copyFromUtf8("foo"),
                defaultWriter.protoRowsBuilder.getSerializedRowsList().get(0));
        assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
    }

    @Test
    public void testAppend() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            AppendRowsResponse.newBuilder().build());
            // First element will be added to append request.
            defaultWriter.write(new Object(), null);
            assertEquals(8, defaultWriter.getAppendRequestSizeBytes());
            assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
            // Invoke append and verify request reset.
            defaultWriter.append();
            assertEquals(0, defaultWriter.getAppendRequestSizeBytes());
            assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertEquals(1, defaultWriter.appendResponseFuturesQueue.size());
        }
    }

    @Test
    public void testWriteWithAppend() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            AppendRowsResponse.newBuilder().build());
            defaultWriter.write(new Object(), null);
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
            // Second element will exceed append request's size, so append will be called with
            // first element in request.
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.appendResponseFuturesQueue.size());
        }
    }

    @Test
    public void testWriteWithResponseValidation() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            AppendRowsResponse.newBuilder().build());
            ApiFuture responseFuture1 =
                    ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build());
            ApiFuture responseFuture2 =
                    ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build());
            defaultWriter.appendResponseFuturesQueue.addAll(
                    Arrays.asList(responseFuture1, responseFuture2));
            assertEquals(2, defaultWriter.appendResponseFuturesQueue.size());
            defaultWriter.write(new Object(), null);
            // Second write will invoke append with first element in request, and Writer will
            // validate existing respone futures before sending new append request.
            // Intented behavior here is validation (and ejection from queue) of responseFuture1
            // and responseFuture2.
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.appendResponseFuturesQueue.size());
            ApiFuture responseFuture3 = (ApiFuture) defaultWriter.appendResponseFuturesQueue.peek();
            assertNotEquals(responseFuture1, responseFuture3);
            assertNotEquals(responseFuture2, responseFuture3);
        }
    }

    @Test
    public void testFlush() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            AppendRowsResponse.newBuilder().build());
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertEquals(1, defaultWriter.appendResponseFuturesQueue.size());
            // Flush will send append request for pending records, and validate all pending append
            // responses.
            defaultWriter.flush(false);
            assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
        }
    }

    @Test
    public void testClose() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            AppendRowsResponse.newBuilder().build());
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
            assertNull(defaultWriter.streamWriter);
            defaultWriter.write(new Object(), null);
            assertEquals(1, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertEquals(1, defaultWriter.appendResponseFuturesQueue.size());
            assertFalse(defaultWriter.streamWriter.isUserClosed());
            defaultWriter.close();
            assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
            assertTrue(defaultWriter.streamWriter.isUserClosed());
        }
    }

    @Test
    public void testWriteWithSerializationException() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(TestBigQuerySerializer.getErringSerializer(), null);
        assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
        // If write experiences a serialization exception, then the element is ignored and no
        // action is taken.
        defaultWriter.write(new Object(), null);
        assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
        assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
    }

    @Test(expected = BigQuerySerializationException.class)
    public void testMaxAppendRequestSizeViolation()
            throws IOException, BigQuerySerializationException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")),
                            AppendRowsResponse.newBuilder().build());
            defaultWriter.getProtoRow(new Object());
        }
    }

    @Test
    public void testWriteWithLargeElement() throws IOException {
        try (MockedStatic<StreamWriter> streamWriterStaticMock =
                Mockito.mockStatic(StreamWriter.class)) {
            streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
            BigQueryDefaultWriter defaultWriter =
                    createDefaultWriter(
                            new TestBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")),
                            AppendRowsResponse.newBuilder().build());
            assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            // This will add 14 bytes to append request but maximum request size is 5, leading to
            // the element being ignored.
            defaultWriter.write(new Object(), null);
            assertEquals(0, defaultWriter.protoRowsBuilder.getSerializedRowsCount());
            assertTrue(defaultWriter.appendResponseFuturesQueue.isEmpty());
        }
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testResponseValidationError() throws IOException {
        BigQueryDefaultWriter defaultWriter = createDefaultWriter(null, null);
        defaultWriter.appendResponseFuturesQueue.add(
                ApiFutures.immediateFuture(
                        AppendRowsResponse.newBuilder()
                                .setError(Status.newBuilder().setCode(4).build())
                                .build()));
        defaultWriter.validateAppendResponses(false);
    }

    private BigQueryDefaultWriter createDefaultWriter(
            BigQueryProtoSerializer mockSerializer, AppendRowsResponse appendResponse)
            throws IOException {
        return new BigQueryDefaultWriter(
                0,
                StorageClientFaker.createConnectOptionsForWrite(appendResponse),
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                "/projects/project/datasets/dataset/tables/table");
    }
}

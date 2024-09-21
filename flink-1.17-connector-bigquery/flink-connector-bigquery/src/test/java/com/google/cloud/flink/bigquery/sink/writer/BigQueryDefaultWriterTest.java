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

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQueryDefaultWriter}. */
public class BigQueryDefaultWriterTest {

    MockedStatic<StreamWriter> streamWriterStaticMock;

    @Before
    public void setUp() {
        streamWriterStaticMock = Mockito.mockStatic(StreamWriter.class);
        streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
    }

    @After
    public void tearDown() throws Exception {
        streamWriterStaticMock.close();
    }

    @Test
    public void testConstructor() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getEmptySerializer(), null);
        assertNotNull(defaultWriter);
        assertNull(defaultWriter.streamWriter);
        assertEquals(
                "/projects/project/datasets/dataset/tables/table/streams/_default",
                defaultWriter.streamName);
        assertEquals(0, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(0, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getProtoRows().getSerializedRowsList().isEmpty());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test
    public void testWrite_withoutAppend() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")), null);
        // ByteString for "foo" will be 3 bytes in size, and serialization overhead of 2 will be
        // added.
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(5, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(
                ByteString.copyFromUtf8("foo"),
                defaultWriter.getProtoRows().getSerializedRowsList().get(0));
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test
    public void testAppend() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        // First element will be added to append request.
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(8, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Invoke append and verify request reset.
        defaultWriter.append();
        assertEquals(1, defaultWriter.totalRecordsSeen);
        // The totalRecordsWritten attribute is incremented after response validation.
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(0, defaultWriter.getAppendRequestSizeBytes());
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
    }

    @Test
    public void testWrite_withAppend() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Second element will exceed append request's size, so append will be called with
        // first element in request.
        defaultWriter.write(new Object(), null);
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        // Third element will again exceed append request's size, so append will be called with
        // second element in request. Response future from first AppendRows request will be
        // validated, incrementing totalRecordsWritten.
        defaultWriter.write(new Object(), null);
        assertEquals(3, defaultWriter.totalRecordsSeen);
        assertEquals(1, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
    }

    @Test
    public void testFlush() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        defaultWriter.write(new Object(), null);
        // AppendRows invoked, response future stored.
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        // Flush will send append request for pending records, and validate all pending append
        // responses.
        defaultWriter.flush(false);
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(2, defaultWriter.totalRecordsWritten);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test
    public void testClose() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        assertNull(defaultWriter.streamWriter);
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        assertFalse(defaultWriter.streamWriter.isUserClosed());
        defaultWriter.close();
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        assertTrue(defaultWriter.streamWriter.isUserClosed());
    }

    @Test
    public void testWrite_withSerializationException() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getErringSerializer(), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        // If write experiences a serialization exception, then the element is ignored and no
        // action is taken.
        defaultWriter.write(new Object(), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test(expected = BigQuerySerializationException.class)
    public void testGetProtoRow_withMaxAppendRequestSizeViolation()
            throws IOException, BigQuerySerializationException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")), null);
        // The serializer.serialize method will return ByteString with 14 bytes, exceeding the
        // maximum request size, causing getProtoRow method to throw
        // BigQuerySerializationException.
        defaultWriter.getProtoRow(new Object());
    }

    @Test
    public void testWrite_withLargeElement() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        // This will add 14 bytes to append request, which exceeds the maximum request size,
        // leading to the element being ignored.
        defaultWriter.write(new Object(), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withResponseError() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getEmptySerializer(), null);
        defaultWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFuture(
                                AppendRowsResponse.newBuilder()
                                        .setError(Status.newBuilder().setCode(4).build())
                                        .build()),
                        -1L,
                        10L));
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withExecutionException() throws IOException {
        BigQueryDefaultWriter defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getEmptySerializer(), null);
        defaultWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFailedFuture(new RuntimeException("foo")), -1L, 10L));
    }

    private BigQueryDefaultWriter createDefaultWriter(
            BigQueryProtoSerializer mockSerializer, AppendRowsResponse appendResponse)
            throws IOException {
        Sink.InitContext mockInitContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(mockInitContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        return new BigQueryDefaultWriter(
                0,
                "/projects/project/datasets/dataset/tables/table",
                StorageClientFaker.createConnectOptionsForWrite(appendResponse),
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                mockInitContext);
    }
}

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
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.cloud.flink.bigquery.sink.serializer.TestSchemaProvider;
import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BigQueryDefaultWriter}. */
public class BigQueryDefaultWriterTest {

    MockedStatic<StreamWriter> streamWriterStaticMock;
    BigQueryConnectOptions connectOptions;
    Sink.InitContext mockInitContext;

    @Before
    public void setUp() {
        streamWriterStaticMock = Mockito.mockStatic(StreamWriter.class);
        streamWriterStaticMock.when(StreamWriter::getApiMaxRequestBytes).thenReturn(10L);
        connectOptions = null;
        mockInitContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(mockInitContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        Mockito.when(mockInitContext.getSubtaskId()).thenReturn(0);
    }

    @After
    public void tearDown() throws Exception {
        streamWriterStaticMock.close();
        streamWriterStaticMock = null;
        connectOptions = null;
        mockInitContext = null;
    }

    @Test
    public void testConstructor() {
        BigQueryDefaultWriter<Object> defaultWriter =
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
        assertFalse(defaultWriter.fatalizeSerializer);
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
    }

    @Test
    public void testWrite_withoutAppend() {
        BigQueryDefaultWriter<Object> defaultWriter =
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
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        getTestQueryClient();
    }

    @Test
    public void testAppend() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        TestBigQuerySchemas.getSimpleRecordSchema(),
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build(),
                        null,
                        true);
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
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        // Ensure table creation is not attempted, since table exists in this test's setup.
        FakeBigQueryServices.FakeQueryDataClient testQueryClient = getTestQueryClient();
        assertEquals(1, testQueryClient.getTableExistsInvocatioks());
        assertEquals(0, testQueryClient.getCreateDatasetInvocatioks());
        assertEquals(0, testQueryClient.getCreateTableInvocatioks());
    }

    @Test
    public void testCreateTable() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new TestSchemaProvider(null, null),
                        new FakeBigQuerySerializer(
                                ByteString.copyFromUtf8("foobar"),
                                StorageClientFaker.SIMPLE_AVRO_SCHEMA),
                        AppendRowsResponse.newBuilder().build(),
                        new CreateTableOptions(true, null, null, null, null, null),
                        false);
        // First element will be added to append request.
        defaultWriter.write(new Object(), null);
        // Invoke append.
        defaultWriter.append();
        // Ensure table creation is attempted.
        FakeBigQueryServices.FakeQueryDataClient testQueryClient = getTestQueryClient();
        assertEquals(2, testQueryClient.getTableExistsInvocatioks());
        assertEquals(1, testQueryClient.getCreateDatasetInvocatioks());
        assertEquals(1, testQueryClient.getCreateTableInvocatioks());
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateTable_withTableCreationDisabled() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new TestSchemaProvider(null, null),
                        new FakeBigQuerySerializer(
                                ByteString.copyFromUtf8("foobar"),
                                StorageClientFaker.SIMPLE_AVRO_SCHEMA),
                        AppendRowsResponse.newBuilder().build(),
                        new CreateTableOptions(false, null, null, null, null, null),
                        false);
        // First element will be added to append request.
        defaultWriter.write(new Object(), null);
        // Invoke append.
        defaultWriter.append();
    }

    @Test
    public void testWrite_withAppend() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        // Second element will exceed append request's size, so append will be called with
        // first element in request.
        defaultWriter.write(new Object(), null);
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        // Third element will again exceed append request's size, so append will be called with
        // second element in request. Response future from first AppendRows request will be
        // validated, incrementing totalRecordsWritten.
        defaultWriter.write(new Object(), null);
        assertEquals(3, defaultWriter.totalRecordsSeen);
        assertEquals(1, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        // Test for metric values.
        assertEquals(1, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(3, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(3, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
    }

    @Test
    public void testFlush() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        defaultWriter.write(new Object(), null);
        // AppendRows invoked, response future stored.
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(0, defaultWriter.totalRecordsWritten);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        // Flush will send append request for pending records, and validate all pending append
        // responses.
        defaultWriter.flush(false);
        assertEquals(2, defaultWriter.totalRecordsSeen);
        assertEquals(2, defaultWriter.totalRecordsWritten);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        assertEquals(2, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
    }

    @Test
    public void testClose() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        AppendRowsResponse.newBuilder().build());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        assertNull(defaultWriter.streamWriter);
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        defaultWriter.write(new Object(), null);
        assertEquals(1, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, defaultWriter.getAppendResponseFuturesQueue().size());
        assertFalse(defaultWriter.streamWriter.isUserClosed());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(2, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
        defaultWriter.close();
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        assertTrue(defaultWriter.streamWriter.isUserClosed());
    }

    @Test
    public void testWrite_withSerializationException_withoutFatalSerializer() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getErringSerializer(), null);
        assertFalse(defaultWriter.fatalizeSerializer);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        // If write experiences a serialization exception, then the element is ignored and no
        // action is taken.
        defaultWriter.write(new Object(), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testWrite_withSerializationException_withFatalSerializer() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getErringSerializer(), true);
        assertTrue(defaultWriter.fatalizeSerializer);
        defaultWriter.write(new Object(), null);
    }

    @Test(expected = BigQuerySerializationException.class)
    public void testGetProtoRow_withMaxAppendRequestSizeViolation()
            throws BigQuerySerializationException {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")), null);
        // The serializer.serialize method will return ByteString with 14 bytes, exceeding the
        // maximum request size, causing getProtoRow method to throw
        // BigQuerySerializationException.
        defaultWriter.getProtoRow(new Object());
    }

    @Test
    public void testWrite_withLargeElement() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        // This will add 14 bytes to append request, which exceeds the maximum request size,
        // leading to the element being ignored.
        defaultWriter.write(new Object(), null);
        assertEquals(0, defaultWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(defaultWriter.getAppendResponseFuturesQueue().isEmpty());
        // Test for metric values.
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuery.getCount());
        assertEquals(0, defaultWriter.numberOfRecordsWrittenToBigQuerySinceCheckpoint.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriter.getCount());
        assertEquals(1, defaultWriter.numberOfRecordsSeenByWriterSinceCheckpoint.getCount());
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withResponseError() {
        BigQueryDefaultWriter<Object> defaultWriter =
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
    public void testValidateAppendResponse_withExecutionException() {
        BigQueryDefaultWriter<Object> defaultWriter =
                createDefaultWriter(FakeBigQuerySerializer.getEmptySerializer(), null);
        defaultWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFailedFuture(new RuntimeException("foo")), -1L, 10L));
    }

    private BigQueryDefaultWriter<Object> createDefaultWriter(
            BigQueryProtoSerializer<Object> mockSerializer, AppendRowsResponse appendResponse) {
        return createDefaultWriter(
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                appendResponse,
                null,
                true);
    }

    private BigQueryDefaultWriter<Object> createDefaultWriter(
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer<Object> mockSerializer,
            AppendRowsResponse appendResponse,
            CreateTableOptions createTableOptions,
            boolean tableExists) {
        FakeBigQueryServices.FakeBigQueryStorageWriteClient writeClient =
                new FakeBigQueryServices.FakeBigQueryStorageWriteClient(appendResponse);
        FakeBigQueryServices.FakeQueryDataClient queryClient =
                new FakeBigQueryServices.FakeQueryDataClient(tableExists, null, null, null);
        connectOptions = StorageClientFaker.createConnectOptions(null, writeClient, queryClient);
        return new BigQueryDefaultWriter<>(
                "/projects/project/datasets/dataset/tables/table",
                connectOptions,
                schemaProvider,
                mockSerializer,
                createTableOptions,
                false,
                128,
                "traceId",
                false,
                null,
                null,
                mockInitContext);
    }

    private BigQueryDefaultWriter<Object> createDefaultWriter(
            BigQueryProtoSerializer<Object> mockSerializer, boolean fatalizeSerializer) {
        return new BigQueryDefaultWriter<>(
                "/projects/project/datasets/dataset/tables/table",
                Mockito.mock(BigQueryConnectOptions.class),
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                null,
                fatalizeSerializer,
                128,
                "traceId",
                false,
                null,
                null,
                mockInitContext);
    }

    private FakeBigQueryServices.FakeQueryDataClient getTestQueryClient() {
        // FakeBigQueryServices (used for testing) creates a single instance of FakeQueryDataClient,
        // and returns it every time createQueryDataClient is called.
        return (FakeBigQueryServices.FakeQueryDataClient)
                ((FakeBigQueryServices) connectOptions.getTestingBigQueryServices().get())
                        .createQueryDataClient(null);
    }
}

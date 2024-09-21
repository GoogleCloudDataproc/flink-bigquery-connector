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

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse.AppendResult;
import com.google.cloud.bigquery.storage.v1.Exceptions.OffsetAlreadyExists;
import com.google.cloud.bigquery.storage.v1.Exceptions.OffsetOutOfRange;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.Exceptions.StreamFinalizedException;
import com.google.cloud.bigquery.storage.v1.Exceptions.StreamNotFound;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryCommittable;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.rpc.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** Tests for {@link BigQueryBufferedWriter}. */
public class BigQueryBufferedWriterTest {

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
    public void testConstructor_withNewWriter() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(null, 0L, 0L, 0L, FakeBigQuerySerializer.getEmptySerializer());
        assertNotNull(bufferedWriter);
        checkStreamlessWriterAttributes(bufferedWriter);
        assertEquals(0, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(0, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getProtoRows().getSerializedRowsList().isEmpty());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test
    public void testConstructor_withRestoredWriter() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        "foo", 100L, 210L, 200L, FakeBigQuerySerializer.getEmptySerializer());
        assertNotNull(bufferedWriter);
        assertNull(bufferedWriter.streamWriter);
        assertEquals("foo", bufferedWriter.streamName);
        assertEquals(100, bufferedWriter.getStreamOffset());
        assertEquals(100, bufferedWriter.getStreamOffsetInState());
        assertEquals(210, bufferedWriter.totalRecordsSeen);
        assertEquals(200, bufferedWriter.totalRecordsWritten);
        assertEquals(0, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getProtoRows().getSerializedRowsList().isEmpty());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test
    public void testWrite_withoutAppend() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("hi")));
        // ByteString for "hi" will be 2 bytes in size, and serialization overhead of 2 will be
        // added.
        bufferedWriter.write(new Object(), null);
        checkStreamlessWriterAttributes(bufferedWriter);
        assertEquals(1, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(4, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

        bufferedWriter.write(new Object(), null);
        checkStreamlessWriterAttributes(bufferedWriter);
        assertEquals(2, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(2, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

        ProtoRows protoRows = bufferedWriter.getProtoRows();
        assertEquals(ByteString.copyFromUtf8("hi"), protoRows.getSerializedRowsList().get(0));
        assertEquals(ByteString.copyFromUtf8("hi"), protoRows.getSerializedRowsList().get(1));
    }

    @Test
    public void testWrite_withAppend_withNewStream() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            // First append at offset 0.
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                            // Second append at offset 1. This second offset wont be actually tested
                            // here. Being pedantic to help the reader understand.
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(1))
                                                            .build())
                                            .build()),
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        checkStreamlessWriterAttributes(bufferedWriter);
        assertEquals(1, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

        // Second element will exceed append request's size, so append will be called with
        // first element in request.
        // Observe the first append behavior when writer does not have an existing stream.
        bufferedWriter.write(new Object(), null);
        assertEquals(
                1,
                ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                        .getCreateWriteStreamInvocations());
        assertNotNull(bufferedWriter.streamWriter);
        assertEquals("new_stream", bufferedWriter.streamName);
        assertEquals(1, bufferedWriter.getStreamOffset());
        assertEquals(2, bufferedWriter.totalRecordsSeen);
        // The totalRecordsWritten attribute is incremented after response validation.
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        // Second element was added to new request.
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

        // Third write, second append, first response validation.
        bufferedWriter.write(new Object(), null);
        assertEquals(2, bufferedWriter.getStreamOffset());
        assertEquals(3, bufferedWriter.totalRecordsSeen);
        // Upon successful response validation, totalRecordsWritten is incremented.
        assertEquals(1, bufferedWriter.totalRecordsWritten);
        // One future was added by latest append, and one was removed for validation.
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

        ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                .verifytAppendWithOffsetInvocations(2);

        // Ensure new write stream was not created again.
        assertEquals(
                1,
                ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                        .getCreateWriteStreamInvocations());
    }

    @Test
    public void testWrite_withAppend_withUsableRestoredStream() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        "restored_stream",
                        100L,
                        210L,
                        200L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(100))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(101))
                                                            .build())
                                            .build())
                        },
                        null,
                        null);
        bufferedWriter.write(new Object(), null);
        assertNull(bufferedWriter.streamWriter);
        assertEquals("restored_stream", bufferedWriter.streamName);
        assertEquals(100, bufferedWriter.getStreamOffset());
        assertEquals(211, bufferedWriter.totalRecordsSeen);
        assertEquals(200, bufferedWriter.totalRecordsWritten);
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

        // Second element will exceed append request's size, so append will be called with
        // first element in request.
        // Observe the first append behavior when writer has an existing stream.
        bufferedWriter.write(new Object(), null);
        assertNotNull(bufferedWriter.streamWriter);
        assertEquals(101, bufferedWriter.getStreamOffset());
        assertEquals(212, bufferedWriter.totalRecordsSeen);
        assertEquals(200, bufferedWriter.totalRecordsWritten);
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

        // Third write, second append, first response validation.
        bufferedWriter.write(new Object(), null);
        assertEquals(102, bufferedWriter.getStreamOffset());
        assertEquals(213, bufferedWriter.totalRecordsSeen);
        assertEquals(201, bufferedWriter.totalRecordsWritten);
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

        ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                .verifytAppendWithOffsetInvocations(2);

        // Existing stream was used, so finalize should not be called.
        assertEquals(
                0,
                ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                        .getFinalizeWriteStreamInvocations());
        // New stream was not created.
        assertEquals(
                0,
                ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                        .getCreateWriteStreamInvocations());
    }

    @Test
    public void testFirstAppend_withUnusableRestoredStream() throws IOException {
        // This is a rare test where parameterization is needed. However, we are not using standard
        // JUnit parameterization techniques to avoid importing a new dependency. Motivation is to
        // keep the connector artifact as small as possible.
        for (StorageException storageException :
                Arrays.asList(
                        Mockito.mock(OffsetAlreadyExists.class),
                        Mockito.mock(OffsetOutOfRange.class),
                        Mockito.mock(StreamFinalizedException.class),
                        Mockito.mock(StreamNotFound.class))) {
            BigQueryBufferedWriter bufferedWriter =
                    createBufferedWriter(
                            "restored_stream",
                            100L,
                            210L,
                            200L,
                            new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                            new ApiFuture[] {
                                ApiFutures.immediateFailedFuture(storageException),
                                ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                                ApiFutures.immediateFuture(
                                        AppendRowsResponse.newBuilder()
                                                .setAppendResult(
                                                        AppendResult.newBuilder()
                                                                .setOffset(Int64Value.of(1))
                                                                .build()))
                            },
                            WriteStream.newBuilder().setName("new_stream").build(),
                            FinalizeWriteStreamResponse.getDefaultInstance());
            bufferedWriter.write(new Object(), null);
            assertEquals("restored_stream", bufferedWriter.streamName);
            assertEquals(100, bufferedWriter.getStreamOffset());
            assertEquals(211, bufferedWriter.totalRecordsSeen);
            assertEquals(200, bufferedWriter.totalRecordsWritten);
            assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
            assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
            assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

            bufferedWriter.write(new Object(), null);
            // Existing stream was finalized.
            assertEquals(
                    1,
                    ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                            .getFinalizeWriteStreamInvocations());
            // New stream was created.
            assertEquals(
                    1,
                    ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                            .getCreateWriteStreamInvocations());
            assertEquals("new_stream", bufferedWriter.streamName);
            assertEquals(1, bufferedWriter.getStreamOffset());
            assertEquals(212, bufferedWriter.totalRecordsSeen);
            assertEquals(200, bufferedWriter.totalRecordsWritten);
            assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
            assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
            assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

            bufferedWriter.write(new Object(), null);
            assertEquals(2, bufferedWriter.getStreamOffset());
            assertEquals(213, bufferedWriter.totalRecordsSeen);
            assertEquals(201, bufferedWriter.totalRecordsWritten);
            assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());

            // First invocation on existing stream, which failed.
            // Second invocation on appending same request to new stream.
            // Third invocation for appending second request on new stream.
            ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                    .verifytAppendWithOffsetInvocations(3);

            // Ensure finalize or new stream creation were not invoked again.
            assertEquals(
                    1,
                    ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                            .getFinalizeWriteStreamInvocations());
            assertEquals(
                    1,
                    ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                            .getCreateWriteStreamInvocations());
        }
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testFirstAppend_withUnusableRestoredStream_withUnexpectedError()
            throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        "restored_stream",
                        100L,
                        210L,
                        200L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {ApiFutures.immediateFailedFuture(new RuntimeException())},
                        WriteStream.newBuilder().setName("new_stream").build(),
                        FinalizeWriteStreamResponse.getDefaultInstance());
        bufferedWriter.write(new Object(), null);
        assertNull(bufferedWriter.streamWriter);
        assertEquals("restored_stream", bufferedWriter.streamName);
        assertEquals(100, bufferedWriter.getStreamOffset());
        assertEquals(211, bufferedWriter.totalRecordsSeen);
        assertEquals(200, bufferedWriter.totalRecordsWritten);
        assertEquals(8, bufferedWriter.getAppendRequestSizeBytes());
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
        bufferedWriter.write(new Object(), null);
    }

    @Test
    public void testValidateAppendResponse_withOffsetAlreadyExists() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null, 0L, 10L, 0L, FakeBigQuerySerializer.getEmptySerializer());
        bufferedWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFailedFuture(mock(OffsetAlreadyExists.class)), 0L, 0L));
        // OffsetAlreadyExists is ignored and validation ends successfully.
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withResponseError() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null, 0L, 10L, 0L, FakeBigQuerySerializer.getEmptySerializer());
        bufferedWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFuture(
                                AppendRowsResponse.newBuilder()
                                        .setError(Status.newBuilder().setCode(4).build())
                                        .build()),
                        0L,
                        10L));
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withOffsetMismatch() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null, 0L, 10L, 0L, FakeBigQuerySerializer.getEmptySerializer());
        bufferedWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFuture(
                                AppendRowsResponse.newBuilder()
                                        .setAppendResult(
                                                AppendResult.newBuilder()
                                                        .setOffset(Int64Value.of(10))
                                                        .build())
                                        .build()),
                        0L,
                        10L));
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testValidateAppendResponse_withUnexpectedError() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null, 0L, 10L, 0L, FakeBigQuerySerializer.getEmptySerializer());
        bufferedWriter.validateAppendResponse(
                new BigQueryDefaultWriter.AppendInfo(
                        ApiFutures.immediateFailedFuture(mock(OffsetOutOfRange.class)), 0L, 0L));
    }

    @Test
    public void testFlush() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(1))
                                                            .build())
                                            .build()),
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        assertEquals(1, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
        bufferedWriter.write(new Object(), null);
        // AppendRows invoked, response future stored.
        assertEquals(2, bufferedWriter.totalRecordsSeen);
        assertEquals(0, bufferedWriter.totalRecordsWritten);
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());
        // Flush will send append request for pending records, and validate all pending append
        // responses.
        bufferedWriter.flush(false);
        assertEquals(2, bufferedWriter.totalRecordsSeen);
        assertEquals(2, bufferedWriter.totalRecordsWritten);
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());

        ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                .verifytAppendWithOffsetInvocations(2);
    }

    @Test
    public void testPrepareCommit_withAppends() throws IOException, InterruptedException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(1))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(2))
                                                            .build())
                                            .build()),
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.flush(false);
        Collection<BigQueryCommittable> committables = bufferedWriter.prepareCommit();
        assertEquals(1, committables.size());
        BigQueryCommittable committable = (BigQueryCommittable) committables.toArray()[0];
        assertEquals(1, committable.getProducerId());
        assertEquals("new_stream", committable.getStreamName());
        assertEquals(3, committable.getStreamOffset());
    }

    @Test
    public void testPrepareCommit_withoutAppends() throws IOException, InterruptedException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {},
                        null,
                        null);
        // No writes.
        bufferedWriter.flush(false);
        Collection<BigQueryCommittable> committables = bufferedWriter.prepareCommit();
        assertTrue(committables.isEmpty());
    }

    @Test
    public void testSnapshotState_withNewWriter() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(1))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(2))
                                                            .build())
                                            .build())
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.flush(false);
        assertEquals("", bufferedWriter.getStreamNameInState());
        assertEquals(0, bufferedWriter.getStreamOffsetInState());
        Collection<BigQueryWriterState> writerStates = bufferedWriter.snapshotState(1);
        BigQueryWriterState writerState = (BigQueryWriterState) writerStates.toArray()[0];
        assertEquals(1, writerStates.size());
        assertEquals("new_stream", writerState.getStreamName());
        assertEquals(3, writerState.getTotalRecordsSeen());
        assertEquals(3, writerState.getTotalRecordsWritten());
        assertEquals("new_stream", bufferedWriter.getStreamNameInState());
        assertEquals(3, bufferedWriter.getStreamOffsetInState());
    }

    @Test
    public void testSnapshotState_withRestoredWriter_withUsableStream() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        "restored_stream",
                        100L,
                        210L,
                        200L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(100))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(101))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(102))
                                                            .build())
                                            .build())
                        },
                        null,
                        null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.flush(false);
        assertEquals("restored_stream", bufferedWriter.getStreamNameInState());
        assertEquals(100, bufferedWriter.getStreamOffsetInState());
        Collection<BigQueryWriterState> writerStates = bufferedWriter.snapshotState(1);
        BigQueryWriterState writerState = (BigQueryWriterState) writerStates.toArray()[0];
        assertEquals(1, writerStates.size());
        assertEquals("restored_stream", writerState.getStreamName());
        assertEquals(213, writerState.getTotalRecordsSeen());
        assertEquals(203, writerState.getTotalRecordsWritten());
        assertEquals("restored_stream", bufferedWriter.getStreamNameInState());
        assertEquals(103, bufferedWriter.getStreamOffsetInState());
    }

    @Test
    public void testSnapshotState_withRestoredWriter_withUnusableStream() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        "restored_stream",
                        100L,
                        210L,
                        200L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFailedFuture(mock(StreamFinalizedException.class)),
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(1))
                                                            .build())
                                            .build()),
                            ApiFutures.immediateFuture(
                                    AppendRowsResponse.newBuilder()
                                            .setAppendResult(
                                                    AppendResult.newBuilder()
                                                            .setOffset(Int64Value.of(2))
                                                            .build())
                                            .build())
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.write(new Object(), null);
        bufferedWriter.flush(false);
        assertEquals("restored_stream", bufferedWriter.getStreamNameInState());
        assertEquals(100, bufferedWriter.getStreamOffsetInState());
        Collection<BigQueryWriterState> writerStates = bufferedWriter.snapshotState(1);
        BigQueryWriterState writerState = (BigQueryWriterState) writerStates.toArray()[0];
        assertEquals(1, writerStates.size());
        assertEquals("new_stream", writerState.getStreamName());
        assertEquals(213, writerState.getTotalRecordsSeen());
        assertEquals(203, writerState.getTotalRecordsWritten());
        assertEquals("new_stream", bufferedWriter.getStreamNameInState());
        assertEquals(3, bufferedWriter.getStreamOffsetInState());
    }

    @Test
    public void testClose_withStreamFinalize() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {
                            ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().build())
                        },
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        assertNull(bufferedWriter.streamWriter);
        bufferedWriter.write(new Object(), null);
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertEquals(1, bufferedWriter.getAppendResponseFuturesQueue().size());
        assertFalse(bufferedWriter.streamWriter.isUserClosed());
        bufferedWriter.close();
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
        assertTrue(bufferedWriter.streamWriter.isUserClosed());
        assertEquals(
                1,
                ((FakeBigQueryStorageWriteClient) bufferedWriter.writeClient)
                        .getFinalizeWriteStreamInvocations());
    }

    @Test
    public void testClose_withoutStreamFinalize() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        0L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobar")),
                        new ApiFuture[] {},
                        WriteStream.newBuilder().setName("new_stream").build(),
                        null);
        bufferedWriter.write(new Object(), null);
        assertEquals(1, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
        assertNull(bufferedWriter.streamWriter);
        bufferedWriter.close();
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
        assertNull(bufferedWriter.streamWriter);
        assertNull(bufferedWriter.writeClient);
    }

    @Test
    public void testWrite_withSerializationException() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null, 0L, 10L, 0L, FakeBigQuerySerializer.getErringSerializer());
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        // If write experiences a serialization exception, then the element is ignored and no
        // action is taken.
        bufferedWriter.write(new Object(), null);
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    @Test(expected = BigQuerySerializationException.class)
    public void testGetProtoRow_withMaxAppendRequestSizeViolation()
            throws IOException, BigQuerySerializationException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        10L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")));
        // The serializer.serialize method will return ByteString with 14 bytes, exceeding the
        // maximum request size, causing getProtoRow method to throw
        // BigQuerySerializationException.
        bufferedWriter.getProtoRow(new Object());
    }

    @Test
    public void testWrite_withLargeElement() throws IOException {
        BigQueryBufferedWriter bufferedWriter =
                createBufferedWriter(
                        null,
                        0L,
                        10L,
                        0L,
                        new FakeBigQuerySerializer(ByteString.copyFromUtf8("foobarbazqux")));
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        // This will add 14 bytes to append request, which exceeds the maximum request size,
        // leading to the element being ignored.
        bufferedWriter.write(new Object(), null);
        assertEquals(0, bufferedWriter.getProtoRows().getSerializedRowsCount());
        assertTrue(bufferedWriter.getAppendResponseFuturesQueue().isEmpty());
    }

    private BigQueryBufferedWriter createBufferedWriter(
            String streamName,
            long streamOffset,
            long totalRecordsSeen,
            long totalRecordsWritten,
            BigQueryProtoSerializer mockSerializer)
            throws IOException {
        Sink.InitContext mockSinkContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(mockSinkContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        return new BigQueryBufferedWriter(
                1,
                streamName,
                streamOffset,
                "/projects/project/datasets/dataset/tables/table",
                totalRecordsSeen,
                totalRecordsWritten,
                StorageClientFaker.createConnectOptionsForWrite(null),
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                mockSinkContext);
    }

    private BigQueryBufferedWriter createBufferedWriter(
            String streamName,
            long streamOffset,
            long totalRecordsSeen,
            long totalRecordsWritten,
            BigQueryProtoSerializer mockSerializer,
            ApiFuture[] appendResponseFutures,
            WriteStream writeStream,
            FinalizeWriteStreamResponse finalizeResponse)
            throws IOException {
        Sink.InitContext mockSinkContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(mockSinkContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSinkWriterMetricGroup());
        return new BigQueryBufferedWriter(
                1,
                streamName,
                streamOffset,
                "/projects/project/datasets/dataset/tables/table",
                totalRecordsSeen,
                totalRecordsWritten,
                StorageClientFaker.createConnectOptionsForWrite(
                        appendResponseFutures, writeStream, null, finalizeResponse),
                TestBigQuerySchemas.getSimpleRecordSchema(),
                mockSerializer,
                mockSinkContext);
    }

    private void checkStreamlessWriterAttributes(BigQueryBufferedWriter bufferedWriter) {
        assertNull(bufferedWriter.streamWriter);
        assertEquals("", bufferedWriter.streamName);
        assertEquals("", bufferedWriter.getStreamNameInState());
        assertEquals(0, bufferedWriter.getStreamOffset());
        assertEquals(0, bufferedWriter.getStreamOffsetInState());
    }
}

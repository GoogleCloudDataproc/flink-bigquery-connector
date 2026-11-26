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

package com.google.cloud.flink.bigquery.source.split.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceSplitReaderTest {

    @Test
    public void testFetchFailsOnAnErrorThatARetryCannotFix() throws Exception {
        BigQuerySourceSplitReader reader =
                readerWithFailingStream(
                        new IOException(
                                "could not open the stream",
                                StorageClientFaker.apiException(
                                        StatusCode.Code.PERMISSION_DENIED, "denied")));
        List<Long> delays = recordBackOffDelays(reader);
        try {
            IOException error = assertThrows(IOException.class, reader::fetch);
            assertThat(error).hasMessageThat().contains("retry cannot fix");
            assertThat(delays).isEmpty();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testFetchFailsOnAnErrorWithoutAStatusCode() throws Exception {
        BigQuerySourceSplitReader reader =
                readerWithFailingStream(
                        new IOException(
                                "could not decode",
                                new IllegalArgumentException("Avro schema not initialized")));
        List<Long> delays = recordBackOffDelays(reader);
        try {
            IOException error = assertThrows(IOException.class, reader::fetch);
            assertThat(error).hasMessageThat().contains("retry cannot fix");
            assertThat(delays).isEmpty();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testFetchRetriesATransientErrorWithGrowingDelaysThenFails() throws Exception {
        BigQuerySourceSplitReader reader =
                readerWithFailingStream(
                        StorageClientFaker.apiException(
                                StatusCode.Code.UNAVAILABLE, "connection reset"));
        List<Long> delays = recordBackOffDelays(reader);
        try {
            for (int attempt = 1;
                    attempt < BigQuerySourceSplitReader.MAX_CONSECUTIVE_FETCH_FAILURES;
                    attempt++) {
                RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
                assertThat(batch.nextSplit()).isNull();
                assertThat(batch.finishedSplits()).isEmpty();
            }
            IOException error = assertThrows(IOException.class, reader::fetch);
            assertThat(error).hasMessageThat().contains("10 times in a row");
            assertThat(delays)
                    .containsExactly(
                            1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L, 30_000L,
                            30_000L)
                    .inOrder();
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSuccessfulFetchResetsTheRetryDelay() throws Exception {
        BigQuerySourceSplitReader reader =
                Mockito.spy(
                        new BigQuerySourceSplitReader(
                                StorageClientFaker.createReadOptions(
                                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING),
                                new BigQuerySourceReaderContext(
                                        Mockito.mock(SourceReaderContext.class), -1)));
        ApiException transientError =
                StorageClientFaker.apiException(StatusCode.Code.UNAVAILABLE, "connection reset");
        Mockito.doThrow(transientError)
                .doCallRealMethod()
                .doThrow(transientError)
                .doCallRealMethod()
                .when(reader)
                .retrieveReadStream(Mockito.any());
        List<Long> delays = recordBackOffDelays(reader);
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(
                                new BigQuerySourceSplit("stream1", 0L),
                                new BigQuerySourceSplit("stream2", 0L))));
        try {
            List<String> finished = new ArrayList<>();
            for (int fetch = 0; fetch < 4; fetch++) {
                finished.addAll(reader.fetch().finishedSplits());
            }
            assertThat(finished).containsExactly("stream1", "stream2").inOrder();
            assertThat(delays).containsExactly(1_000L, 1_000L).inOrder();
        } finally {
            reader.close();
        }
    }

    private static BigQuerySourceSplitReader readerWithFailingStream(Exception error)
            throws IOException {
        BigQuerySourceSplitReader reader =
                Mockito.spy(
                        new BigQuerySourceSplitReader(
                                StorageClientFaker.createReadOptions(
                                        10, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING),
                                new BigQuerySourceReaderContext(
                                        Mockito.mock(SourceReaderContext.class), -1)));
        Mockito.doThrow(error).when(reader).retrieveReadStream(Mockito.any());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(new BigQuerySourceSplit("stream", 0L))));
        return reader;
    }

    private static List<Long> recordBackOffDelays(BigQuerySourceSplitReader reader)
            throws InterruptedException {
        List<Long> delays = new ArrayList<>();
        Mockito.doAnswer(
                        invocation -> {
                            delays.add(invocation.getArgument(0));
                            return null;
                        })
                .when(reader)
                .backOff(Mockito.anyLong());
        return delays;
    }

    @Test
    public void testSplitReaderSmall() throws IOException {
        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, 10);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        BigQuerySourceSplit split2 = new BigQuerySourceSplit("stream2", 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Arrays.asList(split, split2));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // there is one finished split and is named stream1
        assertThat(records.finishedSplits()).hasSize(1);

        String firstSplit = records.nextSplit();
        assertThat(firstSplit).isNotNull();
        assertThat(firstSplit).isEqualTo(splitName);

        int i = 0;
        while (records.nextRecordFromSplit() != null) {
            i++;
        }
        // there were 10 generic records read
        assertThat(i).isEqualTo(10);
        // there are no more splits
        assertThat(records.nextSplit()).isNull();

        // now there should be another split to process
        records = reader.fetch();
        assertThat(records.finishedSplits()).isNotEmpty();

        // after processing no more splits can be retrieved
        records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
    }

    @Test
    public void testSplitReaderMultipleFetch() throws IOException {
        Integer totalRecordCount = 15000;
        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        totalRecordCount, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        // no limits in the read
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, -1);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0L);
        SplitsAddition<BigQuerySourceSplit> change = new SplitsAddition<>(Arrays.asList(split));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // there shouldn't be a finished split
        assertThat(records.finishedSplits()).isEmpty();

        String firstPartialSplit = records.nextSplit();
        assertThat(firstPartialSplit).isNotNull();
        assertThat(firstPartialSplit).isEqualTo(splitName);

        int i = 0;
        while (records.nextRecordFromSplit() != null) {
            i++;
        }
        // there were less than 10000 generic records read, the max per fetch
        assertThat(i).isLessThan(10001);
        // there are no more splits
        assertThat(records.nextSplit()).isNull();

        // now there should be more data in the split and now should be able to finalize it
        records = reader.fetch();
        assertThat(records.finishedSplits()).isNotEmpty();

        // after processing no more splits can be retrieved
        records = reader.fetch();
        assertThat(records.finishedSplits()).isEmpty();
    }

    @Test
    public void testClientReuse() throws IOException {
        // Reset the counter
        StorageClientFaker.FakeBigQueryServices.STORAGE_READ_CLIENT_INVOCATIONS.set(0);

        // init the read options for BQ
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, 10);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        // Assign 2 splits. If client is NOT reused, it would be created twice (once per split).
        // If reused, it should be created only once.
        String splitName1 = "stream1";
        BigQuerySourceSplit split1 = new BigQuerySourceSplit(splitName1, 0L);
        String splitName2 = "stream2";
        BigQuerySourceSplit split2 = new BigQuerySourceSplit(splitName2, 0L);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Arrays.asList(split1, split2));

        // send an assignment
        reader.handleSplitsChanges(change);

        // Fetch data for first split
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // Consume first split fully
        while (records.nextSplit() != null) {
            while (records.nextRecordFromSplit() != null) {}
        }

        // Fetch again to process second split
        records = reader.fetch();
        // Consume second split fully
        while (records.nextSplit() != null) {
            while (records.nextRecordFromSplit() != null) {}
        }

        // Assert that the client was created exactly ONCE
        assertThat(StorageClientFaker.FakeBigQueryServices.STORAGE_READ_CLIENT_INVOCATIONS.get())
                .isEqualTo(1);
    }
}

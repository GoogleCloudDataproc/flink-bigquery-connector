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

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceSplitReaderTest {

    @Test
    public void testLimitAcrossPartialFetches() throws Exception {
        assertLimit(0L, 175, 175);
    }

    @Test
    public void testLimitAcrossPartialFetchesFromRestoredOffset() throws Exception {
        assertLimit(50L, 175, 175);
    }

    @Test
    public void testRestoredOffsetDoesNotCountAgainstReadLimit() throws Exception {
        assertLimit(50L, 500, 250);
    }

    private void assertLimit(long restoredOffset, int limit, int expectedCount) throws Exception {
        String schemaString = StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING;
        Schema schema = new Schema.Parser().parse(schemaString);
        List<GenericRecord> expected = new ArrayList<>();
        for (long index = 0; index < 300; index++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", "row-" + index);
            record.put("number", index);
            record.put("ts", index);
            expected.add(record);
        }
        BigQuerySourceReaderContext context =
                new BigQuerySourceReaderContext(Mockito.mock(SourceReaderContext.class), limit);
        BigQueryReadOptions options =
                StorageClientFaker.createReadOptions(300, 1, schemaString)
                        .toBuilder()
                        .setMaxRecordsPerSplitFetch(100)
                        .build();
        BigQuerySourceSplitReader reader =
                Mockito.spy(new BigQuerySourceSplitReader(options, context));
        Mockito.doAnswer(
                        invocation -> {
                            long offset = reader.offsetToFetch(invocation.getArgument(0));
                            List<ReadRowsResponse> responses = new ArrayList<>();
                            for (int start = (int) offset; start < expected.size(); start += 100) {
                                responses.addAll(
                                        StorageClientFaker.createResponse(
                                                schemaString,
                                                expected.subList(
                                                        start,
                                                        Math.min(start + 100, expected.size())),
                                                0,
                                                1));
                            }
                            return new BigQueryServices.BigQueryServerStream<ReadRowsResponse>() {
                                @Override
                                public Iterator<ReadRowsResponse> iterator() {
                                    return responses.iterator();
                                }

                                @Override
                                public void cancel() {}
                            };
                        })
                .when(reader)
                .retrieveReadStream(Mockito.any());
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(
                                    new BigQuerySourceSplit("stream1", restoredOffset))));
            List<Long> emitted = new ArrayList<>();
            int finished = 0;
            for (int fetch = 0; fetch < 10 && finished == 0; fetch++) {
                RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
                while (batch.nextSplit() != null) {
                    GenericRecord record;
                    while ((record = batch.nextRecordFromSplit()) != null) {
                        emitted.add((Long) record.get("number"));
                    }
                }
                finished += batch.finishedSplits().size();
                batch.recycle();
            }
            assertThat(finished).isEqualTo(1);
            assertThat(emitted).hasSize(expectedCount);
            List<Long> expectedIds = new ArrayList<>();
            for (long index = restoredOffset; index < restoredOffset + expectedCount; index++) {
                expectedIds.add(index);
            }
            assertThat(emitted).containsExactlyElementsIn(expectedIds).inOrder();
            assertThat(context.currentReadCount()).isEqualTo((long) expectedCount);
        } finally {
            reader.close();
        }
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

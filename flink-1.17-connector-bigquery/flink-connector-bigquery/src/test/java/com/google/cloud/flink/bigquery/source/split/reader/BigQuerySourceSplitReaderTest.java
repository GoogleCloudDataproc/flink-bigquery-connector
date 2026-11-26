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

import com.google.cloud.bigquery.storage.v1.AvroRows;
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
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** */
public class BigQuerySourceSplitReaderTest {

    @Test
    public void testProjectedFetchAcrossResponsesAndSplits() throws Exception {
        checkProjectedFetch(-1, 0L, 5000, 5000L);
        checkProjectedFetch(311, 0L, 311, 311L);
        checkProjectedFetch(274, 0L, 274, 274L);
        checkProjectedFetch(-1, 123L, 4877, 4877L);
        checkProjectedFetch(274, 123L, 151, 274L);
        checkProjectedFetch(274, 274L, 0, 274L);
    }

    private void checkProjectedFetch(int limit, long offset, int expected, long expectedCount)
            throws Exception {
        BigQueryReadOptions options =
                StorageClientFaker.createReadOptions(
                                2500, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING)
                        .toBuilder()
                        .setColumnNames(Arrays.asList("name"))
                        .setAvroProjectedBatchSize(137)
                        .build();
        BigQuerySourceReaderContext context =
                new BigQuerySourceReaderContext(Mockito.mock(SourceReaderContext.class), limit);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(options, context);
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Arrays.asList(
                                    new BigQuerySourceSplit("stream1", offset),
                                    new BigQuerySourceSplit("stream2", 0L))));
            int total = 0;
            int finished = 0;
            for (int fetch = 0; fetch < 100 && finished < 2; fetch++) {
                RecordsWithSplitIds<GenericRecord> records = reader.fetch();
                int batch = 0;
                while (records.nextSplit() != null) {
                    GenericRecord record;
                    while ((record = records.nextRecordFromSplit()) != null) {
                        assertThat(record.getSchema().getFields()).hasSize(1);
                        assertThat(record.getSchema().getField("name")).isNotNull();
                        batch++;
                    }
                }
                assertThat(batch).isAtMost(137);
                total += batch;
                if (offset > 0 && finished == 0 && records.finishedSplits().isEmpty()) {
                    assertThat(reader.offsetToFetch(new BigQuerySourceSplit("stream1", offset)))
                            .isEqualTo(offset + total);
                }
                finished += records.finishedSplits().size();
                records.recycle();
            }
            assertThat(total).isEqualTo(expected);
            assertThat(finished).isEqualTo(2);
            assertThat(context.currentReadCount()).isEqualTo(expectedCount);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testProjectedFetchReturnsTheLegacyValues() throws Exception {
        // all columns, then a selection in another order than the schema's
        for (List<String> columns :
                Arrays.asList(Arrays.<String>asList(), Arrays.asList("ts", "name"))) {
            List<String> legacy = readRows(0, columns);
            assertThat(legacy).hasSize(2500 - 123 + 2500);
            assertThat(readRows(137, columns)).containsExactlyElementsIn(legacy).inOrder();
        }
    }

    /** Reads both splits, the first restored at offset 123, as one string per row. */
    private static List<String> readRows(int batchSize, List<String> columns) throws Exception {
        BigQueryReadOptions options =
                StorageClientFaker.createReadOptions(
                                2500,
                                2,
                                StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING,
                                BigQuerySourceSplitReaderTest::distinctRecords)
                        .toBuilder()
                        .setColumnNames(columns)
                        .setAvroProjectedBatchSize(batchSize)
                        .build();
        List<String> fields = columns.isEmpty() ? Arrays.asList("name", "number", "ts") : columns;
        BigQuerySourceSplitReader reader =
                new BigQuerySourceSplitReader(
                        options,
                        new BigQuerySourceReaderContext(
                                Mockito.mock(SourceReaderContext.class), -1));
        List<String> rows = new ArrayList<>();
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Arrays.asList(
                                    new BigQuerySourceSplit("stream1", 123L),
                                    new BigQuerySourceSplit("stream2", 0L))));
            int finished = 0;
            for (int fetch = 0; fetch < 100 && finished < 2; fetch++) {
                RecordsWithSplitIds<GenericRecord> records = reader.fetch();
                String split;
                while ((split = records.nextSplit()) != null) {
                    GenericRecord record;
                    while ((record = records.nextRecordFromSplit()) != null) {
                        StringBuilder row = new StringBuilder(split);
                        for (String field : fields) {
                            row.append('|').append(record.get(field));
                        }
                        rows.add(row.toString());
                    }
                }
                finished += records.finishedSplits().size();
                records.recycle();
            }
            assertThat(finished).isEqualTo(2);
        } finally {
            reader.close();
        }
        return rows;
    }

    private static List<GenericRecord> distinctRecords(
            StorageClientFaker.RecordGenerationParams params) {
        Schema schema = new Schema.Parser().parse(params.getAvroSchemaString());
        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < params.getRecordCount(); i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", "name-" + i);
            record.put("number", 7L * i);
            record.put("ts", 1_700_000_000_000_000L + i);
            records.add(record);
        }
        return records;
    }

    @Test
    public void testProjectedFetchSkipsResponsesWithoutRows() throws Exception {
        List<GenericRecord> records = records(300);
        // a heartbeat before the first schema, then responses with an empty row block
        ListStream stream =
                new ListStream(
                        Arrays.asList(
                                ReadRowsResponse.getDefaultInstance(),
                                dataResponse(records.subList(0, 150)),
                                ReadRowsResponse.newBuilder()
                                        .setAvroRows(AvroRows.getDefaultInstance())
                                        .build(),
                                ReadRowsResponse.getDefaultInstance(),
                                dataResponse(records.subList(150, 300))));
        BigQuerySourceSplitReader reader = projectedReader(context(-1), stream);
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new BigQuerySourceSplit("stream1", 0L))));
            List<String> names = new ArrayList<>();
            int finished = 0;
            for (int fetch = 0; fetch < 20 && finished == 0; fetch++) {
                RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
                while (batch.nextSplit() != null) {
                    GenericRecord record;
                    while ((record = batch.nextRecordFromSplit()) != null) {
                        names.add(record.get("name").toString());
                    }
                }
                finished += batch.finishedSplits().size();
                batch.recycle();
            }
            assertThat(finished).isEqualTo(1);
            assertThat(names)
                    .containsExactlyElementsIn(
                            records.stream()
                                    .map(record -> record.get("name").toString())
                                    .collect(Collectors.toList()))
                    .inOrder();
            assertThat(stream.cancels).isEqualTo(0);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testProjectedFetchCountsRestoredOffsetsBeforeOpeningAStream() throws Exception {
        BigQuerySourceReaderContext context = context(150);
        BigQuerySourceSplitReader reader =
                projectedReader(
                        context,
                        new ListStream(Collections.singletonList(dataResponse(records(300)))));
        try {
            // neither restored split reaches the limit alone, but both together do
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Arrays.asList(
                                    new BigQuerySourceSplit("stream1", 100L),
                                    new BigQuerySourceSplit("stream2", 100L))));
            assertThat(context.currentReadCount()).isEqualTo(200L);
            RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
            assertThat(batch.nextSplit()).isNull();
            assertThat(batch.finishedSplits()).containsExactly("stream1", "stream2");
            Mockito.verify(reader, Mockito.never()).retrieveReadStream(Mockito.any());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testProjectedFetchCancelsTheStreamWhenTheLimitStopsIt() throws Exception {
        ListStream stream = new ListStream(Collections.singletonList(dataResponse(records(300))));
        BigQuerySourceSplitReader reader = projectedReader(context(10), stream);
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new BigQuerySourceSplit("stream1", 0L))));
            RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
            int read = 0;
            while (batch.nextSplit() != null) {
                while (batch.nextRecordFromSplit() != null) {
                    read++;
                }
            }
            assertThat(read).isEqualTo(10);
            assertThat(batch.finishedSplits()).containsExactly("stream1");
            assertThat(stream.cancels).isEqualTo(1);
        } finally {
            reader.close();
        }
    }

    private static BigQuerySourceReaderContext context(int limit) {
        return new BigQuerySourceReaderContext(Mockito.mock(SourceReaderContext.class), limit);
    }

    @Test
    public void testProjectedFetchFailsOnAnInvalidColumnPath() throws Exception {
        ListStream stream = new ListStream(Collections.singletonList(dataResponse(records(300))));
        BigQuerySourceSplitReader reader =
                projectedReader(context(-1), stream, Arrays.asList("name", "missing"));
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new BigQuerySourceSplit("stream1", 0L))));
            IllegalArgumentException error =
                    assertThrows(IllegalArgumentException.class, reader::fetch);
            assertThat(error).hasMessageThat().contains("[missing]");
            assertThat(stream.cancels).isEqualTo(1);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testProjectedFetchFinishesWhenCancellingTheStreamFails() throws Exception {
        ListStream stream =
                new ListStream(Collections.singletonList(dataResponse(records(300))), -1, true);
        BigQuerySourceSplitReader reader = projectedReader(context(10), stream);
        try {
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new BigQuerySourceSplit("stream1", 0L))));
            RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
            int read = 0;
            while (batch.nextSplit() != null) {
                while (batch.nextRecordFromSplit() != null) {
                    read++;
                }
            }
            assertThat(read).isEqualTo(10);
            assertThat(batch.finishedSplits()).containsExactly("stream1");
            assertThat(stream.cancels).isEqualTo(1);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testProjectedFetchRetriesAMidResponseFailureOfARestoredSplit() throws Exception {
        List<GenericRecord> records = records(300);
        BigQuerySourceReaderContext context = context(200);
        BigQuerySourceSplitReader reader =
                projectedReader(context, new ListStream(Collections.emptyList()));
        List<Long> offsets = new ArrayList<>();
        List<ListStream> streams = new ArrayList<>();
        Mockito.doAnswer(
                        invocation -> {
                            long offset = reader.offsetToFetch(invocation.getArgument(0));
                            offsets.add(offset);
                            List<ReadRowsResponse> responses = new ArrayList<>();
                            for (int start = (int) offset; start < records.size(); start += 100) {
                                responses.add(
                                        dataResponse(
                                                records.subList(
                                                        start,
                                                        Math.min(start + 100, records.size()))));
                            }
                            // the first stream fails when asked for its second response
                            ListStream stream =
                                    new ListStream(responses, streams.isEmpty() ? 1 : -1, false);
                            streams.add(stream);
                            return stream;
                        })
                .when(reader)
                .retrieveReadStream(Mockito.any());
        try {
            // restored at offset 50, with a limit of 200 rows
            reader.handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(new BigQuerySourceSplit("stream1", 50L))));
            List<String> names = new ArrayList<>();
            int finished = 0;
            for (int fetch = 0; fetch < 20 && finished == 0; fetch++) {
                RecordsWithSplitIds<GenericRecord> batch = reader.fetch();
                while (batch.nextSplit() != null) {
                    GenericRecord record;
                    while ((record = batch.nextRecordFromSplit()) != null) {
                        names.add(record.get("name").toString());
                    }
                }
                finished += batch.finishedSplits().size();
                batch.recycle();
            }
            assertThat(finished).isEqualTo(1);
            // the failed fetch emits nothing, and the retry reopens at the restored offset
            assertThat(offsets).containsExactly(50L, 50L).inOrder();
            assertThat(names)
                    .containsExactlyElementsIn(
                            records.subList(50, 200).stream()
                                    .map(record -> record.get("name").toString())
                                    .collect(Collectors.toList()))
                    .inOrder();
            assertThat(context.currentReadCount()).isEqualTo(200L);
            assertThat(streams).hasSize(2);
            assertThat(streams.get(0).cancels).isEqualTo(1);
            assertThat(streams.get(1).cancels).isEqualTo(1);
        } finally {
            reader.close();
        }
    }

    private static BigQuerySourceSplitReader projectedReader(
            BigQuerySourceReaderContext context, ListStream stream) throws IOException {
        return projectedReader(context, stream, Arrays.asList("name"));
    }

    private static BigQuerySourceSplitReader projectedReader(
            BigQuerySourceReaderContext context, ListStream stream, List<String> columns)
            throws IOException {
        BigQueryReadOptions options =
                StorageClientFaker.createReadOptions(
                                300, 1, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING)
                        .toBuilder()
                        .setColumnNames(columns)
                        .setAvroProjectedBatchSize(137)
                        .build();
        BigQuerySourceSplitReader reader =
                Mockito.spy(new BigQuerySourceSplitReader(options, context));
        Mockito.doReturn(stream).when(reader).retrieveReadStream(Mockito.any());
        return reader;
    }

    private static List<GenericRecord> records(int count) {
        return distinctRecords(
                new StorageClientFaker.RecordGenerationParams(
                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING, count));
    }

    private static ReadRowsResponse dataResponse(List<GenericRecord> records) {
        List<ReadRowsResponse> responses =
                StorageClientFaker.createResponse(
                        StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING, records, 0, 1);
        assertThat(responses).hasSize(1);
        return responses.get(0);
    }

    /**
     * Returns the given responses and counts the cancel calls. It can fail when asked for a
     * response after the first {@code failAfter} ones, and when cancelled.
     */
    private static class ListStream
            implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {
        private final List<ReadRowsResponse> responses;
        private final int failAfter;
        private final boolean failOnCancel;
        private int cancels = 0;

        private ListStream(List<ReadRowsResponse> responses) {
            this(responses, -1, false);
        }

        private ListStream(List<ReadRowsResponse> responses, int failAfter, boolean failOnCancel) {
            this.responses = responses;
            this.failAfter = failAfter;
            this.failOnCancel = failOnCancel;
        }

        @Override
        public Iterator<ReadRowsResponse> iterator() {
            Iterator<ReadRowsResponse> delegate = responses.iterator();
            return new Iterator<ReadRowsResponse>() {
                private int returned = 0;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public ReadRowsResponse next() {
                    if (returned == failAfter) {
                        throw new RuntimeException("transient read failure");
                    }
                    returned++;
                    return delegate.next();
                }
            };
        }

        @Override
        public void cancel() {
            cancels++;
            if (failOnCancel) {
                throw new RuntimeException("cancel failure");
            }
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

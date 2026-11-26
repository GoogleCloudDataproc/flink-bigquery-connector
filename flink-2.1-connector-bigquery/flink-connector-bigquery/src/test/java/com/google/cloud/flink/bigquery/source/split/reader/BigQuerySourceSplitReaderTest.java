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
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceSplitReaderTest {

    @Test
    public void testGenericRecordReaderDecodesResponsesInPlace() throws IOException {
        String schemaString = StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING;
        BigQuerySourceSplitReader.GenericRecordReader reader =
                BigQuerySourceSplitReader.GenericRecordReader.create(
                        new Schema.Parser().parse(schemaString));
        List<GenericRecord> retained = new ArrayList<>();
        List<String> retainedExpected = new ArrayList<>();
        for (int count : new int[] {50, 1, 0, 75, 2}) {
            List<GenericRecord> expected =
                    StorageClientFaker.createRecordList(
                            new StorageClientFaker.RecordGenerationParams(schemaString, count));
            byte[] rows =
                    expected.isEmpty()
                            ? new byte[0]
                            : StorageClientFaker.createResponse(schemaString, expected, 0, 1)
                                    .get(0)
                                    .getAvroRows()
                                    .getSerializedBinaryRows()
                                    .toByteArray();
            // the rows start inside a larger buffer, as a slice of a bigger response can
            byte[] padded = new byte[rows.length + 7];
            System.arraycopy(rows, 0, padded, 7, rows.length);
            AvroRows avroRows =
                    AvroRows.newBuilder()
                            .setSerializedBinaryRows(ByteString.copyFrom(padded).substring(7))
                            .build();
            List<String> decoded = new ArrayList<>();
            List<GenericRecord> responseRecords = reader.processRows(avroRows);
            retained.addAll(responseRecords);
            for (GenericRecord record : responseRecords) {
                decoded.add(record.toString());
            }
            List<String> expectedRows = new ArrayList<>();
            for (GenericRecord record : expected) {
                expectedRows.add(record.toString());
            }
            assertThat(decoded).containsExactlyElementsIn(expectedRows).inOrder();
            retainedExpected.addAll(expectedRows);
        }
        List<String> retainedRows = new ArrayList<>();
        for (GenericRecord record : retained) {
            retainedRows.add(record.toString());
        }
        assertThat(retainedRows).containsExactlyElementsIn(retainedExpected).inOrder();
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

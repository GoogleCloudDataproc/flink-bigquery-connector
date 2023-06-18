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

package org.apache.flink.connector.bigquery.source.split.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.connector.bigquery.source.reader.BigQuerySourceReaderContext;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;
import org.apache.flink.connector.bigquery.utils.StorageClientMocker;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

/** */
public class BigQuerySourceSplitReaderTest {

    private BigQueryReadOptions readOptions;

    @Before
    public void beforeTest() throws Exception {
        // init the read options for BQ
        this.readOptions =
                StorageClientMocker.createReadOptions(
                        10, 2, StorageClientMocker.SIMPLE_AVRO_SCHEMA_STRING);
    }

    @Test
    public void testSplitReader() throws IOException {
        SourceReaderContext readerContext = Mockito.mock(SourceReaderContext.class);
        BigQuerySourceReaderContext context = new BigQuerySourceReaderContext(readerContext, 10);
        BigQuerySourceSplitReader reader = new BigQuerySourceSplitReader(readOptions, context);
        // wake the thing up
        reader.wakeUp();

        String splitName = "stream1";
        BigQuerySourceSplit split = new BigQuerySourceSplit(splitName, 0);
        BigQuerySourceSplit split2 = new BigQuerySourceSplit("stream2", 0);
        SplitsAddition<BigQuerySourceSplit> change =
                new SplitsAddition<>(Lists.newArrayList(split, split2));

        // send an assignment
        reader.handleSplitsChanges(change);

        // this should fetch us some data
        RecordsWithSplitIds<GenericRecord> records = reader.fetch();
        // there is one finished split and is named stream1
        Assert.assertTrue(records.finishedSplits().size() == 1);

        String firstSplit = records.nextSplit();
        Assert.assertNotNull(firstSplit);
        Assert.assertTrue(firstSplit.equals(splitName));

        int i = 0;
        while (records.nextRecordFromSplit() != null) {
            i++;
        }
        // there were 10 generic records read
        Assert.assertTrue(i == 10);
        // there are no more splits
        Assert.assertNull(records.nextSplit());

        // now there should be another split to process
        records = reader.fetch();
        Assert.assertTrue(!records.finishedSplits().isEmpty());

        // after processing no more splits can be retrieved
        records = reader.fetch();
        Assert.assertTrue(records.finishedSplits().isEmpty());
    }
}

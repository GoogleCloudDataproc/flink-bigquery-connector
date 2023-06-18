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

package org.apache.flink.connector.bigquery.source.split;

import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.connector.bigquery.source.enumerator.BigQuerySourceEnumState;
import org.apache.flink.connector.bigquery.utils.StorageClientMocker;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

/** */
public class BigQuerySourceSplitAssignerTest {

    private BigQueryReadOptions readOptions;

    @Before
    public void beforeTest() throws IOException {
        this.readOptions =
                StorageClientMocker.createReadOptions(
                        0, 2, StorageClientMocker.SIMPLE_AVRO_SCHEMA_STRING);
    }

    @Test
    public void testAssignment() {
        // initialize the assigner with default options since we are faking the bigquery services
        BigQuerySourceSplitAssigner assigner =
                new BigQuerySourceSplitAssigner(
                        this.readOptions, BigQuerySourceEnumState.initialState());
        // request the retrieval of the bigquery table info
        assigner.open();

        // should retrieve the first split representing the firt stream
        Optional<BigQuerySourceSplit> maybeSplit = assigner.getNext();
        Assert.assertTrue(maybeSplit.isPresent());
        // should retrieve the second split representing the second stream
        maybeSplit = assigner.getNext();
        Assert.assertTrue(maybeSplit.isPresent());
        BigQuerySourceSplit split = maybeSplit.get();
        // no more splits should be available
        maybeSplit = assigner.getNext();
        Assert.assertTrue(!maybeSplit.isPresent());
        Assert.assertTrue(assigner.noMoreSplits());
        // lets check on the enum state
        BigQuerySourceEnumState state = assigner.snapshotState(0);
        Assert.assertTrue(state.getRemaniningTableStreams().isEmpty());
        Assert.assertTrue(state.getRemainingSourceSplits().isEmpty());
        // add some splits back
        assigner.addSplitsBack(Lists.newArrayList(split));
        // check again on the enum state
        state = assigner.snapshotState(0);
        Assert.assertTrue(state.getRemaniningTableStreams().isEmpty());
        Assert.assertTrue(!state.getRemainingSourceSplits().isEmpty());
        // empty it again and check
        assigner.getNext();
        maybeSplit = assigner.getNext();
        Assert.assertTrue(!maybeSplit.isPresent());
        Assert.assertTrue(assigner.noMoreSplits());
    }
}

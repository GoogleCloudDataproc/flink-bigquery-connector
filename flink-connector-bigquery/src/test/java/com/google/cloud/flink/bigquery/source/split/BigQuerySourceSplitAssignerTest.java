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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.common.truth.Truth8;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceSplitAssignerTest {

    private BigQueryReadOptions readOptions;

    @Before
    public void beforeTest() throws IOException {
        this.readOptions =
                StorageClientFaker.createReadOptions(
                        0, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
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
        Truth8.assertThat(maybeSplit).isPresent();
        // should retrieve the second split representing the second stream
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isPresent();
        BigQuerySourceSplit split = maybeSplit.get();
        // no more splits should be available
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isEmpty();
        assertThat(assigner.noMoreSplits()).isTrue();
        // lets check on the enum state
        BigQuerySourceEnumState state = assigner.snapshotState(0);
        assertThat(state.getRemaniningTableStreams()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isEmpty();
        // add some splits back
        assigner.addSplitsBack(Lists.newArrayList(split));
        // check again on the enum state
        state = assigner.snapshotState(0);
        assertThat(state.getRemaniningTableStreams()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isNotEmpty();
        // empty it again and check
        assigner.getNext();
        maybeSplit = assigner.getNext();
        Truth8.assertThat(maybeSplit).isEmpty();
        assertThat(assigner.noMoreSplits()).isTrue();
    }
}

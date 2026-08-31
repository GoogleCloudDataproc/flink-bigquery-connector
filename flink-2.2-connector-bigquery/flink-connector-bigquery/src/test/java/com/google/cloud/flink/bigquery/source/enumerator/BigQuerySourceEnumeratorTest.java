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

package com.google.cloud.flink.bigquery.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQuerySourceEnumerator}. */
public class BigQuerySourceEnumeratorTest {

    private BigQueryReadOptions readOptions;

    @Before
    public void beforeTest() throws IOException {
        this.readOptions =
                StorageClientFaker.createReadOptions(
                                0, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING)
                        .toBuilder()
                        .setParallelism(1)
                        .build();
    }

    @Test
    public void noMoreSplitsIsOnlySentToTheRequestingReader() throws Exception {
        BigQuerySourceEnumState seed = seedWithOneRemainingStream();

        try (MockSplitEnumeratorContext<BigQuerySourceSplit> ctx =
                new MockSplitEnumeratorContext<>(2)) {
            BigQuerySourceEnumerator enumerator =
                    new BigQuerySourceEnumerator(Boundedness.BOUNDED, ctx, readOptions, seed);
            enumerator.start();
            ctx.registerReader(new ReaderInfo(0, host(0)));
            ctx.registerReader(new ReaderInfo(1, host(1)));

            // Reader 0 picks up the only available split.
            enumerator.handleSplitRequest(0, host(0));
            // Reader 1 asks while reader 0 is still processing stream-0.
            enumerator.handleSplitRequest(1, host(1));

            // The requesting reader is told no-more-splits.
            assertThat(ctx.hasNoMoreSplits(1)).isTrue();

            // The reader that is still processing a split must NOT be signaled until it requests a
            // split
            assertThat(ctx.hasNoMoreSplits(0)).isFalse();
            enumerator.handleSplitRequest(0, host(0));
            assertThat(ctx.hasNoMoreSplits(0)).isTrue();
        }
    }

    @Test
    public void readerSignaledNoMoreSplitsIsNotAssignedOnPoolRefill() throws Exception {
        BigQuerySourceEnumState seed = seedWithOneRemainingStream();

        try (MockSplitEnumeratorContext<BigQuerySourceSplit> ctx =
                new MockSplitEnumeratorContext<>(2)) {
            BigQuerySourceEnumerator enumerator =
                    new BigQuerySourceEnumerator(Boundedness.BOUNDED, ctx, readOptions, seed);
            enumerator.start();
            ctx.registerReader(new ReaderInfo(0, host(0)));
            ctx.registerReader(new ReaderInfo(1, host(1)));

            // Reader 0 picks up the only split; reader 1 then requests and gets no-more-splits.
            enumerator.handleSplitRequest(0, host(0));
            SplitsAssignment<BigQuerySourceSplit> firstAssignment =
                    ctx.getSplitsAssignmentSequence().get(0);
            List<BigQuerySourceSplit> splitsAssignedToReader0 = firstAssignment.assignment().get(0);
            assertThat(splitsAssignedToReader0).hasSize(1);
            BigQuerySourceSplit returnedSplit = splitsAssignedToReader0.get(0);

            enumerator.handleSplitRequest(1, host(1));
            assertThat(ctx.hasNoMoreSplits(1)).isTrue();
            int assignmentsBeforeRefill = ctx.getSplitsAssignmentSequence().size();

            // Reader 0's subtask fails; its split comes back to the pool.
            enumerator.addSplitsBack(Collections.singletonList(returnedSplit), 0);

            // Next split-discovery tick runs assignSplits with a refilled pool.
            enumerator.notifySplits();

            // Reader 1 never re-requested, so it must not be assigned the returned split.
            assertThat(ctx.getSplitsAssignmentSequence()).hasSize(assignmentsBeforeRefill);
        }
    }

    private static String host(int subtaskId) {
        return "host-" + subtaskId;
    }

    private BigQuerySourceEnumState seedWithOneRemainingStream() {
        return new BigQuerySourceEnumState(
                Collections.emptyList(),
                Collections.singletonList("stream-0"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                true);
    }
}

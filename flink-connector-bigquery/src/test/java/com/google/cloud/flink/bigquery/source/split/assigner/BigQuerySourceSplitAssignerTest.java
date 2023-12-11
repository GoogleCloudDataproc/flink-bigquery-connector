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

package com.google.cloud.flink.bigquery.source.split.assigner;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.SplitDiscoveryScheduler;
import com.google.common.truth.Truth8;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

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
    public void testBoundedAssignment() {
        // initialize the assigner with default options since we are faking the bigquery services
        BigQuerySourceSplitAssigner assigner =
                BigQuerySourceSplitAssigner.createBounded(
                        this.readOptions, BigQuerySourceEnumState.initialState());
        // request the retrieval of the bigquery table info
        assigner.openAndDiscoverSplits();

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
        assigner.addSplitsBack(Arrays.asList(split));
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

    @Test
    public void testReadConfigurationChangeForQueryResultRead() throws IOException {
        String queryProject = "a-diff-project";
        String query = "SELECT 1 FROM BQ";
        // lets keep the faked impls for the testing connection options
        BigQueryConnectOptions bqOptions = this.readOptions.getBigQueryConnectOptions();
        BigQueryReadOptions readOpt =
                this.readOptions
                        .toBuilder()
                        // this overrides the bq connect options in regular usage
                        .setQueryAndExecutionProject(query, queryProject)
                        // lets set the faked ones again
                        .setBigQueryConnectOptions(bqOptions)
                        .build();
        // lets get the reference of the spied query data client reference
        BigQueryServices.QueryDataClient qdc =
                this.readOptions
                        .getBigQueryConnectOptions()
                        .getTestingBigQueryServices()
                        .get()
                        .getQueryDataClient(null);

        BigQuerySourceSplitAssigner assigner =
                BigQuerySourceSplitAssigner.createBounded(
                        readOpt, BigQuerySourceEnumState.initialState());

        assigner.openAndDiscoverSplits();

        // validate that a query has been executed with the passed params
        Mockito.verify(qdc, Mockito.times(1)).runQuery(queryProject, query);
    }

    private SplitDiscoveryScheduler createObserver() {
        // mock the observer to return our mocked context
        SplitDiscoveryScheduler observer = Mockito.mock(SplitDiscoveryScheduler.class);
        // make the mocked context to call the discovery and handler methods, without errors when
        // async call is set in the assigner.
        Mockito.doAnswer(
                        inv -> {
                            Callable<UnboundedSplitAssigner.DiscoveryResult> discoverCall =
                                    inv.getArgument(0, Callable.class);
                            BiConsumer<UnboundedSplitAssigner.DiscoveryResult, Throwable> handler =
                                    inv.getArgument(1, BiConsumer.class);
                            handler.accept(discoverCall.call(), null);
                            return null;
                        })
                .when(observer)
                .schedule(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong());
        return observer;
    }

    private UnboundedSplitAssigner createUnbounded(SplitDiscoveryScheduler observer) {
        return (UnboundedSplitAssigner)
                BigQuerySourceSplitAssigner.createUnbounded(
                        observer, readOptions, BigQuerySourceEnumState.initialState());
    }

    @Test
    public void testUnboundedAssignment() {
        SplitDiscoveryScheduler observer = createObserver();
        BigQuerySourceSplitAssigner assigner = createUnbounded(observer);

        assigner.discoverSplits();
        // we should validate that the context and notify methods were called in the observer during
        // the discovery process
        Mockito.verify(observer, Mockito.times(1))
                .schedule(Mockito.any(), Mockito.any(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(observer, Mockito.times(1)).notifySplits();

        BigQuerySourceEnumState state = assigner.snapshotState(0);

        // we should have captured 4 partitions
        assertThat(state.getLastSeenPartitions()).hasSize(4);
        // and since we are hardcoding 2 streams per each read session in the fake service we should
        // see 8 read streams being discovered
        assertThat(state.getRemaniningTableStreams()).hasSize(8);
        // since no assign request has been done, no splits or anything else should have been
        // registered in the state
        assertThat(state.getCompletedTableStreams()).isEmpty();
        assertThat(state.getAssignedSourceSplits()).isEmpty();
        assertThat(state.getRemainingSourceSplits()).isEmpty();
    }

    @Test(expected = RuntimeException.class)
    public void testRuntimeExWhenHandlingExceptionAndNoData() {
        SplitDiscoveryScheduler observer = createObserver();
        UnboundedSplitAssigner assigner = createUnbounded(observer);

        // no discovery has been done, so no data in assigner state
        assigner.handlePartitionSplitDiscovery(null, new Throwable());
    }

    @Test()
    public void testNoObserverNotificationWhenNoData() {
        SplitDiscoveryScheduler observer = createObserver();
        UnboundedSplitAssigner assigner = createUnbounded(observer);

        // no data was discovered
        assigner.handlePartitionSplitDiscovery(
                new UnboundedSplitAssigner.DiscoveryResult(Arrays.asList(), Arrays.asList()), null);
        // so no notification should be triggered
        Mockito.verify(observer, Mockito.times(0)).notifySplits();
    }

    @Test()
    public void testAppendedRightRestrictionWhenPreviouslyExisting() {
        SplitDiscoveryScheduler observer = createObserver();
        UnboundedSplitAssigner assigner = createUnbounded(observer);

        String someRestriction = "adummyrestriction";
        String newRestriction = "anewone";

        String result = assigner.combineRestrictions(someRestriction, newRestriction);

        assertThat(result).startsWith(someRestriction);
        assertThat(result).endsWith(newRestriction);
        assertThat(result).isEqualTo(someRestriction + " AND " + newRestriction);
    }
}

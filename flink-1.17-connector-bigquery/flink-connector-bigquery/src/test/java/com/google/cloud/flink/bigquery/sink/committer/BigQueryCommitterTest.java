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

package com.google.cloud.flink.bigquery.sink.committer;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Tests for {@link BigQueryCommitter}. */
public class BigQueryCommitterTest {

    private BigQueryConnectOptions connectOptions;

    @Test
    public void testCommit_withEmptyCommitRequest() {
        BigQueryCommitter committer = createCommitter(null);
        // BQ write client used in this test throws a RuntimeException if flushRows is invoked with
        // flushRowsResponse set as null. Since flush should not be called, this commit should not
        // throw any exception.
        committer.commit(Collections.EMPTY_LIST);
    }

    @Test
    public void testCommit() {
        BigQueryCommitter committer =
                createCommitter(FlushRowsResponse.newBuilder().setOffset(10L).build());
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testCommit_withOffsetMismatch() {
        BigQueryCommitter committer =
                createCommitter(FlushRowsResponse.newBuilder().setOffset(5L).build());
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testCommit_withFlushRowsApiFailure() {
        // BQ write client used in this test throws a RuntimeException if flushRows is invoked with
        // flushRowsResponse set as null. The committer should wrap client errors in a
        // BigQueryConnectorException.
        BigQueryCommitter committer = createCommitter(null);
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
    }

    @Test
    public void testCommit_finalizesStreamReplacedByProducer() throws IOException {
        BigQueryCommitter committer =
                createCommitter(
                        FlushRowsResponse.newBuilder().setOffset(10L).build(),
                        FinalizeWriteStreamResponse.getDefaultInstance());
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
        // Same stream committed again, as after a restore: nothing to finalize.
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
        assertEquals(0, getWriteClient().getFinalizeWriteStreamInvocations());
        // Producer moved on to a new stream: the previous one is finalized after the commit.
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "bar", 10L))));
        assertEquals(1, getWriteClient().getFinalizeWriteStreamInvocations());
        assertEquals("foo", getWriteClient().getLastFinalizedStreamName());
        // First stream of another producer: nothing to finalize.
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(2L, "baz", 10L))));
        assertEquals(1, getWriteClient().getFinalizeWriteStreamInvocations());
    }

    @Test
    public void testCommit_withFinalizeFailure() throws IOException {
        // BQ write client used in this test throws a RuntimeException if finalizeWriteStream is
        // invoked with finalizeResponse set as null. The commit itself must still succeed.
        BigQueryCommitter committer =
                createCommitter(FlushRowsResponse.newBuilder().setOffset(10L).build(), null);
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L))));
        committer.commit(
                Collections.singletonList(
                        new TestCommitRequest(new BigQueryCommittable(1L, "bar", 10L))));
        assertEquals(1, getWriteClient().getFinalizeWriteStreamInvocations());
        assertEquals("foo", getWriteClient().getLastFinalizedStreamName());
    }

    private BigQueryCommitter createCommitter(FlushRowsResponse flushRowsResponse) {
        return createCommitter(flushRowsResponse, null);
    }

    private BigQueryCommitter createCommitter(
            FlushRowsResponse flushRowsResponse, FinalizeWriteStreamResponse finalizeResponse) {
        connectOptions =
                StorageClientFaker.createConnectOptionsForWrite(
                        new ApiFuture[] {null}, null, flushRowsResponse, finalizeResponse);
        return new BigQueryCommitter(connectOptions);
    }

    private FakeBigQueryStorageWriteClient getWriteClient() throws IOException {
        // FakeBigQueryServices returns the same write client instance on every invocation.
        return (FakeBigQueryStorageWriteClient)
                ((FakeBigQueryServices) connectOptions.getTestingBigQueryServices().get())
                        .createStorageWriteClient(null);
    }

    static class TestCommitRequest implements CommitRequest<BigQueryCommittable> {

        private final BigQueryCommittable committable;

        TestCommitRequest(BigQueryCommittable committable) {
            this.committable = committable;
        }

        @Override
        public BigQueryCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            // Do nothing.
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            // Do nothing.
        }

        @Override
        public void retryLater() {
            // Do nothing.
        }

        @Override
        public void updateAndRetryLater(BigQueryCommittable committable) {
            // Do nothing.
        }

        @Override
        public void signalAlreadyCommitted() {
            // Do nothing.
        }
    }
}

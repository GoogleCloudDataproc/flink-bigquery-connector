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
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices.FakeBigQueryStorageWriteClient;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker.FakeBigQueryServices.TestStatusCode;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void testCommit_withStreamAlreadyFlushedPastOffset() throws IOException {
        BigQueryCommitter committer =
                createCommitter(FlushRowsResponse.newBuilder().setOffset(10L).build());
        // Restored committable of an earlier checkpoint, while the stream was already flushed
        // further by the committable of a later one.
        getWriteClient().setFlushRowsError(flushRowsError(StatusCode.Code.ALREADY_EXISTS));
        TestCommitRequest earlier = new TestCommitRequest(new BigQueryCommittable(1L, "foo", 5L));
        committer.commit(Collections.singletonList(earlier));
        assertTrue(earlier.isAlreadyCommitted());
        // Restored committable of the later checkpoint: flushing to the flushed offset succeeds.
        getWriteClient().setFlushRowsError(null);
        TestCommitRequest later = new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L));
        committer.commit(Collections.singletonList(later));
        assertFalse(later.isAlreadyCommitted());
    }

    @Test
    public void testCommit_withOtherFlushRowsErrors() throws IOException {
        // An offset beyond the appended rows, or a finalized stream, must still fail the commit.
        for (StatusCode.Code code :
                new StatusCode.Code[] {
                    StatusCode.Code.OUT_OF_RANGE, StatusCode.Code.INVALID_ARGUMENT
                }) {
            BigQueryCommitter committer =
                    createCommitter(FlushRowsResponse.newBuilder().setOffset(10L).build());
            getWriteClient().setFlushRowsError(flushRowsError(code));
            TestCommitRequest request =
                    new TestCommitRequest(new BigQueryCommittable(1L, "foo", 10L));
            try {
                committer.commit(Collections.singletonList(request));
                fail("Commit should fail for " + code);
            } catch (BigQueryConnectorException e) {
                assertFalse(request.isAlreadyCommitted());
            }
        }
    }

    private static ApiException flushRowsError(StatusCode.Code code) {
        return new ApiException(
                new RuntimeException("testing error scenario"), new TestStatusCode(code), false);
    }

    private BigQueryCommitter createCommitter(FlushRowsResponse flushRowsResponse) {
        connectOptions =
                StorageClientFaker.createConnectOptionsForWrite(
                        new ApiFuture[] {null}, null, flushRowsResponse, null);
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
        private boolean alreadyCommitted;

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
            alreadyCommitted = true;
        }

        boolean isAlreadyCommitted() {
            return alreadyCommitted;
        }
    }
}

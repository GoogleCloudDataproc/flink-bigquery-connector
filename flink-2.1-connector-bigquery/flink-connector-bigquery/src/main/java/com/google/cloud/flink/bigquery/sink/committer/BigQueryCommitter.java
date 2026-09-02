/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Committer implementation for {@link BigQueryExactlyOnceSink}.
 *
 * <p>The committer is responsible for committing records buffered in BigQuery write stream to
 * BigQuery table. It also finalizes a producer's previous write stream once a committable for a
 * different stream has been committed, because that is the earliest point at which no restorable
 * checkpoint references the previous stream anymore.
 */
public class BigQueryCommitter implements Committer<BigQueryCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCommitter.class);

    private final BigQueryConnectOptions connectOptions;

    // Stream last committed for each producer. A committable that names a different stream means
    // the writer discarded the previous one (its first append after a restore or a checkpoint was
    // rejected), and that stream can now be finalized. It cannot be finalized any earlier: BigQuery
    // rejects
    // FlushRows on a finalized stream even for an offset that was already flushed, and until the
    // checkpoint that introduced the new stream has completed, which is what a commit implies, a
    // restore could still require this committer to flush the previous stream again.
    private final Map<Long, String> lastCommittedStreamNames = new HashMap<>();

    public BigQueryCommitter(BigQueryConnectOptions connectOptions) {
        this.connectOptions = connectOptions;
    }

    @Override
    public void commit(Collection<CommitRequest<BigQueryCommittable>> commitRequests) {
        if (commitRequests.isEmpty()) {
            LOG.info("No committable found. Nothing to commit!");
            return;
        }
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            for (CommitRequest<BigQueryCommittable> commitRequest : commitRequests) {
                BigQueryCommittable committable = commitRequest.getCommittable();
                long producerId = committable.getProducerId();
                String streamName = committable.getStreamName();
                long streamOffset = committable.getStreamOffset();
                LOG.info("Committing records appended by producer {}", producerId);
                LOG.debug(
                        "Invoking flushRows API on stream {} till offset {}",
                        streamName,
                        streamOffset);
                FlushRowsResponse response = writeClient.flushRows(streamName, streamOffset);
                if (response.getOffset() != streamOffset) {
                    LOG.error(
                            "BigQuery FlushRows API failed. Returned offset {}, expected {}",
                            response.getOffset(),
                            streamOffset);
                    throw new BigQueryConnectorException(
                            String.format("Commit operation failed for producer %d", producerId));
                }
                String previousStreamName = lastCommittedStreamNames.put(producerId, streamName);
                if (previousStreamName != null && !previousStreamName.equals(streamName)) {
                    finalizeReplacedStream(writeClient, previousStreamName, producerId);
                }
            }
        } catch (IOException | ApiException e) {
            throw new BigQueryConnectorException("Commit operation failed", e);
        }
    }

    private void finalizeReplacedStream(
            BigQueryServices.StorageWriteClient writeClient, String streamName, long producerId) {
        LOG.info("Finalizing write stream {} replaced by producer {}", streamName, producerId);
        try {
            writeClient.finalizeWriteStream(streamName);
        } catch (Exception e) {
            // Not fatal: nothing is appended to or flushed from this stream anymore, and BigQuery
            // expires idle streams on its own.
            LOG.warn(
                    String.format(
                            "Failed to finalize write stream %s replaced by producer %d",
                            streamName, producerId),
                    e);
        }
    }

    @Override
    public void close() {
        // No op.
    }
}

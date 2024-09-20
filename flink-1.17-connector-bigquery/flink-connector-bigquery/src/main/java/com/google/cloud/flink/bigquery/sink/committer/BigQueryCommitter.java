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
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * Committer implementation for {@link BigQueryExactlyOnceSink}.
 *
 * <p>The committer is responsible for committing records buffered in BigQuery write stream to
 * BigQuery table.
 */
public class BigQueryCommitter implements Committer<BigQueryCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCommitter.class);

    private final BigQueryConnectOptions connectOptions;

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
                FlushRowsResponse response = writeClient.flushRows(streamName, streamOffset);
                if (response.getOffset() != streamOffset) {
                    LOG.error(
                            "BigQuery FlushRows API failed. Returned offset {}, expected {}",
                            response.getOffset(),
                            streamOffset);
                    throw new BigQueryConnectorException(
                            String.format("Commit operation failed for producer %d", producerId));
                }
            }
        } catch (IOException | ApiException e) {
            throw new BigQueryConnectorException("Commit operation failed", e);
        }
    }

    @Override
    public void close() {
        // No op.
    }
}

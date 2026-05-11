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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesImpl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Custom Committer that wraps FileSink's committer and triggers a BigQuery Load Job. */
class IndirectSinkCommitter implements Committer<FileSinkCommittable> {

    private final Committer<FileSinkCommittable> fileSinkCommitter;
    private final BigQuerySinkConfig<?> sinkConfig;
    private final BigQueryServices.QueryDataClient queryDataClient;

    public IndirectSinkCommitter(
            Committer<FileSinkCommittable> fileSinkCommitter, BigQuerySinkConfig<?> sinkConfig) {
        this.fileSinkCommitter = fileSinkCommitter;
        this.sinkConfig = sinkConfig;
        this.queryDataClient =
                new BigQueryServicesImpl()
                        .createQueryDataClient(
                                sinkConfig.getConnectOptions().getCredentialsOptions());
    }

    @Override
    public void commit(Collection<CommitRequest<FileSinkCommittable>> requests)
            throws IOException, InterruptedException {
        // 1. Delegate to FileSink's committer to finalize files.
        fileSinkCommitter.commit(requests);

        // 2. Scan GCS bucket to find produced specific files.
        String tempGcsBucket = sinkConfig.getTemporaryGcsBucket();
        Path path = new Path(tempGcsBucket);
        String bucketName = path.toUri().getHost();
        String prefix = path.toUri().getPath();
        if (prefix.startsWith("/")) {
            prefix = prefix.substring(1);
        }

        Storage storage =
                StorageOptions.newBuilder()
                        .setCredentials(
                                sinkConfig
                                        .getConnectOptions()
                                        .getCredentialsOptions()
                                        .getCredentials())
                        .build()
                        .getService();

        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix));
        List<String> sourceUris = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().endsWith(".avro")) {
                sourceUris.add("gs://" + bucketName + "/" + blob.getName());
            }
        }

        // 3. Trigger BigQuery Load Job.
        if (!sourceUris.isEmpty()) {
            queryDataClient.submitLoadJob(
                    sinkConfig.getConnectOptions().getProjectId(),
                    sinkConfig.getConnectOptions().getDataset(),
                    sinkConfig.getConnectOptions().getTable(),
                    sourceUris,
                    FormatOptions.avro());
        }

        // 4. Perform conditional file cleanup.
        if (!sinkConfig.getPersistentGcsBucket()) {
            for (Blob blob : blobs.iterateAll()) {
                storage.delete(blob.getBlobId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        fileSinkCommitter.close();
    }
}

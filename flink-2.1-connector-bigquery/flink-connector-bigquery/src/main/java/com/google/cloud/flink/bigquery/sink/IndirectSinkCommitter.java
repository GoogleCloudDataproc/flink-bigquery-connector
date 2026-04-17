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

import java.io.IOException;
import java.util.Collection;

/** Custom Committer that wraps FileSink's committer and triggers a BigQuery Load Job. */
class IndirectSinkCommitter implements Committer<FileSinkCommittable> {

    private final Committer<FileSinkCommittable> fileSinkCommitter;

    public IndirectSinkCommitter(Committer<FileSinkCommittable> fileSinkCommitter) {
        this.fileSinkCommitter = fileSinkCommitter;
    }

    @Override
    public void commit(Collection<CommitRequest<FileSinkCommittable>> requests)
            throws IOException, InterruptedException {
        // 1. Delegate to FileSink's committer to finalize files.
        fileSinkCommitter.commit(requests);

        // 2. Scan GCS bucket to find produced specific files.
        // TODO: Implement directory listing resolving specific file URIs.

        // 3. Trigger BigQuery Load Job.
        // TODO: Use BigQueryServices to submit load job.

        // 4. Perform conditional file cleanup.
        // TODO: Based on persistentGcsBucket flag.
    }

    @Override
    public void close() throws Exception {
        fileSinkCommitter.close();
    }
}

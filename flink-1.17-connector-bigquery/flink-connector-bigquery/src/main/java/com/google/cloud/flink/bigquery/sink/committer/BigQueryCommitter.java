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

    @Override
    public void commit(Collection<CommitRequest<BigQueryCommittable>> commitRequests)
            throws IOException, InterruptedException {}

    @Override
    public void close() {}
}
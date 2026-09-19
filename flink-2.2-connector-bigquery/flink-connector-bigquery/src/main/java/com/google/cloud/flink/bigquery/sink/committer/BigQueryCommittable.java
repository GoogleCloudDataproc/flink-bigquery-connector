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

import com.google.cloud.flink.bigquery.sink.state.BigQueryStreamState;

/**
 * Information required for a commit operation, passed from {@link BigQueryBufferedWriter} to {@link
 * BigQueryCommitter}.
 */
public class BigQueryCommittable extends BigQueryStreamState {

    private final long producerId;

    public BigQueryCommittable(long producerId, String streamName, long streamOffset) {
        super(streamName, streamOffset);
        this.producerId = producerId;
    }

    public long getProducerId() {
        return producerId;
    }
}

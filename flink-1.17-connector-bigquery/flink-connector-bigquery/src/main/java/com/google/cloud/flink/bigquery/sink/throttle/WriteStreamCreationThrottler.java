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

package com.google.cloud.flink.bigquery.sink.throttle;

import com.google.cloud.flink.bigquery.sink.BigQueryExactlyOnceSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Throttler implementation for BigQuery write stream creation.
 *
 * <p>Each {@link BigQueryBufferedWriter} will invoke BigQuery's CreateWriteStream API before its
 * initial write to a BigQuery table. This API, however, requires a low QPS (~3) for best
 * performance in steady state since write stream creation is an expensive operation for BigQuery
 * storage backend. Hence, this throttler is responsible for distributing writers into buckets which
 * correspond to a specific "wait" duration before calling the CreateWriteStream API.
 *
 * <p>Note that actual separation between CreateWriteStream invocations across all writers will not
 * ensure exact QPS of 3, because neither all writers are initialized at the same instant, nor do
 * they all identify the need to create a write stream after some uniform fixed duration. Given
 * these uncontrollable factors, this throttler aims to achieve 3 QPS on a best effort basis.
 */
public class WriteStreamCreationThrottler implements Throttler {

    // MAX_SINK_PARALLELISM is set as 128.
    public static final int MAX_BUCKETS = BigQueryExactlyOnceSink.MAX_SINK_PARALLELISM / 3;
    private static final Logger LOG = LoggerFactory.getLogger(WriteStreamCreationThrottler.class);
    private final int writerId;

    public WriteStreamCreationThrottler(int writerId) {
        this.writerId = writerId;
    }

    public void throttle() {
        int waitSeconds = writerId % MAX_BUCKETS;
        LOG.debug("Throttling writer {} for {} second", writerId, waitSeconds);
        try {
            // Sleep does nothing if input is 0 or less.
            TimeUnit.SECONDS.sleep(waitSeconds);
        } catch (InterruptedException e) {
            LOG.warn("Throttle attempt interrupted in subtask {}", writerId);
            Thread.currentThread().interrupt();
        }
    }
}

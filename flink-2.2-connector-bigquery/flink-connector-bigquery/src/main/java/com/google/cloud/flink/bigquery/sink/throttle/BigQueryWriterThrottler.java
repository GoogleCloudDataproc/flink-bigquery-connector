/*
 * Copyright 2025 The Apache Software Foundation.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Throttler implementation for BigQuery sink writers.
 *
 * <p>BigQuery APIs used by this sink's writers are subject BigQuery imposed quotas and limits.
 *
 * <p>The createTable API is used if destination BigQuery table does not already exist, and this API
 * allows 10 QPS.
 *
 * <p>The {@link BigQueryBufferedWriter} invokes BigQuery's CreateWriteStream API before its initial
 * write to a BigQuery table. This API expects a low QPS (~3) for best performance in steady state,
 * since write stream creation is an expensive operation.
 *
 * <p>This throttler allocates writers into buckets which correspond to a specific "wait" duration
 * before invoking above BigQuery APIs. Given the distributed nature of Flink deployments, we aim to
 * achieve 3 QPS on a best effort basis.
 */
public class BigQueryWriterThrottler implements Throttler {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriterThrottler.class);
    private final int writerId;
    private final int maxBuckets;

    public BigQueryWriterThrottler(int writerId, int maxParallelism) {
        this.writerId = writerId;
        this.maxBuckets = maxParallelism / 3;
    }

    @Override
    public void throttle() {
        int waitSeconds = writerId % maxBuckets;
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

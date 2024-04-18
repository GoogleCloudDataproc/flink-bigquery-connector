/*
 * Copyright (C) 2023 Google Inc.
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

package com.google.cloud.flink.bigquery.source.split;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/** Defines the behavior of a scheduler used for asynchronous BigQuery source split discovery. */
public interface SplitDiscoveryScheduler {

    /**
     * Schedules the next execution of the split discovery process and chains the results handler.
     *
     * @param <T> The type of the discovery results
     * @param callable The discovery process reference
     * @param handler The discovery result handler reference
     * @param initialDelayMillis The initial delay for the next execution
     * @param periodMillis The frequency of the async execution.
     */
    <T> void schedule(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelayMillis,
            long periodMillis);

    /** Called as soon new splits has been discovered and stored in the assigner's state. */
    void notifySplits();
}

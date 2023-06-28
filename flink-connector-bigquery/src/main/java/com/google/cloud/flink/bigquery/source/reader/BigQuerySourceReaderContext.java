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

package com.google.cloud.flink.bigquery.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.concurrent.atomic.AtomicInteger;

/** A {@link SourceReaderContext} proxy that adds limit and counts for state management. */
@Internal
public class BigQuerySourceReaderContext implements SourceReaderContext {

    private final SourceReaderContext readerContext;
    private final AtomicInteger readCount = new AtomicInteger(0);
    private final Integer limit;

    public BigQuerySourceReaderContext(SourceReaderContext readerContext, Integer limit) {
        this.readerContext = readerContext;
        this.limit = limit;
    }

    @Override
    public SourceReaderMetricGroup metricGroup() {
        return readerContext.metricGroup();
    }

    @Override
    public Configuration getConfiguration() {
        return readerContext.getConfiguration();
    }

    @Override
    public String getLocalHostName() {
        return readerContext.getLocalHostName();
    }

    @Override
    public int getIndexOfSubtask() {
        return readerContext.getIndexOfSubtask();
    }

    @Override
    public void sendSplitRequest() {
        readerContext.sendSplitRequest();
    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
        readerContext.sendSourceEventToCoordinator(sourceEvent);
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return readerContext.getUserCodeClassLoader();
    }

    public Integer updateReadCount(Integer newReads) {
        return readCount.addAndGet(newReads);
    }

    public Integer currentReadCount() {
        return readCount.get();
    }

    public boolean isLimitPushedDown() {
        return limit > 0;
    }

    public boolean willItBeOverLimit(Integer newReads) {
        return limit > 0 && (readCount.get() + newReads) >= limit;
    }

    public int getLimit() {
        return limit;
    }
}

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

package com.google.cloud.flink.bigquery.source.split.assigner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.split.SplitDiscoverer;

/**
 * A bounded implementation for a split assigner based on the BigQuery {@link ReadSession} streams.
 */
@Internal
public class BoundedSplitAssigner extends BigQuerySourceSplitAssigner {

    BoundedSplitAssigner(BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        super(readOptions, sourceEnumState);
    }

    @Override
    public void discoverSplits() {

        this.remainingTableStreams.addAll(
                SplitDiscoverer.discoverSplits(
                        this.readOptions.getBigQueryConnectOptions(),
                        DataFormat.AVRO,
                        this.readOptions.getColumnNames(),
                        this.readOptions.getRowRestriction(),
                        this.readOptions.getSnapshotTimestampInMillis(),
                        this.readOptions.getMaxStreamCount()));
    }

    @Override
    public boolean noMoreSplits() {
        Preconditions.checkState(
                initialized, "The noMoreSplits method was called but not initialized.");
        return remainingTableStreams.isEmpty() && remainingSourceSplits.isEmpty();
    }
}

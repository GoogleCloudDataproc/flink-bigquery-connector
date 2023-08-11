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
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.split.ContextAwareSplitObserver;
import com.google.cloud.flink.bigquery.source.split.SplitDiscoverer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.cloud.flink.bigquery.common.utils.BigQueryPartition.formatPartitionRestrictionBasedOnInfo;
import static com.google.cloud.flink.bigquery.common.utils.BigQueryPartition.partitionValuesFromIdAndDataType;

/** */
@Internal
public class UnboundedSplitAssigner extends BigQuerySourceSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSplitAssigner.class);

    private final ContextAwareSplitObserver observer;

    UnboundedSplitAssigner(
            ContextAwareSplitObserver observer,
            BigQueryReadOptions readOptions,
            BigQuerySourceEnumState sourceEnumState) {
        super(readOptions, sourceEnumState);
        this.observer = observer;
    }

    @VisibleForTesting
    DiscoveryResult discoverNewSplits() {
        LOG.info("Searching for new data to read.");
        BigQueryConnectOptions options = this.readOptions.getBigQueryConnectOptions();
        try {
            BigQueryServices.QueryDataClient client =
                    BigQueryServicesFactory.instance(options).queryClient();

            List<PartitionIdWithInfoAndStatus> newPartitions =
                    client.retrievePartitionsStatus(
                            options.getProjectId(), options.getDataset(), options.getTable());

            LOG.info("Table partitions and their status: {}", newPartitions);
            LOG.info("Already seen partition ids: {}", this.lastSeenPartitions);
            newPartitions =
                    newPartitions.stream()
                            // if configured filter partitions older than the provided id
                            .filter(
                                    pIdStatus ->
                                            Optional.ofNullable(
                                                            this.readOptions.getOldestPartitionId())
                                                    .map(
                                                            oldestPartitionId ->
                                                                    pIdStatus
                                                                                    .getPartitionId()
                                                                                    .compareTo(
                                                                                            oldestPartitionId)
                                                                            >= 0)
                                                    .orElse(true))
                            .filter(pIdStatus -> pIdStatus.isCompleted())
                            .filter(
                                    pIdStatus ->
                                            !this.lastSeenPartitions.contains(
                                                    pIdStatus.getPartitionId()))
                            .collect(Collectors.toList());
            return new DiscoveryResult(
                    newPartitions.stream()
                            .map(p -> p.getPartitionId())
                            .collect(Collectors.toList()),
                    newPartitions.stream()
                            .map(
                                    pId ->
                                            formatPartitionRestrictionBasedOnInfo(
                                                    Optional.of(pId.getInfo()),
                                                    pId.getInfo().getColumnName(),
                                                    partitionValuesFromIdAndDataType(
                                                                    Lists.newArrayList(
                                                                            pId.getPartitionId()),
                                                                    pId.getInfo().getColumnType())
                                                            .get(0)))
                            .flatMap(
                                    restriction ->
                                            SplitDiscoverer.discoverSplits(
                                                    options,
                                                    DataFormat.AVRO,
                                                    this.readOptions.getColumnNames(),
                                                    shouldAppendRestriction(
                                                            this.readOptions.getRowRestriction(),
                                                            restriction),
                                                    this.readOptions.getSnapshotTimestampInMillis(),
                                                    this.readOptions.getMaxStreamCount())
                                                    .stream())
                            .collect(Collectors.toList()));
        } catch (Exception ex) {
            throw new RuntimeException("Problems while trying to discover new splits.", ex);
        }
    }

    @VisibleForTesting
    String shouldAppendRestriction(String existing, String newRestriction) {
        if (existing.isEmpty() || existing.isBlank()) {
            return newRestriction;
        }
        return existing + " AND " + newRestriction;
    }

    @VisibleForTesting
    void handlePartitionSplitDiscovery(DiscoveryResult discovery, Throwable t) {
        if (t != null && this.remainingTableStreams.isEmpty()) {
            // If this was the first split discovery and it failed, throw an error
            throw new RuntimeException(t);
        } else if (t != null) {
            LOG.error("Failed to poll for new read streams, continuing", t);
            return;
        }
        if (discovery.getReadStreams().isEmpty() && discovery.getNewPartitions().isEmpty()) {
            LOG.info("No new partitions for now.");
            return;
        }
        LOG.info("Discovered new partitions: {}", discovery.getNewPartitions());
        LOG.info("Discovered new read streams: {}", discovery.getReadStreams());
        this.lastSeenPartitions.addAll(discovery.getNewPartitions());
        this.remainingTableStreams.addAll(discovery.getReadStreams());
        this.observer.notifyDiscovery();
    }

    @Override
    public void discoverSplits() {
        // periodically schedule the discovery and processing of any new splits
        this.observer
                .context()
                .callAsync(
                        this::discoverNewSplits,
                        this::handlePartitionSplitDiscovery,
                        0,
                        Duration.ofMinutes(
                                        this.readOptions
                                                .getPartitionDiscoveryRefreshIntervalInMinutes())
                                .toMillis());
    }

    @Override
    public boolean noMoreSplits() {
        // we will continue tracking for new partitions been added to the table.
        return false;
    }

    static class DiscoveryResult {

        private final List<String> newPartitions;
        private final List<String> readStreams;

        public DiscoveryResult(List<String> newPartitions, List<String> newStreams) {
            this.newPartitions = newPartitions;
            this.readStreams = newStreams;
        }

        public List<String> getNewPartitions() {
            return newPartitions;
        }

        public List<String> getReadStreams() {
            return readStreams;
        }
    }
}

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

import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.SplitDiscoveryScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/** A base split assigner definition. */
@Internal
public abstract class BigQuerySourceSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceSplitAssigner.class);

    protected final BigQueryReadOptions readOptions;
    protected final Set<String> lastSeenPartitions;
    protected final Queue<String> remainingTableStreams;
    protected final Queue<String> alreadyProcessedTableStreams;
    protected final Queue<BigQuerySourceSplit> remainingSourceSplits;
    protected final Map<String, BigQuerySourceSplit> assignedSourceSplits;
    protected boolean initialized;

    /**
     * Creates a bounded version of the a split assigner.
     *
     * @param readOptions The read options
     * @param sourceEnumState The initial enumerator state
     * @return
     */
    public static BigQuerySourceSplitAssigner createBounded(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        return new BoundedSplitAssigner(readOptions, sourceEnumState);
    }

    /**
     * Creates an unbounded version of the a split assigner.
     *
     * @param observer The split discovery scheduler reference
     * @param readOptions The read options
     * @param sourceEnumState The initial enumerator state
     * @return
     */
    public static BigQuerySourceSplitAssigner createUnbounded(
            SplitDiscoveryScheduler observer,
            BigQueryReadOptions readOptions,
            BigQuerySourceEnumState sourceEnumState) {
        return new UnboundedSplitAssigner(observer, readOptions, sourceEnumState);
    }

    BigQuerySourceSplitAssigner(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        this.readOptions = readOptions;
        this.lastSeenPartitions = ConcurrentHashMap.newKeySet();
        this.lastSeenPartitions.addAll(sourceEnumState.getLastSeenPartitions());
        this.remainingTableStreams =
                new ConcurrentLinkedQueue<>(sourceEnumState.getRemaniningTableStreams());
        this.alreadyProcessedTableStreams =
                new ConcurrentLinkedQueue<>(sourceEnumState.getCompletedTableStreams());
        this.remainingSourceSplits =
                new ConcurrentLinkedQueue<>(sourceEnumState.getRemainingSourceSplits());
        this.assignedSourceSplits = new ConcurrentHashMap<>();
        this.assignedSourceSplits.putAll(sourceEnumState.getAssignedSourceSplits());
        this.initialized = sourceEnumState.isInitialized();
    }

    /**
     * This method implements the split discovery, each of the existing assigner flavors will need
     * to define it.
     */
    public abstract void discoverSplits();

    /** Opens the assigner's context and discovers splits. */
    public void openAndDiscoverSplits() {
        LOG.info("BigQuery source split assigner is opening.");
        if (!initialized) {
            discoverSplits();
            initialized = true;
        }
    }

    /**
     * It will return the provided splits to the internal state, and have them ready to be
     * reassigned.
     *
     * @param splits A list of splits to be added back as available for assignment.
     */
    public void addSplitsBack(List<BigQuerySourceSplit> splits) {
        for (BigQuerySourceSplit split : splits) {
            remainingSourceSplits.add((BigQuerySourceSplit) split);
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSourceSplits.remove(split.splitId());
        }
    }

    /**
     * Transforms the internal assigner's state into an {@link BigQuerySourceEnumState} for
     * checkpoint purposes.
     *
     * @param checkpointId The id of the checkpoint
     * @return The enumeration state instance.
     */
    public BigQuerySourceEnumState snapshotState(long checkpointId) {
        return new BigQuerySourceEnumState(
                new ArrayList<>(lastSeenPartitions),
                new ArrayList<>(remainingTableStreams),
                new ArrayList<>(alreadyProcessedTableStreams),
                new ArrayList<>(remainingSourceSplits),
                new HashMap<>(assignedSourceSplits),
                initialized);
    }

    /** Closes the assigner's execution. */
    public void close() {
        // so far not much to be done on the base type.
        LOG.info("BigQuery source split assigner is closed.");
    }

    /**
     * Given the discovered splits, it will assign and return one, if there is any available.
     *
     * @return An Optional with value if there were splits assigned, empty otherwise.
     */
    public Optional<BigQuerySourceSplit> getNext() {
        if (!remainingSourceSplits.isEmpty()) {
            // return remaining splits firstly
            BigQuerySourceSplit split = remainingSourceSplits.poll();
            assignedSourceSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextStream = remainingTableStreams.poll();
            if (nextStream != null) {
                BigQuerySourceSplit split = new BigQuerySourceSplit(nextStream);
                remainingSourceSplits.add(split);
                alreadyProcessedTableStreams.add(nextStream);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Returns if all the splits has been assigned.
     *
     * @return True if all the splits has been assigned, false otherwise.
     */
    public abstract boolean noMoreSplits();
}

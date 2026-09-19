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

package com.google.cloud.flink.bigquery.source.enumerator;

import org.apache.flink.annotation.PublicEvolving;

import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The state representation for the BigQuery source enumerator. */
@PublicEvolving
public class BigQuerySourceEnumState {

    private final List<String> lastSeenPartitions;
    private final List<String> remaniningTableStreams;
    private final List<String> completedTableStreams;
    private final List<BigQuerySourceSplit> remainingSourceSplits;
    private final Map<String, BigQuerySourceSplit> assignedSourceSplits;
    private final Boolean initialized;

    public BigQuerySourceEnumState(
            List<String> lastSeenPartitions,
            List<String> remaniningTableStreams,
            List<String> completedTableStreams,
            List<BigQuerySourceSplit> remainingSourceSplits,
            Map<String, BigQuerySourceSplit> assignedSourceSplits,
            Boolean initialized) {
        this.lastSeenPartitions = new ArrayList<>(lastSeenPartitions);
        this.remaniningTableStreams = new ArrayList<>(remaniningTableStreams);
        this.completedTableStreams = new ArrayList<>(completedTableStreams);
        this.remainingSourceSplits = new ArrayList<>(remainingSourceSplits);
        this.assignedSourceSplits = new HashMap<>(assignedSourceSplits);
        this.initialized = initialized;
    }

    public List<String> getLastSeenPartitions() {
        return this.lastSeenPartitions;
    }

    public List<String> getRemaniningTableStreams() {
        return remaniningTableStreams;
    }

    public List<String> getCompletedTableStreams() {
        return completedTableStreams;
    }

    public List<BigQuerySourceSplit> getRemainingSourceSplits() {
        return remainingSourceSplits;
    }

    public Map<String, BigQuerySourceSplit> getAssignedSourceSplits() {
        return assignedSourceSplits;
    }

    public Boolean isInitialized() {
        return initialized;
    }

    public static BigQuerySourceEnumState initialState() {
        return new BigQuerySourceEnumState(
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                false);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.lastSeenPartitions,
                this.remaniningTableStreams,
                this.completedTableStreams,
                this.remainingSourceSplits,
                this.assignedSourceSplits,
                this.initialized);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BigQuerySourceEnumState other = (BigQuerySourceEnumState) obj;
        return Objects.equals(this.lastSeenPartitions, other.lastSeenPartitions)
                && Objects.equals(this.remaniningTableStreams, other.remaniningTableStreams)
                && Objects.equals(this.completedTableStreams, other.completedTableStreams)
                && Objects.equals(this.remainingSourceSplits, other.remainingSourceSplits)
                && Objects.equals(this.assignedSourceSplits, other.assignedSourceSplits)
                && Objects.equals(this.initialized, other.initialized);
    }

    @Override
    public String toString() {
        return String.format(
                "BigQuerySourceEnumState{"
                        + "lastSeenPartitions=%s"
                        + ", remaniningTableStreams=%s"
                        + ", completedTableStreams=%s"
                        + ", remainingSourceSplits=%s"
                        + ", assignedSourceSplits=%s"
                        + ", initialized=%s}",
                lastSeenPartitions,
                remaniningTableStreams,
                completedTableStreams,
                remainingSourceSplits,
                assignedSourceSplits,
                initialized);
    }
}

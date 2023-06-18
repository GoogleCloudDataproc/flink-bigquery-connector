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

package org.apache.flink.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The state representation for the BigQuery source enumerator. */
@PublicEvolving
public class BigQuerySourceEnumState {

    private final List<String> remaniningTableStreams;
    private final List<String> completedTableStreams;
    private final List<BigQuerySourceSplit> remainingSourceSplits;
    private final Map<String, BigQuerySourceSplit> assignedSourceSplits;
    private final Boolean initialized;

    public BigQuerySourceEnumState(
            List<String> remaniningTableStreams,
            List<String> completedTableStreams,
            List<BigQuerySourceSplit> remainingSourceSplits,
            Map<String, BigQuerySourceSplit> assignedSourceSplits,
            Boolean initialized) {
        this.remaniningTableStreams = remaniningTableStreams;
        this.completedTableStreams = completedTableStreams;
        this.remainingSourceSplits = remainingSourceSplits;
        this.assignedSourceSplits = assignedSourceSplits;
        this.initialized = initialized;
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
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new HashMap<>(), false);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 29 * hash + Objects.hashCode(this.remaniningTableStreams);
        hash = 29 * hash + Objects.hashCode(this.completedTableStreams);
        hash = 29 * hash + Objects.hashCode(this.remainingSourceSplits);
        hash = 29 * hash + Objects.hashCode(this.assignedSourceSplits);
        hash = 29 * hash + Objects.hashCode(this.initialized);
        return hash;
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
        if (!Objects.equals(this.remaniningTableStreams, other.remaniningTableStreams)) {
            return false;
        }
        if (!Objects.equals(this.completedTableStreams, other.completedTableStreams)) {
            return false;
        }
        if (!Objects.equals(this.remainingSourceSplits, other.remainingSourceSplits)) {
            return false;
        }
        if (!Objects.equals(this.assignedSourceSplits, other.assignedSourceSplits)) {
            return false;
        }
        return Objects.equals(this.initialized, other.initialized);
    }

    @Override
    public String toString() {
        return "BigQuerySourceEnumState{"
                + "remaniningTableStreams="
                + remaniningTableStreams
                + ", completedTableStreams="
                + completedTableStreams
                + ", remainingSourceSplits="
                + remainingSourceSplits
                + ", assignedSourceSplits="
                + assignedSourceSplits
                + ", initialized="
                + initialized
                + '}';
    }
}

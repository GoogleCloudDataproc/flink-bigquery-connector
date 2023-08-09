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

package com.google.cloud.flink.bigquery.services;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Data value for the last modification of a BigQuery partition. */
@Internal
public class PartitionIdWithLastModification {

    private final String id;
    private final Long lastModificationMillisFromEpoch;

    public PartitionIdWithLastModification(String id, Long lastModificationMillisFromEpoch) {
        this.id = id;
        this.lastModificationMillisFromEpoch = lastModificationMillisFromEpoch;
    }

    public String getId() {
        return id;
    }

    public Long getLastModificationMillisFromEpoch() {
        return lastModificationMillisFromEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.lastModificationMillisFromEpoch);
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
        final PartitionIdWithLastModification other = (PartitionIdWithLastModification) obj;
        return Objects.equals(this.id, other.id)
                && Objects.equals(
                        this.lastModificationMillisFromEpoch,
                        other.lastModificationMillisFromEpoch);
    }

    @Override
    public String toString() {
        return "PartitionIdWithLastModification{"
                + "id="
                + id
                + ", lastModificationMillisFromEpoch="
                + lastModificationMillisFromEpoch
                + '}';
    }
}

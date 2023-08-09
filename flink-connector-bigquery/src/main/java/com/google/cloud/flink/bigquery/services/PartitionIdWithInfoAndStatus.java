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

import com.google.cloud.flink.bigquery.common.utils.BigQueryPartition;

import java.util.Objects;

/** */
@Internal
public class PartitionIdWithInfoAndStatus {
    private final String partitionId;
    private final TablePartitionInfo info;
    private final BigQueryPartition.PartitionStatus status;

    public PartitionIdWithInfoAndStatus(
            String partitionId, TablePartitionInfo info, BigQueryPartition.PartitionStatus status) {
        this.partitionId = partitionId;
        this.info = info;
        this.status = status;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public TablePartitionInfo getInfo() {
        return info;
    }

    public BigQueryPartition.PartitionStatus getStatus() {
        return status;
    }

    public Boolean isCompleted() {
        return status.equals(BigQueryPartition.PartitionStatus.COMPLETED);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPartitionId(), getInfo(), getStatus());
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
        final PartitionIdWithInfoAndStatus other = (PartitionIdWithInfoAndStatus) obj;
        return Objects.equals(this.getPartitionId(), other.getPartitionId())
                && Objects.equals(this.getInfo(), other.getInfo())
                && this.getStatus() == other.getStatus();
    }

    @Override
    public String toString() {
        return "PartitionIdWithInfoAndStatus{"
                + "partitionId="
                + partitionId
                + ", info="
                + info
                + ", status="
                + status
                + '}';
    }
}

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

/** */
@Internal
public class PartitionIdWithInfo {
    private final String partitionId;
    private final TablePartitionInfo info;

    public PartitionIdWithInfo(String partitionId, TablePartitionInfo info) {
        this.partitionId = partitionId;
        this.info = info;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public TablePartitionInfo getInfo() {
        return info;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.partitionId);
        hash = 97 * hash + Objects.hashCode(this.info);
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
        final PartitionIdWithInfo other = (PartitionIdWithInfo) obj;
        if (!Objects.equals(this.partitionId, other.partitionId)) {
            return false;
        }
        return Objects.equals(this.info, other.info);
    }

    @Override
    public String toString() {
        return "PartitionIdWithInfo{" + "partitionId=" + partitionId + ", info=" + info + '}';
    }
}

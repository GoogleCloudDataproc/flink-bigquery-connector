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

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartitionUtils.PartitionType;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Represents the information of the BigQuery table's partition. */
@Internal
public class TablePartitionInfo {

    private final String columnName;
    private final StandardSQLTypeName columnType;
    private final PartitionType partitionType;
    private final Instant streamingBufferOldestEntryTime;

    public TablePartitionInfo(
            String columnName,
            PartitionType partitionType,
            StandardSQLTypeName columnType,
            Instant streamingBufferOldestEntryTime) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.partitionType = partitionType;
        this.streamingBufferOldestEntryTime = streamingBufferOldestEntryTime;
    }

    public String getColumnName() {
        return columnName;
    }

    public StandardSQLTypeName getColumnType() {
        return columnType;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public Instant getStreamingBufferOldestEntryTime() {
        return streamingBufferOldestEntryTime;
    }

    public List<PartitionIdWithInfo> toPartitionsWithInfo(List<String> partitionIds) {
        return Optional.ofNullable(partitionIds)
                .map(
                        ps ->
                                ps.stream()
                                        .map(id -> new PartitionIdWithInfo(id, this))
                                        .collect(Collectors.toList()))
                .orElse(Lists.newArrayList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.columnName,
                this.columnType,
                this.partitionType,
                this.streamingBufferOldestEntryTime);
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
        final TablePartitionInfo other = (TablePartitionInfo) obj;
        return Objects.equals(this.columnName, other.columnName)
                && this.columnType == other.columnType
                && this.partitionType == other.partitionType
                && Objects.equals(
                        this.streamingBufferOldestEntryTime, other.streamingBufferOldestEntryTime);
    }

    @Override
    public String toString() {
        return "TablePartitionInfo{"
                + "columnName="
                + columnName
                + ", columnType="
                + columnType
                + ", partitionType="
                + partitionType
                + ", streamingBufferOldestEntryTime="
                + streamingBufferOldestEntryTime
                + '}';
    }
}

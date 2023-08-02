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
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartition.PartitionType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Represents the information of the BigQuery table's partition. */
@Internal
public class TablePartitionInfo {

    private final String columnName;
    private final StandardSQLTypeName columnType;
    private final PartitionType partitionType;

    public TablePartitionInfo(
            String columnName, PartitionType partitionType, StandardSQLTypeName columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.partitionType = partitionType;
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
    public String toString() {
        return "TablePartitionInfo{"
                + "columnName="
                + columnName
                + ", columnType="
                + columnType
                + ", partitionType="
                + partitionType
                + '}';
    }
}

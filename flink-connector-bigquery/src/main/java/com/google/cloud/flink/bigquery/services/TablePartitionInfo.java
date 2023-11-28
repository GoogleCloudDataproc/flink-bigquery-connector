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

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.table.restrictions.BigQueryPartition;

/** Represents the information of the BigQuery table's partition. */
@Internal
public class TablePartitionInfo {

    private final String columnName;
    private final StandardSQLTypeName columnType;
    private final BigQueryPartition.PartitionType partitionType;

    public TablePartitionInfo(
            String columnName,
            BigQueryPartition.PartitionType partitionType,
            StandardSQLTypeName columnType) {
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

    public BigQueryPartition.PartitionType getPartitionType() {
        return partitionType;
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

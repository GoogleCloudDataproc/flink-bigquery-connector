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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.RowDataToProtoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/** A {@link org.apache.flink.table.connector.sink.DynamicTableSink} for Google BigQuery. */
@Internal
public class BigQueryDynamicTableSink implements DynamicTableSink, SupportsPartitioning {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicTableSink.class);

    private BigQuerySinkConfig sinkConfig;
    private DataType producedDataType;

    public BigQueryDynamicTableSink(BigQuerySinkConfig sinkConfig, DataType producedDataType) {
        this.sinkConfig = sinkConfig;
        this.producedDataType = producedDataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sinkConfig);
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
        final BigQueryDynamicTableSink other = (BigQueryDynamicTableSink) obj;
        return Objects.equals(this.sinkConfig, other.sinkConfig);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // init should be called itself.
        LogicalType logicalType = this.producedDataType.getLogicalType();
        ((RowDataToProtoSerializer) sinkConfig.getSerializer()).setLogicalType(logicalType);
        System.out.println("producedDataType: " + producedDataType);
        return SinkV2Provider.of(BigQuerySink.get(sinkConfig, null));
    }

    @Override
    public DynamicTableSink copy() {
        return new BigQueryDynamicTableSink(sinkConfig, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {}

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return SupportsPartitioning.super.requiresPartitionGrouping(supportsGrouping);
    }
}

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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.RowDataToProtoSerializer;

import java.util.Objects;
import java.util.ResourceBundle;

/** A {@link org.apache.flink.table.connector.sink.DynamicTableSink} for Google BigQuery. */
@Internal
public class BigQueryDynamicTableSink implements DynamicTableSink {

    private final BigQuerySinkConfig sinkConfig;
    private final LogicalType logicalType;
    private final Integer parallelism;

    public BigQueryDynamicTableSink(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            LogicalType logicalType) {
        this(connectOptions, deliveryGuarantee, logicalType, null);
    }

    public BigQueryDynamicTableSink(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            LogicalType logicalType,
            Integer parallelism) {
        this.logicalType = logicalType;
        this.sinkConfig =
                BigQuerySinkConfig.forTable(connectOptions, deliveryGuarantee, logicalType);
        this.parallelism = parallelism;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sinkConfig, this.logicalType);
    }

    /**
     * Method overwritten to check equality, required for testing.
     *
     * @param obj Target Object to check equality.
     * @return True if {@link Object} is equal to current object.
     */
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
        BigQueryDynamicTableSink object = (BigQueryDynamicTableSink) obj;
        return (this.logicalType == object.logicalType)
                && (this.sinkConfig.equals(object.sinkConfig));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // init() should be called itself.
        // Set the logical type.
        ((RowDataToProtoSerializer) sinkConfig.getSerializer()).setLogicalType(this.logicalType);
        // Get the Datastream-API Sink.
        if (this.parallelism == null) {
            return SinkV2Provider.of(BigQuerySink.get(this.sinkConfig));
        }
        return SinkV2Provider.of(BigQuerySink.get(this.sinkConfig), this.parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        if (this.parallelism == null) {
            return new BigQueryDynamicTableSink(
                    this.sinkConfig.getConnectOptions(),
                    this.sinkConfig.getDeliveryGuarantee(),
                    this.logicalType);
        }
        return new BigQueryDynamicTableSink(
                this.sinkConfig.getConnectOptions(),
                this.sinkConfig.getDeliveryGuarantee(),
                this.logicalType,
                this.parallelism);
    }

    @Override
    public String asSummaryString() {
        ResourceBundle connectorResources = ResourceBundle.getBundle("connector");
        return connectorResources.getString("connector");
    }

    @VisibleForTesting
    LogicalType getLogicalType() {
        return this.logicalType;
    }

    @VisibleForTesting
    BigQuerySinkConfig getSinkConfig() {
        return this.sinkConfig;
    }

    @VisibleForTesting
    Integer getSinkParallelism() {
        return this.parallelism;
    }
}

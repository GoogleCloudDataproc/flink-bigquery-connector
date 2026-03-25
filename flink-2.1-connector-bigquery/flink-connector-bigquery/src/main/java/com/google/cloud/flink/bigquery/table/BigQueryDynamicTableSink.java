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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQueryIndirectSink;
import com.google.cloud.flink.bigquery.sink.BigQueryIndirectSinkConfig;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.WriteMode;
import com.google.cloud.flink.bigquery.sink.writer.RowDataAvroWriterFactory;

import java.util.List;
import java.util.Objects;
import java.util.ResourceBundle;

/** A {@link org.apache.flink.table.connector.sink.DynamicTableSink} for Google BigQuery. */
@Internal
public class BigQueryDynamicTableSink implements DynamicTableSink {

    private final BigQueryConnectOptions connectOptions;
    private final DeliveryGuarantee deliveryGuarantee;
    private final LogicalType logicalType;
    private final Integer parallelism;
    private final boolean enableTableCreation;
    private final String partitionField;
    private final TimePartitioning.Type partitionType;
    private final Long partitionExpirationMillis;
    private final List<String> clusteredFields;
    private final String region;
    private final boolean fatalizeSerializer;
    private final WriteMode writeMode;
    private final String gcsTempPath;

    public BigQueryDynamicTableSink(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            LogicalType logicalType,
            Integer parallelism,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region,
            boolean fatalizeSerializer,
            WriteMode writeMode,
            String gcsTempPath) {
        this.connectOptions = connectOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.logicalType = logicalType;
        this.parallelism = parallelism;
        this.enableTableCreation = enableTableCreation;
        this.partitionField = partitionField;
        this.partitionType = partitionType;
        this.partitionExpirationMillis = partitionExpirationMillis;
        this.clusteredFields = clusteredFields;
        this.region = region;
        this.fatalizeSerializer = fatalizeSerializer;
        this.writeMode = writeMode;
        this.gcsTempPath = gcsTempPath;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectOptions,
                deliveryGuarantee,
                logicalType,
                parallelism,
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region,
                fatalizeSerializer,
                writeMode,
                gcsTempPath);
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
        BigQueryDynamicTableSink other = (BigQueryDynamicTableSink) obj;
        return Objects.equals(connectOptions, other.connectOptions)
                && deliveryGuarantee == other.deliveryGuarantee
                && Objects.equals(logicalType, other.logicalType)
                && Objects.equals(parallelism, other.parallelism)
                && enableTableCreation == other.enableTableCreation
                && Objects.equals(partitionField, other.partitionField)
                && partitionType == other.partitionType
                && Objects.equals(partitionExpirationMillis, other.partitionExpirationMillis)
                && Objects.equals(clusteredFields, other.clusteredFields)
                && Objects.equals(region, other.region)
                && fatalizeSerializer == other.fatalizeSerializer
                && writeMode == other.writeMode
                && Objects.equals(gcsTempPath, other.gcsTempPath);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Sink<RowData> sink;
        if (writeMode == WriteMode.INDIRECT) {
            RowType rowType = (RowType) logicalType;
            BigQueryIndirectSinkConfig<RowData> config =
                    BigQueryIndirectSinkConfig.<RowData>newBuilder()
                            .connectOptions(connectOptions)
                            .gcsTempPath(gcsTempPath)
                            .bulkWriterFactory(new RowDataAvroWriterFactory(rowType))
                            .build();
            sink = new BigQueryIndirectSink<>(config);
        } else {
            BigQuerySinkConfig<RowData> config =
                    BigQuerySinkConfig.forTable(
                            connectOptions,
                            deliveryGuarantee,
                            logicalType,
                            enableTableCreation,
                            partitionField,
                            partitionType,
                            partitionExpirationMillis,
                            clusteredFields,
                            region,
                            fatalizeSerializer);
            sink = BigQuerySink.get(config);
        }

        if (parallelism == null) {
            return SinkV2Provider.of(sink);
        }
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigQueryDynamicTableSink(
                connectOptions,
                deliveryGuarantee,
                logicalType,
                parallelism,
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region,
                fatalizeSerializer,
                writeMode,
                gcsTempPath);
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
    Integer getSinkParallelism() {
        return this.parallelism;
    }

    @VisibleForTesting
    DeliveryGuarantee getDeliveryGuarantee() {
        return this.deliveryGuarantee;
    }

    @VisibleForTesting
    BigQueryConnectOptions getConnectOptions() {
        return this.connectOptions;
    }

    @VisibleForTesting
    BigQuerySinkConfig<RowData> getSinkConfig() {
        return BigQuerySinkConfig.forTable(
                connectOptions,
                deliveryGuarantee,
                logicalType,
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region,
                fatalizeSerializer);
    }
}

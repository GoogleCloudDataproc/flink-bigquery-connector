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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.RowDataToProtoSerializer;
import org.apache.avro.Schema;

import java.util.Objects;

/** A {@link org.apache.flink.table.connector.sink.DynamicTableSink} for Google BigQuery. Tho */
@Internal
public class BigQueryDynamicTableSink implements DynamicTableSink {

    private final BigQuerySinkConfig sinkConfig;
    private final LogicalType logicalType;

    public BigQueryDynamicTableSink(BigQuerySinkConfig sinkConfig, LogicalType logicalType) {
        this.logicalType = logicalType;
        Schema avroSchema =
                BigQueryTableSchemaProvider.getAvroSchemaFromLogicalSchema(this.logicalType);
        this.sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .schemaProvider(new BigQuerySchemaProviderImpl(avroSchema))
                        .deliveryGuarantee(sinkConfig.getDeliveryGuarantee())
                        .connectOptions(sinkConfig.getConnectOptions())
                        .serializer(sinkConfig.getSerializer())
                        .build();
    }

    @Override
    public String toString() {
        String sinkConfigString =
                String.format(
                        "Sink Config: %n"
                                + "\tSchema Provider: %s%n"
                                + "\tSerializer: %s%n"
                                + "\tConnect Options: %s %n"
                                + "\tDelivery Guarantee: %s%n",
                        this.sinkConfig.getSchemaProvider(),
                        this.sinkConfig.getSerializer(),
                        this.sinkConfig.getConnectOptions(),
                        this.sinkConfig.getDeliveryGuarantee());

        return String.format("Logical Type: %s%n" + "%s", this.logicalType, sinkConfigString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sinkConfig, this.logicalType);
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
        // init() should be called itself.
        // Set the logical type.
        ((RowDataToProtoSerializer) sinkConfig.getSerializer()).setLogicalType(this.logicalType);
        // Get the Datastream-API Sink.
        return SinkV2Provider.of(BigQuerySink.get(this.sinkConfig, null));
    }

    @Override
    public DynamicTableSink copy() {
        return new BigQueryDynamicTableSink(this.sinkConfig, this.logicalType);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery";
    }

    @VisibleForTesting
    LogicalType getLogicalType() {
        return this.logicalType;
    }

    @VisibleForTesting
    BigQuerySinkConfig getSinkConfig() {
        return this.sinkConfig;
    }
}

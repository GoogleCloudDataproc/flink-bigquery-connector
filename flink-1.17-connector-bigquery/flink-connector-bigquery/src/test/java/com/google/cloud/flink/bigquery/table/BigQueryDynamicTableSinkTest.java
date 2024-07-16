/*
 * Copyright (C) 2024 Google Inc.
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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.RowDataToProtoSerializer;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Class to test {@link BigQueryDynamicTableSink}. */
public class BigQueryDynamicTableSinkTest {
    static BigQueryDynamicTableSink bigQueryDynamicTableSink = null;
    static LogicalType logicalTypeSchema = null;
    static BigQuerySinkConfig bigQuerySinkConfig = null;

    private static final int PARALLELISM = 1;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @BeforeAll
    public static void beforeTest() throws IOException {
        logicalTypeSchema =
                DataTypes.ROW(DataTypes.FIELD("number", DataTypes.BIGINT().notNull()))
                        .notNull()
                        .getLogicalType();
        bigQuerySinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .serializer(new RowDataToProtoSerializer())
                        .build();
        bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);
    }

    @Test
    public void testConstructor() {
        BigQuerySinkConfig obtainedSinkConfig = bigQueryDynamicTableSink.getSinkConfig();
        assertEquals(logicalTypeSchema, bigQueryDynamicTableSink.getLogicalType());
        assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, obtainedSinkConfig.getDeliveryGuarantee());
        Schema convertedAvroSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"record\","
                                        + "\"namespace\":\"org.apache.flink.avro.generated\",\"fields\":"
                                        + "[{\"name\":\"number\",\"type\":\"long\"}]}");
        assertEquals(convertedAvroSchema, obtainedSinkConfig.getSchemaProvider().getAvroSchema());
    }

    @Test
    public void testCopy() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);
        BigQueryDynamicTableSink bigQueryDynamicTableSinkCopy =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);

        assertTrue(checkEquality(bigQueryDynamicTableSinkCopy, bigQueryDynamicTableSink.copy()));
    }

    @Test
    public void testSummaryString() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);
        assertEquals("BigQuery", bigQueryDynamicTableSink.asSummaryString());
    }

    @Test
    public void testChangelogMode() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);
        assertEquals(
                ChangelogMode.insertOnly(),
                bigQueryDynamicTableSink.getChangelogMode(Mockito.mock(ChangelogMode.class)));
    }

    @Test
    public void testSinkRuntimeProvider() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(bigQuerySinkConfig, logicalTypeSchema);
        assertInstanceOf(
                SinkV2Provider.class,
                bigQueryDynamicTableSink.getSinkRuntimeProvider(
                        Mockito.mock(DynamicTableSink.Context.class)));
    }

    /**
     * Method overwritten to check equality, required for testing.
     *
     * @param bigQueryDynamicSink1 Object 1.
     * @param bigQueryDynamicSink2 Target Object to check equality with.
     * @return True if {@link Object} is equal to current object.
     */
    private boolean checkEquality(Object bigQueryDynamicSink1, Object bigQueryDynamicSink2) {
        if (bigQueryDynamicSink1 == bigQueryDynamicSink2) {
            return true;
        }
        if (bigQueryDynamicSink1.getClass() != bigQueryDynamicSink2.getClass()) {
            return false;
        }
        BigQueryDynamicTableSink bigQueryDynamicTableSink1 =
                (BigQueryDynamicTableSink) bigQueryDynamicSink1;
        BigQueryDynamicTableSink bigQueryDynamicTableSink2 =
                (BigQueryDynamicTableSink) bigQueryDynamicSink2;
        BigQuerySinkConfig sinkConfig1 = bigQueryDynamicTableSink1.getSinkConfig();
        BigQuerySinkConfig sinkConfig2 = bigQueryDynamicTableSink2.getSinkConfig();

        if ((bigQueryDynamicTableSink1.getLogicalType()
                        == bigQueryDynamicTableSink2.getLogicalType())
                && (sinkConfig1.getConnectOptions() == sinkConfig2.getConnectOptions())
                && (sinkConfig1.getSerializer() == sinkConfig2.getSerializer())
                && (sinkConfig1.getDeliveryGuarantee() == sinkConfig2.getDeliveryGuarantee())) {
            BigQuerySchemaProvider thisSchemaProvider = sinkConfig1.getSchemaProvider();
            BigQuerySchemaProvider objSchemaProvider = sinkConfig2.getSchemaProvider();
            return thisSchemaProvider.getAvroSchema().equals(objSchemaProvider.getAvroSchema());
        }
        return false;
    }
}

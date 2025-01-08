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

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Class to test {@link BigQueryDynamicTableSink}. */
public class BigQueryDynamicTableSinkTest {

    private static final LogicalType SCHEMA =
            DataTypes.ROW(DataTypes.FIELD("number", DataTypes.BIGINT().notNull()))
                    .notNull()
                    .getLogicalType();

    private static final int PARALLELISM = 5;
    private static final String PARTITIONING_FIELD = "foo";
    private static final List<String> CLUSTERED_FIELDS = Arrays.asList("foo", "bar");
    private static final String REGION = "us-central1";

    private static final BigQueryDynamicTableSink TABLE_SINK =
            new BigQueryDynamicTableSink(
                    StorageClientFaker.createConnectOptionsForWrite(null),
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    SCHEMA,
                    PARALLELISM,
                    true,
                    PARTITIONING_FIELD,
                    TimePartitioning.Type.DAY,
                    10000000000000L,
                    CLUSTERED_FIELDS,
                    REGION);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @Test
    public void testConstructor() {
        Schema convertedAvroSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"record\","
                                        + "\"namespace\":\"org.apache.flink.avro.generated\",\"fields\":"
                                        + "[{\"name\":\"number\",\"type\":\"long\"}]}");
        BigQuerySinkConfig obtainedSinkConfig = TABLE_SINK.getSinkConfig();
        assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, obtainedSinkConfig.getDeliveryGuarantee());
        assertEquals(convertedAvroSchema, obtainedSinkConfig.getSchemaProvider().getAvroSchema());
        assertTrue(obtainedSinkConfig.enableTableCreation());
        assertEquals(PARTITIONING_FIELD, obtainedSinkConfig.getPartitionField());
        assertEquals(TimePartitioning.Type.DAY, obtainedSinkConfig.getPartitionType());
        assertTrue(obtainedSinkConfig.getPartitionExpirationMillis() == 10000000000000L);
        assertEquals(CLUSTERED_FIELDS, obtainedSinkConfig.getClusteredFields());
        assertEquals(REGION, obtainedSinkConfig.getRegion());
        assertEquals(SCHEMA, TABLE_SINK.getLogicalType());
        assertEquals(PARALLELISM, TABLE_SINK.getSinkParallelism());
    }

    @Test
    public void testCopy() {
        assertEquals(TABLE_SINK, TABLE_SINK.copy());
    }

    @Test
    public void testSummaryString() {
        assertEquals("BigQuery", TABLE_SINK.asSummaryString());
    }

    @Test
    public void testChangelogMode() {
        assertEquals(
                ChangelogMode.insertOnly(),
                TABLE_SINK.getChangelogMode(Mockito.mock(ChangelogMode.class)));
    }

    @Test
    public void testSinkRuntimeProvider() {
        assertInstanceOf(
                SinkV2Provider.class,
                TABLE_SINK.getSinkRuntimeProvider(Mockito.mock(DynamicTableSink.Context.class)));
    }
}

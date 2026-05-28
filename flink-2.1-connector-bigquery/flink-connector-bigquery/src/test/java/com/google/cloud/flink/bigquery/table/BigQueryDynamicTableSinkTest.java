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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.WriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                    REGION,
                    true,
                    false,
                    null,
                    null,
                    null,
                    null,
                    WriteMode.STORAGE_WRITE_API,
                    null,
                    null,
                    null);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @Test
    public void testConstructor() {
        assertEquals(
                DeliveryGuarantee.AT_LEAST_ONCE, TABLE_SINK.getSinkConfig().getDeliveryGuarantee());
        assertEquals(SCHEMA, TABLE_SINK.getLogicalType());
        assertEquals(PARALLELISM, TABLE_SINK.getSinkParallelism());
        assertEquals(
                StorageClientFaker.createConnectOptionsForWrite(null),
                TABLE_SINK.getSinkConfig().getConnectOptions());
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

    @Test
    public void testSinkRuntimeProviderDirectModeReturnsDirectBigQuerySink() throws Exception {
        // BigQuerySink.get() dispatches to BigQueryDefaultSink (package-private) for
        // AT_LEAST_ONCE delivery. We assert on the class name to avoid exposing the
        // package-private type from tests.
        SinkV2Provider provider =
                (SinkV2Provider)
                        TABLE_SINK.getSinkRuntimeProvider(
                                Mockito.mock(DynamicTableSink.Context.class));
        assertTrue(
                provider.createSink().getClass().getSimpleName().equals("BigQueryDefaultSink"),
                "Expected a BigQueryDefaultSink for DIRECT/AT_LEAST_ONCE, got: "
                        + provider.createSink().getClass().getName());
    }

    @Test
    public void testSinkRuntimeProviderIndirectModeReturnsBigQueryIndirectSink() {
        // INDIRECT rejects table-creation and CDC at runtime-provider time; pass false/false here.
        BigQueryDynamicTableSink indirectSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SCHEMA,
                        PARALLELISM,
                        false,
                        PARTITIONING_FIELD,
                        TimePartitioning.Type.DAY,
                        10000000000000L,
                        CLUSTERED_FIELDS,
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/tmp",
                        "temp_p",
                        "temp_d");

        SinkV2Provider provider =
                (SinkV2Provider)
                        indirectSink.getSinkRuntimeProvider(
                                Mockito.mock(DynamicTableSink.Context.class));
        // BigQueryIndirectSink is package-private; assert by simple name to avoid exposing it.
        assertEquals("BigQueryIndirectSink", provider.createSink().getClass().getSimpleName());
    }

    @Test
    public void testSinkRuntimeProviderIndirectModeNullGcsTempPathThrows() {
        BigQueryDynamicTableSink indirectSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SCHEMA,
                        PARALLELISM,
                        false,
                        PARTITIONING_FIELD,
                        TimePartitioning.Type.DAY,
                        10000000000000L,
                        CLUSTERED_FIELDS,
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        null,
                        "temp_p",
                        "temp_d");

        assertThatThrownBy(
                        () ->
                                indirectSink.getSinkRuntimeProvider(
                                        Mockito.mock(DynamicTableSink.Context.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tempGcsPath is required");
    }

    @Test
    public void testSinkRuntimeProviderIndirectModeNullTempDatasetThrows() {
        BigQueryDynamicTableSink indirectSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SCHEMA,
                        PARALLELISM,
                        false,
                        PARTITIONING_FIELD,
                        TimePartitioning.Type.DAY,
                        10000000000000L,
                        CLUSTERED_FIELDS,
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/tmp",
                        "temp_p",
                        null);

        assertThatThrownBy(
                        () ->
                                indirectSink.getSinkRuntimeProvider(
                                        Mockito.mock(DynamicTableSink.Context.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tempDataset is required");
    }

    @Test
    public void testSinkRuntimeProviderIndirectModeNullTempProjectThrows() {
        BigQueryDynamicTableSink indirectSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SCHEMA,
                        PARALLELISM,
                        false,
                        PARTITIONING_FIELD,
                        TimePartitioning.Type.DAY,
                        10000000000000L,
                        CLUSTERED_FIELDS,
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/tmp",
                        null,
                        "temp_d");

        assertThatThrownBy(
                        () ->
                                indirectSink.getSinkRuntimeProvider(
                                        Mockito.mock(DynamicTableSink.Context.class)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tempProject is required");
    }

    @Test
    public void testCopyPreservesWriteModeAndTempGcsPath() {
        BigQueryDynamicTableSink indirectSink =
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
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/tmp",
                        "temp_p",
                        "temp_d");
        assertEquals(indirectSink, indirectSink.copy());
    }

    @Test
    public void testEqualsDiffersByWriteMode() {
        BigQueryDynamicTableSink indirect =
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
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/tmp",
                        "temp_p",
                        "temp_d");
        BigQueryDynamicTableSink direct =
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
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.STORAGE_WRITE_API,
                        "gs://bucket/tmp",
                        "temp_p",
                        "temp_d");
        assertNotEquals(indirect, direct);
    }

    @Test
    public void testEqualsDiffersByGcsTempPath() {
        BigQueryDynamicTableSink a =
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
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/a",
                        "temp_p",
                        "temp_d");
        BigQueryDynamicTableSink b =
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
                        REGION,
                        true,
                        false,
                        null,
                        null,
                        null,
                        null,
                        WriteMode.INDIRECT,
                        "gs://bucket/b",
                        "temp_p",
                        "temp_d");
        assertNotEquals(a, b);
    }

    @Test
    public void testCdcConstructor() {
        BigQueryDynamicTableSink cdcTableSink =
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
                        REGION,
                        true,
                        true,
                        "event_ts",
                        Arrays.asList("shop_id", "event_date"),
                        "INTERVAL 10 MINUTE",
                        null,
                        WriteMode.STORAGE_WRITE_API,
                        null,
                        null,
                        null);

        BigQuerySinkConfig<RowData> obtainedSinkConfig = cdcTableSink.getSinkConfig();
        assertTrue(obtainedSinkConfig.isCdcEnabled());
        assertEquals("event_ts", obtainedSinkConfig.getCdcSequenceField());
        assertEquals(
                Arrays.asList("shop_id", "event_date"),
                obtainedSinkConfig.getCdcPrimaryKeyColumns());
        assertEquals("INTERVAL 10 MINUTE", obtainedSinkConfig.getCdcMaxStaleness());
        assertEquals(
                ChangelogMode.all(),
                cdcTableSink.getChangelogMode(Mockito.mock(ChangelogMode.class)));
    }
}
